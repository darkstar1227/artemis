use crate::config::Config;
use crate::incident::{Incident, Severity, Source};
use crate::matcher::{LiveScanner, StreamMatcher};
use crate::store::Store;
use anyhow::{Context, Result};
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// 把 channel 裡尚未處理的事件(含 scanner.finish() 補送的收尾事件)全部記錄
/// 下來,避免在 child 結束當下(不論是 Timeout 分支偵測到、還是 reader
/// thread 先把 tx 都 drop 掉導致 Disconnected)遺漏最後幾筆錯誤事件。
fn drain_events(
    rx: &mpsc::Receiver<crate::matcher::ErrorEvent>,
    store: &Store,
    cfg: &Config,
    restart_count: u32,
) {
    while let Ok(event) = rx.try_recv() {
        let incident = Incident::detected(
            cfg.name.clone(),
            Source::Process,
            event.message,
            event.frames,
            event.raw,
            cfg.command.clone(),
            None,
            false,
            restart_count,
            Severity::High,
        );
        if let Err(e) = store.record(incident, cfg) {
            eprintln!("[artemis] 記錄事件失敗:{e}");
        }
    }
}

/// 判斷子程序是否異常結束、記錄事件、並回報是否應該自動重啟。
/// 用 `ExitStatus::success()` 而非只看 exit code 來判斷是否崩潰:被訊號終止
/// (OOM SIGKILL、SIGSEGV 等)時 `status.code()` 會是 `None`,若只看
/// `exit_code.unwrap_or(0) != 0` 會誤判為正常結束、不記錄事件也不重啟。
fn handle_exit(
    status: std::process::ExitStatus,
    store: &Store,
    cfg: &Config,
    restart_count: u32,
) -> bool {
    let exit_code = status.code();
    eprintln!("[artemis] 程序已結束,exit code = {exit_code:?}");

    let crashed = !status.success();
    if !crashed {
        return false;
    }

    #[cfg(unix)]
    let signal_suffix = {
        use std::os::unix::process::ExitStatusExt;
        status.signal().map(|sig| {
            let name = match sig {
                9 => " (SIGKILL)".to_string(),
                11 => " (SIGSEGV)".to_string(),
                15 => " (SIGTERM)".to_string(),
                6 => " (SIGABRT)".to_string(),
                other => format!(" (signal {other})"),
            };
            format!(",killed by signal {sig}{name}")
        })
    };
    #[cfg(not(unix))]
    let signal_suffix: Option<String> = None;

    let message = format!(
        "程序異常結束,exit code = {exit_code:?}{}",
        signal_suffix.unwrap_or_default()
    );

    let incident = Incident::detected(
        cfg.name.clone(),
        Source::Process,
        message.clone(),
        vec![],
        message,
        cfg.command.clone(),
        exit_code,
        cfg.auto_restart && restart_count < cfg.max_restarts,
        restart_count,
        Severity::Critical,
    );
    let _ = store.record(incident, cfg);

    cfg.auto_restart && restart_count < cfg.max_restarts
}

fn spawn_reader<R: std::io::Read + Send + 'static>(
    reader: R,
    matcher: Arc<StreamMatcher>,
    tx: mpsc::Sender<crate::matcher::ErrorEvent>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut scanner = LiveScanner::new(&matcher);
        let buf = BufReader::new(reader);
        for line in buf.lines() {
            let line = match line {
                Ok(l) => l,
                Err(_) => break,
            };
            println!("{line}");
            if let Some(event) = scanner.feed(&line) {
                let _ = tx.send(event);
            }
        }
        if let Some(event) = scanner.finish() {
            let _ = tx.send(event);
        }
    })
}

pub fn watch(cfg: &Config, store: &Store) -> Result<()> {
    let matcher = Arc::new(StreamMatcher::new(&cfg.error_patterns)?);
    let mut restart_count = 0u32;

    loop {
        eprintln!("[artemis] 啟動被監控程序:{}", cfg.command);
        let mut child = Command::new("sh")
            .arg("-c")
            .arg(&cfg.command)
            .current_dir(&cfg.cwd)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("無法啟動被監控程序,請確認 artemis.toml 的 command 設定")?;

        let (tx, rx) = mpsc::channel();
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let h_out = spawn_reader(stdout, matcher.clone(), tx.clone());
        let h_err = spawn_reader(stderr, matcher.clone(), tx);

        // 邊等待程序結束、邊即時處理偵測到的錯誤事件。
        loop {
            match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(event) => {
                    let incident = Incident::detected(
                        cfg.name.clone(),
                        Source::Process,
                        event.message,
                        event.frames,
                        event.raw,
                        cfg.command.clone(),
                        None,
                        false,
                        restart_count,
                        Severity::High,
                    );
                    if let Err(e) = store.record(incident, cfg) {
                        eprintln!("[artemis] 記錄事件失敗:{e}");
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if let Ok(Some(status)) = child.try_wait() {
                        let _ = h_out.join();
                        let _ = h_err.join();
                        drain_events(&rx, store, cfg, restart_count);
                        let should_restart = handle_exit(status, store, cfg, restart_count);

                        if should_restart {
                            restart_count += 1;
                            eprintln!(
                                "[artemis] {} 毫秒後自動重啟(第 {}/{} 次)",
                                cfg.restart_delay_ms, restart_count, cfg.max_restarts
                            );
                            thread::sleep(Duration::from_millis(cfg.restart_delay_ms));
                        } else {
                            return Ok(());
                        }
                        break;
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    // reader thread 結束得比 child.try_wait() 偵測到結束還快時,
                    // 兩個 tx 已經先被 drop,recv_timeout 會直接回報 Disconnected
                    // 而不是等到下一次 Timeout。過去的寫法在這裡直接 return,完全
                    // 沒有檢查 exit status,導致快速崩潰(例如被訊號瞬間 kill)的
                    // 事件完全不會被記錄、也不會觸發自動重啟。這裡改成比照
                    // Timeout 分支的方式:先把已經收到的事件記錄下來,再依真正的
                    // exit status 判斷是否崩潰。
                    let _ = h_out.join();
                    let _ = h_err.join();
                    drain_events(&rx, store, cfg, restart_count);
                    let status = child.wait().context("等待子程序結束失敗")?;
                    let should_restart = handle_exit(status, store, cfg, restart_count);

                    if should_restart {
                        restart_count += 1;
                        eprintln!(
                            "[artemis] {} 毫秒後自動重啟(第 {}/{} 次)",
                            cfg.restart_delay_ms, restart_count, cfg.max_restarts
                        );
                        thread::sleep(Duration::from_millis(cfg.restart_delay_ms));
                    } else {
                        return Ok(());
                    }
                    break;
                }
            }
        }
    }
}
