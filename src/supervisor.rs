use crate::config::Config;
use crate::incident::{Incident, Source};
use crate::matcher::{LiveScanner, StreamMatcher};
use crate::store::Store;
use anyhow::{Context, Result};
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

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
                    let incident = Incident {
                        id: Incident::new_id(),
                        timestamp: chrono::Utc::now(),
                        project: cfg.name.clone(),
                        source: Source::Process,
                        message: event.message,
                        frames: event.frames,
                        raw: event.raw,
                        command: cfg.command.clone(),
                        exit_code: None,
                        restarted: false,
                        restart_count,
                        escalation: None,
                    };
                    if let Err(e) = store.record(incident, cfg) {
                        eprintln!("[artemis] 記錄事件失敗:{e}");
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if let Ok(Some(status)) = child.try_wait() {
                        let _ = h_out.join();
                        let _ = h_err.join();
                        let exit_code = status.code();
                        eprintln!("[artemis] 程序已結束,exit code = {exit_code:?}");

                        if exit_code.unwrap_or(0) != 0 {
                            let incident = Incident {
                                id: Incident::new_id(),
                                timestamp: chrono::Utc::now(),
                                project: cfg.name.clone(),
                                source: Source::Process,
                                message: format!("程序異常結束,exit code = {exit_code:?}"),
                                frames: vec![],
                                raw: String::new(),
                                command: cfg.command.clone(),
                                exit_code,
                                restarted: cfg.auto_restart && restart_count < cfg.max_restarts,
                                restart_count,
                                escalation: None,
                            };
                            let _ = store.record(incident, cfg);
                        }

                        if exit_code.unwrap_or(0) != 0
                            && cfg.auto_restart
                            && restart_count < cfg.max_restarts
                        {
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
                    let _ = child.wait();
                    let _ = h_out.join();
                    let _ = h_err.join();
                    return Ok(());
                }
            }
        }
    }
}
