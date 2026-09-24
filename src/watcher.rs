use crate::config::Config;
use crate::incident::{Incident, Source};
use crate::matcher::{LiveScanner, StreamMatcher};
use crate::store::Store;
use anyhow::Result;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// 以輪詢方式監看額外指定的 log 檔案(除了被監控程序本身的 stdout/stderr),
/// 偵測到新增內容中出現錯誤特徵時記錄事件。
pub fn watch_log_files(cfg: Arc<Config>, store: Arc<Store>) -> Result<()> {
    if cfg.log_files.is_empty() {
        return Ok(());
    }

    let matcher = Arc::new(StreamMatcher::new(&cfg.error_patterns)?);

    let mut handles = Vec::new();
    for path in cfg.log_files.clone() {
        let cfg = cfg.clone();
        let store = store.clone();
        let matcher = matcher.clone();
        handles.push(thread::spawn(move || {
            if let Err(e) = tail_one(&path, cfg, store, matcher) {
                eprintln!("[artemis] 監看 log 檔案失敗 ({path}): {e}");
            }
        }));
    }
    for h in handles {
        let _ = h.join();
    }
    Ok(())
}

fn tail_one(
    path: &str,
    cfg: Arc<Config>,
    store: Arc<Store>,
    matcher: Arc<StreamMatcher>,
) -> Result<()> {
    let mut scanner = LiveScanner::new(&matcher);

    loop {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(_) => {
                thread::sleep(Duration::from_millis(1000));
                continue;
            }
        };
        let mut reader = BufReader::new(file);
        // 從檔尾開始追蹤,不重播歷史內容。
        reader.seek(SeekFrom::End(0))?;

        loop {
            let mut line = String::new();
            let n = reader.read_line(&mut line)?;
            if n == 0 {
                thread::sleep(Duration::from_millis(500));
                continue;
            }
            let line = line.trim_end_matches(['\n', '\r']);
            if let Some(event) = scanner.feed(line) {
                record_log_incident(&cfg, &store, path, event);
            }
        }
    }
}

fn record_log_incident(
    cfg: &Config,
    store: &Store,
    path: &str,
    event: crate::matcher::ErrorEvent,
) {
    let incident = Incident {
        id: Incident::new_id(),
        timestamp: chrono::Utc::now(),
        project: cfg.name.clone(),
        source: Source::LogFile(path.to_string()),
        message: event.message,
        frames: event.frames,
        raw: event.raw,
        command: cfg.command.clone(),
        exit_code: None,
        restarted: false,
        restart_count: 0,
        escalation: None,
        diagnostics_history: Vec::new(),
    };
    if let Err(e) = store.record(incident, cfg) {
        eprintln!("[artemis] 記錄事件失敗:{e}");
    }
}
