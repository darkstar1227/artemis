use crate::config::Config;
use crate::incident::DiagnosticSample;
use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// 診斷 rolling buffer 檔案位置:跟 incidents_dir 同一層,獨立成
/// `diagnostics/<name>.jsonl`,避免跟事件 JSON 混在一起。
fn buffer_path(cfg: &Config) -> PathBuf {
    let incidents_dir = Path::new(&cfg.incidents_dir);
    let parent = incidents_dir.parent().unwrap_or_else(|| Path::new("."));
    parent
        .join("diagnostics")
        .join(format!("{}.jsonl", cfg.name))
}

fn run_one(command: &str) -> DiagnosticSample {
    let ts_ms = chrono::Utc::now().timestamp_millis();
    let output = Command::new("sh").arg("-c").arg(command).output();
    match output {
        Ok(out) => DiagnosticSample {
            ts_ms,
            command: command.to_string(),
            output: String::from_utf8_lossy(&out.stdout).to_string(),
            exit_code: out.status.code().unwrap_or(-1),
        },
        Err(e) => DiagnosticSample {
            ts_ms,
            command: command.to_string(),
            output: format!("(執行失敗:{e})"),
            exit_code: -1,
        },
    }
}

/// 跑一輪全部設定的診斷指令,回傳這一輪產生的樣本(不落地)。
pub fn run_all(cfg: &Config) -> Vec<DiagnosticSample> {
    cfg.diagnostics.commands.iter().map(|c| run_one(c)).collect()
}

/// 把新樣本追加進 rolling buffer,並剔除超過 retention_mins 的舊樣本。
fn append_and_prune(cfg: &Config, new_samples: &[DiagnosticSample]) -> Result<()> {
    let path = buffer_path(cfg);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("無法建立診斷資料夾:{}", parent.display()))?;
    }

    let mut samples = read_buffer(&path).unwrap_or_default();
    samples.extend_from_slice(new_samples);

    let cutoff_ms = chrono::Utc::now().timestamp_millis()
        - (cfg.diagnostics.retention_mins as i64) * 60_000;
    samples.retain(|s| s.ts_ms >= cutoff_ms);

    let body = samples
        .iter()
        .map(|s| serde_json::to_string(s).unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(&path, body + "\n")
        .with_context(|| format!("無法寫入診斷 buffer:{}", path.display()))?;
    Ok(())
}

fn read_buffer(path: &Path) -> Option<Vec<DiagnosticSample>> {
    let raw = fs::read_to_string(path).ok()?;
    Some(
        raw.lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|l| serde_json::from_str::<DiagnosticSample>(l).ok())
            .collect(),
    )
}

/// 讀出目前 buffer 裡「事故前 retention_mins 分鐘內」的歷史樣本(唯讀,不修改檔案)。
pub fn read_recent(cfg: &Config) -> Vec<DiagnosticSample> {
    let path = buffer_path(cfg);
    let cutoff_ms = chrono::Utc::now().timestamp_millis()
        - (cfg.diagnostics.retention_mins as i64) * 60_000;
    read_buffer(&path)
        .unwrap_or_default()
        .into_iter()
        .filter(|s| s.ts_ms >= cutoff_ms)
        .collect()
}

/// 獨立輪詢執行緒:純唯讀側錄,不觸發 incident、不影響 auto_restart/escalation。
pub fn watch_diagnostics(cfg: Arc<Config>) -> Result<()> {
    if !cfg.diagnostics.enabled || cfg.diagnostics.commands.is_empty() {
        return Ok(());
    }

    loop {
        let samples = run_all(&cfg);
        if let Err(e) = append_and_prune(&cfg, &samples) {
            eprintln!("[artemis] 診斷數值記錄失敗:{e}");
        }
        thread::sleep(Duration::from_millis(cfg.diagnostics.poll_interval_ms));
    }
}
