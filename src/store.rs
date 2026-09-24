use crate::config::Config;
use crate::diagnostics;
use crate::incident::{DiagnosticSample, Incident};
use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// 事件觸發時,立刻補跑一次診斷指令拿「當下值」,再併入 rolling buffer 裡
/// 「事故前 retention_mins 分鐘」的歷史樣本,依時間排序回傳給 Incident。
/// 如果補跑的即時樣本跟 buffer 最後一筆同指令的時間差在 1 秒內,視為重複,
/// 只保留一份(避免輪詢執行緒剛好也在同一刻寫入,報告裡出現看似重複的數字)。
///
/// `recorder.rs` 的 `RealSink::snapshot_diagnostics` 是這個函式唯一的呼叫端。
pub(crate) fn collect_diagnostics_history(cfg: &Config) -> Vec<DiagnosticSample> {
    let mut history = diagnostics::read_recent(cfg);
    let immediate = diagnostics::run_all(cfg);

    for sample in immediate {
        let is_duplicate = history.iter().any(|h| {
            h.command == sample.command && (h.ts_ms - sample.ts_ms).abs() < 1000
        });
        if !is_duplicate {
            history.push(sample);
        }
    }

    history.sort_by_key(|s| s.ts_ms);
    history
}

/// 事件記錄的底層 I/O 原語(JSON 落地、Markdown 報告、`list`/`show`)。
///
/// 從 Milestone 1 step 4 起,`record()` 這個「同步做完一切」的方法已經拆掉
/// —— 實際的記錄流程(去重判斷、非同步升級處置)由 `recorder.rs` 的
/// `Recorder`/intake 執行緒負責,透過 `RealSink`(包著這個 `Store`)呼叫
/// `write_json`/`render_markdown` 等個別方法。`Store` 本身留下來只做兩件事:
/// 這些底層 I/O 原語,以及 `list`/`show` 這兩個仍然同步、一次性的 CLI 指令。
#[derive(Clone)]
pub struct Store {
    dir: PathBuf,
}

impl Store {
    pub fn new(dir: &str) -> Result<Self> {
        let dir = PathBuf::from(dir);
        fs::create_dir_all(&dir)
            .with_context(|| format!("無法建立事件記錄目錄:{}", dir.display()))?;
        Ok(Self { dir })
    }

    pub(crate) fn json_path(&self, id: &str) -> PathBuf {
        self.dir.join(format!("{id}.json"))
    }

    pub(crate) fn report_path(&self, id: &str) -> PathBuf {
        self.dir.join(format!("{id}.md"))
    }

    /// 把事件寫入(或覆寫)JSON 檔案。呼叫端(`RealSink`)在初次記錄、
    /// 次數累加的節流重寫、`EscalationDone` 之後的最終狀態更新時都呼叫這個
    /// 方法 —— 是唯一的落地路徑。
    pub fn write_json(&self, incident: &Incident) -> Result<PathBuf> {
        let json_path = self.json_path(&incident.id);
        let body = serde_json::to_string_pretty(incident)?;
        fs::write(&json_path, body)
            .with_context(|| format!("無法寫入事件記錄:{}", json_path.display()))?;
        Ok(json_path)
    }

    /// 呼叫 `uv` 執行根因分析腳本,產生 Markdown 報告。分析器失敗只回傳
    /// `Err` 給呼叫端記 log,絕不影響已經寫入的事件 JSON。
    pub fn render_markdown(&self, incident: &Incident, cfg: &Config) -> Result<()> {
        let json_path = self.json_path(&incident.id);
        self.run_analyzer(&json_path, cfg)
    }

    fn run_analyzer(&self, json_path: &Path, cfg: &Config) -> Result<()> {
        let analyzer_dir = Path::new(&cfg.analyzer_script)
            .parent()
            .unwrap_or_else(|| Path::new("."));

        let status = Command::new("uv")
            .arg("run")
            .arg("--project")
            .arg(analyzer_dir)
            .arg(&cfg.analyzer_script)
            .arg(json_path)
            .arg("--project-root")
            .arg(&cfg.cwd)
            .arg("--context-lines")
            .arg(cfg.context_lines.to_string())
            .status()
            .context("無法啟動 uv 執行根因分析腳本(請確認已安裝 uv)")?;

        if !status.success() {
            anyhow::bail!("根因分析腳本以非零狀態碼結束:{status}");
        }
        Ok(())
    }

    pub fn list(&self) -> Result<Vec<Incident>> {
        let mut items = Vec::new();
        if !self.dir.exists() {
            return Ok(items);
        }
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("json") {
                let raw = fs::read_to_string(&path)?;
                if let Ok(incident) = serde_json::from_str::<Incident>(&raw) {
                    items.push(incident);
                }
            }
        }
        items.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        Ok(items)
    }

    pub fn show_report(&self, id: &str) -> Result<String> {
        let report = self.report_path(id);
        if report.exists() {
            Ok(fs::read_to_string(report)?)
        } else {
            let json = self.json_path(id);
            Ok(fs::read_to_string(json)
                .with_context(|| format!("找不到事件:{id}"))?)
        }
    }
}
