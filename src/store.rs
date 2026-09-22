use crate::agent_client;
use crate::central_client;
use crate::config::Config;
use crate::incident::Incident;
use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

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

    fn json_path(&self, id: &str) -> PathBuf {
        self.dir.join(format!("{id}.json"))
    }

    fn report_path(&self, id: &str) -> PathBuf {
        self.dir.join(format!("{id}.md"))
    }

    /// 記錄一筆事件:先跑四層分級自主處置(若啟用),把處置結果附進事件裡,
    /// 寫入 JSON,再呼叫 Python(以 uv 管理)進行根因分析,產生 Markdown 報告。
    pub fn record(&self, mut incident: Incident, cfg: &Config) -> Result<PathBuf> {
        eprintln!("[artemis] 偵測到事件:{} ({})", incident.id, incident.message);

        if cfg.escalation.enabled {
            match agent_client::escalate(&incident, cfg) {
                Ok(report) => incident.escalation = Some(report),
                Err(e) => eprintln!("[artemis] AI 分級處置執行失敗,事件仍會照常記錄:{e}"),
            }
        }

        let json_path = self.json_path(&incident.id);
        let body = serde_json::to_string_pretty(&incident)?;
        fs::write(&json_path, body)
            .with_context(|| format!("無法寫入事件記錄:{}", json_path.display()))?;

        eprintln!("[artemis] 事件已記錄:{}", incident.id);

        if let Err(e) = self.run_analyzer(&json_path, cfg) {
            eprintln!("[artemis] 根因分析執行失敗,已保留原始事件 JSON:{e}");
        }

        if cfg.central.enabled {
            let report_markdown = fs::read_to_string(self.report_path(&incident.id)).ok();
            if let Err(e) = central_client::push(&incident, report_markdown.as_deref(), cfg) {
                eprintln!("[artemis] 推送事件到 central collector 失敗,不影響本機記錄:{e}");
            }
        }

        Ok(json_path)
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
