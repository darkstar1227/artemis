use crate::config::Config;
use crate::incident::Incident;
use anyhow::{Context, Result};
use std::time::Duration;

/// 把一筆已記錄的事件(JSON + 已產生的 Markdown 報告)推到共用的 collector,
/// 不論這台 host 有沒有開 escalation。純粹是給多主機/多專案一個共同的查詢
/// 入口,推送失敗絕不影響本機事件記錄 — 呼叫端(store.rs)只記 log。
pub fn push(incident: &Incident, report_markdown: Option<&str>, cfg: &Config) -> Result<()> {
    if !cfg.central.enabled {
        return Ok(());
    }

    let url = format!("{}/incidents", cfg.central.collector_url.trim_end_matches('/'));
    let body = serde_json::json!({
        "host_id": cfg.central.host_id,
        "project": cfg.name,
        "incident": incident,
        "report_markdown": report_markdown,
    });

    let mut req = ureq::post(&url)
        .timeout(Duration::from_secs(30))
        .set("Content-Type", "application/json");
    if let Some(token_env) = &cfg.central.token_env {
        if let Ok(token) = std::env::var(token_env) {
            req = req.set("Authorization", &format!("Bearer {token}"));
        }
    }

    req.send_json(body)
        .with_context(|| format!("推送事件到 central collector 失敗:{url}"))?;
    Ok(())
}
