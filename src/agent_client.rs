use crate::config::Config;
use crate::incident::{EscalationReport, Incident};
use anyhow::{Context, Result};
use std::time::Duration;

/// 呼叫本機 agent_service(Python, OpenAI Agents SDK)執行完整的四層分級
/// 自主處置。這取代了原本直接呼叫 Claude Code CLI 的 healer::escalate —
/// 判斷層與執行層現在都在 agent_service 裡,Rust 只負責偵測、把 incident
/// 與相關設定打包成一個請求送過去,並把回傳的 EscalationReport 附到事件上。
pub fn escalate(incident: &Incident, cfg: &Config) -> Result<EscalationReport> {
    if !cfg.escalation.enabled {
        return Ok(EscalationReport::default());
    }

    let url = format!("{}/escalate", cfg.agent_service.url.trim_end_matches('/'));
    let body = serde_json::json!({
        "incident": incident,
        "cwd": cfg.cwd,
        "escalation": {
            "verify_window_ms": cfg.escalation.verify_window_ms,
            "health_check_command": cfg.escalation.health_check_command,
            "stage1_allowed_tools": cfg.escalation.stage1_allowed_tools,
            "stage3_config_files": cfg.escalation.stage3_config_files,
            "stage4_test_command": cfg.escalation.stage4_test_command,
            "execution_model": cfg.escalation.execution_model,
            "execution_base_url": cfg.escalation.execution_base_url,
            "execution_api_key_env": cfg.escalation.execution_api_key_env,
        },
        "orchestrator": {
            "enabled": cfg.orchestrator.enabled,
            "base_url": cfg.orchestrator.base_url,
            "api_key_env": cfg.orchestrator.api_key_env,
            "model": cfg.orchestrator.model,
            "timeout_secs": cfg.orchestrator.timeout_secs,
        },
        "agents": {
            "risk_analysis": cfg.agents.risk_analysis,
            "security_analysis": cfg.agents.security_analysis,
            "quick_fix_analysis": cfg.agents.quick_fix_analysis,
            "log_analysis": cfg.agents.log_analysis,
            "root_cause_analysis": cfg.agents.root_cause_analysis,
        },
    });

    let mut req = ureq::post(&url)
        .timeout(Duration::from_secs(cfg.agent_service.timeout_secs))
        .set("Content-Type", "application/json");
    if let Some(token_env) = &cfg.agent_service.token_env {
        if let Ok(token) = std::env::var(token_env) {
            req = req.set("Authorization", &format!("Bearer {token}"));
        } else {
            eprintln!(
                "[artemis] 警告:agent_service.token_env 設定為 {token_env},但該環境變數未設定,將不帶 Authorization header 呼叫"
            );
        }
    }

    let resp = req
        .send_json(body)
        .with_context(|| {
            format!(
                "呼叫 agent_service 失敗:{url}(請確認已啟動:cd agent_service && uv run uvicorn main:app --port 8787)"
            )
        })?;

    resp.into_json::<EscalationReport>()
        .context("agent_service 回應不是合法的 EscalationReport JSON")
}
