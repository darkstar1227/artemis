use anyhow::{Context, Result};
use std::path::Path;
use std::process::Command;

/// 呼叫 claude -p headless,回傳最終文字輸出(--output-format json 的 "result" 欄位)。
/// 事件處置(escalation)已改由 agent_client 呼叫 Python agent_service 執行,
/// 這裡只剩 onboard(唯讀掃描目標 repo、決定監控設定)還在用 Claude Code CLI。
pub fn run_claude(
    claude_bin: &str,
    model: Option<&str>,
    cwd: &Path,
    prompt: &str,
    allowed_tools: &[String],
    disallowed_tools: &[String],
    permission_mode: &str,
) -> Result<String> {
    let mut cmd = Command::new(claude_bin);
    cmd.arg("-p")
        .arg(prompt)
        .arg("--output-format")
        .arg("json")
        .arg("--permission-mode")
        .arg(permission_mode)
        .current_dir(cwd);

    if !allowed_tools.is_empty() {
        cmd.arg("--allowedTools").arg(allowed_tools.join(","));
    }
    if !disallowed_tools.is_empty() {
        cmd.arg("--disallowedTools").arg(disallowed_tools.join(","));
    }
    if let Some(model) = model {
        cmd.arg("--model").arg(model);
    }

    let output = cmd
        .output()
        .context("無法呼叫 claude CLI,請確認已安裝 Claude Code 且 `claude` 在 PATH 上")?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&stdout) {
        // `claude -p --output-format json` 印出的是一整個 stream event 陣列,
        // 最終文字結果在陣列最後一個 {"type":"result", "result": "..."} 物件裡,
        // 不是單一物件的頂層欄位。
        let result_obj = v.as_array().and_then(|arr| arr.last()).unwrap_or(&v);
        if let Some(result) = result_obj.get("result").and_then(|r| r.as_str()) {
            return Ok(result.to_string());
        }
    }
    Ok(stdout.to_string())
}

/// 從 claude -p 的回應文字中取出最後一個 ```json ... ``` 區塊並解析。
pub fn extract_json_block(text: &str) -> Option<serde_json::Value> {
    let mut blocks = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find("```json") {
        let after = &rest[start + 7..];
        if let Some(end) = after.find("```") {
            blocks.push(&after[..end]);
            rest = &after[end + 3..];
        } else {
            break;
        }
    }
    blocks
        .last()
        .and_then(|b| serde_json::from_str::<serde_json::Value>(b.trim()).ok())
}

