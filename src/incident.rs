use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Frame {
    pub function: String,
    pub file: String,
    pub line: u32,
    pub column: Option<u32>,
    pub raw: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum Source {
    Process,
    LogFile(String),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Incident {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub project: String,
    pub source: Source,
    pub message: String,
    pub frames: Vec<Frame>,
    pub raw: String,
    pub command: String,
    pub exit_code: Option<i32>,
    pub restarted: bool,
    pub restart_count: u32,
    #[serde(default)]
    pub escalation: Option<EscalationReport>,
    /// 事故前(以及當下補跑一次)的診斷數值歷史,見 config.rs 的
    /// DiagnosticsConfig。舊事件 JSON 沒有這個欄位時預設為空陣列。
    #[serde(default)]
    pub diagnostics_history: Vec<DiagnosticSample>,
}

/// 單一診斷指令在某個時間點的執行結果(唯讀,例如 `docker stats`/`free -m`)。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiagnosticSample {
    pub ts_ms: i64,
    pub command: String,
    pub output: String,
    pub exit_code: i32,
}

/// 單一 AI 處置階段的結果(呼叫 Claude Code CLI headless 後解析出的結構化決策)。
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct StageResult {
    pub stage: String,
    pub ran: bool,
    pub action_taken: Option<String>,
    pub reasoning: Option<String>,
    pub root_cause_hypothesis: Option<String>,
    pub files_changed: Vec<String>,
    pub test_result: Option<String>,
    pub raw_response: String,
    pub verified_resolved: Option<bool>,
}

/// 完整的四層分級自主處置紀錄。
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct EscalationReport {
    /// 多模型 orchestrator 的派工與彙整結果(未啟用 orchestrator 時為 None,
    /// 退回單純由 agent_service 在 stage1 冷啟動判斷)。形狀由 agent_service
    /// (agent_service/schemas.py 的 Synthesis)定義,Rust 端只當作不透明 JSON
    /// 存放與轉發給 Python 分析器,不需要重複維護一份 struct。
    #[serde(default)]
    pub multi_agent_analysis: Option<serde_json::Value>,
    pub stage1_immediate: Option<StageResult>,
    pub stage2_parameter: Option<StageResult>,
    pub stage3_code_fix: Option<StageResult>,
    pub final_resolved: bool,
    pub code_diff: Option<String>,
}

impl Incident {
    pub fn new_id() -> String {
        let now = Utc::now();
        format!("{}-{}", now.format("%Y%m%dT%H%M%S"), &uuid::Uuid::new_v4().to_string()[..8])
    }
}
