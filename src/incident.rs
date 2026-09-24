use crate::fingerprint;
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

    /// 事件指紋(16 碼 16 進位),用來判斷「這是不是同一種錯誤又發生了一次」。
    /// 見 fingerprint.rs。舊事件 JSON 沒有這個欄位時為 None(尚未回填)。
    #[serde(default)]
    pub fingerprint: Option<String>,
    /// 指紋計算時用的正規化 template(方便人工比對指紋為何相同/不同)。
    #[serde(default)]
    pub fingerprint_template: Option<String>,
    /// 同一指紋累積發生次數。目前尚未接上去重邏輯,一律從 1 開始;舊事件
    /// JSON 沒有這個欄位時預設為 1。
    #[serde(default = "default_occurrence_count")]
    pub occurrence_count: u64,
    /// 同一指紋第一次出現的時間。
    #[serde(default)]
    pub first_seen: Option<DateTime<Utc>>,
    /// 同一指紋最近一次出現的時間。
    #[serde(default)]
    pub last_seen: Option<DateTime<Utc>>,
    /// 事件目前的處置狀態。
    #[serde(default)]
    pub status: IncidentStatus,
    /// 狀態變化的原因說明(例如為何被標記為 Suppressed)。
    #[serde(default)]
    pub status_reason: Option<String>,
    /// 若此事件被判定為另一個事件的重複發生,指向那個事件的 id。
    #[serde(default)]
    pub recurrence_of: Option<String>,
    /// 嚴重程度。
    #[serde(default)]
    pub severity: Severity,
}

fn default_occurrence_count() -> u64 {
    1
}

/// 事件目前的處置狀態。
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum IncidentStatus {
    #[default]
    Open,
    Escalating,
    Mitigated,
    Resolved,
    Suppressed,
}

/// 事件嚴重程度。
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Critical,
    #[default]
    High,
    Medium,
    Low,
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

    /// 建立一筆新偵測到的事件。所有事件建立點(supervisor/watcher/resource)都
    /// 應該透過這個建構子而非手刻 struct literal,確保 id/timestamp/指紋/
    /// occurrence_count/status 等欄位的初始化邏輯只有一份。
    ///
    /// 目前尚未接上跨事件去重(比對既有指紋、累加 occurrence_count 等),
    /// 這裡一律視為全新事件:occurrence_count = 1、first_seen = last_seen =
    /// timestamp、status = Open。
    #[allow(clippy::too_many_arguments)]
    pub fn detected(
        project: String,
        source: Source,
        message: String,
        frames: Vec<Frame>,
        raw: String,
        command: String,
        exit_code: Option<i32>,
        restarted: bool,
        restart_count: u32,
        severity: Severity,
    ) -> Incident {
        let timestamp = Utc::now();
        let (fingerprint, fingerprint_template) = fingerprint::compute(&source, &message, &frames);

        Incident {
            id: Incident::new_id(),
            timestamp,
            project,
            source,
            message,
            frames,
            raw,
            command,
            exit_code,
            restarted,
            restart_count,
            escalation: None,
            diagnostics_history: Vec::new(),
            fingerprint: Some(fingerprint),
            fingerprint_template: Some(fingerprint_template),
            occurrence_count: 1,
            first_seen: Some(timestamp),
            last_seen: Some(timestamp),
            status: IncidentStatus::Open,
            status_reason: None,
            recurrence_of: None,
            severity,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn old_format_json_deserializes_with_defaults() {
        // 模擬新欄位加入之前寫到磁碟的舊事件 JSON,確保仍然可以反序列化,
        // 且新欄位都落在合理的預設值上。
        let old_json = r#"{
            "id": "20240101T000000-abcd1234",
            "timestamp": "2024-01-01T00:00:00Z",
            "project": "demo",
            "source": "Process",
            "message": "boom",
            "frames": [],
            "raw": "boom",
            "command": "node app.js",
            "exit_code": 1,
            "restarted": false,
            "restart_count": 0
        }"#;

        let inc: Incident = serde_json::from_str(old_json).expect("舊格式 JSON 應可反序列化");
        assert_eq!(inc.fingerprint, None);
        assert_eq!(inc.fingerprint_template, None);
        assert_eq!(inc.occurrence_count, 1);
        assert_eq!(inc.first_seen, None);
        assert_eq!(inc.last_seen, None);
        assert_eq!(inc.status, IncidentStatus::Open);
        assert_eq!(inc.status_reason, None);
        assert_eq!(inc.recurrence_of, None);
        assert_eq!(inc.severity, Severity::High);
        assert!(inc.diagnostics_history.is_empty());
        assert!(inc.escalation.is_none());
    }

    #[test]
    fn new_incident_round_trips_through_serde_json() {
        let inc = Incident::detected(
            "demo".to_string(),
            Source::Process,
            "boom".to_string(),
            vec![],
            "boom".to_string(),
            "node app.js".to_string(),
            Some(1),
            false,
            0,
            Severity::Critical,
        );

        let json = serde_json::to_string(&inc).expect("序列化不應失敗");
        let round_tripped: Incident = serde_json::from_str(&json).expect("反序列化不應失敗");

        assert_eq!(round_tripped.id, inc.id);
        assert_eq!(round_tripped.fingerprint, inc.fingerprint);
        assert_eq!(round_tripped.fingerprint_template, inc.fingerprint_template);
        assert_eq!(round_tripped.occurrence_count, inc.occurrence_count);
        assert_eq!(round_tripped.status, inc.status);
        assert_eq!(round_tripped.severity, inc.severity);
        assert_eq!(round_tripped.first_seen, inc.first_seen);
        assert_eq!(round_tripped.last_seen, inc.last_seen);
    }
}
