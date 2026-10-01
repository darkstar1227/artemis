use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    #[serde(default = "default_name")]
    pub name: String,
    pub command: String,
    #[serde(default = "default_cwd")]
    pub cwd: String,
    #[serde(default)]
    pub log_files: Vec<String>,
    #[serde(default = "default_error_patterns")]
    pub error_patterns: Vec<String>,
    #[serde(default = "default_true")]
    pub auto_restart: bool,
    #[serde(default = "default_restart_delay_ms")]
    pub restart_delay_ms: u64,
    #[serde(default = "default_max_restarts")]
    pub max_restarts: u32,
    #[serde(default = "default_incidents_dir")]
    pub incidents_dir: String,
    #[serde(default = "default_context_lines")]
    pub context_lines: usize,
    #[serde(default = "default_analyzer_script")]
    pub analyzer_script: String,

    #[serde(default)]
    pub resources: ResourceConfig,
    #[serde(default)]
    pub escalation: EscalationConfig,
    #[serde(default)]
    pub agent_service: AgentServiceConfig,
    #[serde(default)]
    pub central: CentralConfig,
    #[serde(default)]
    pub orchestrator: OrchestratorConfig,
    #[serde(default)]
    pub agents: AgentsConfig,
    #[serde(default)]
    pub remote: RemoteConfig,
    #[serde(default)]
    pub diagnostics: DiagnosticsConfig,
    #[serde(default)]
    pub incidents: IncidentsConfig,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct ResourceConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_cpu_threshold")]
    pub cpu_percent: f32,
    #[serde(default = "default_mem_threshold")]
    pub memory_percent: f32,
    #[serde(default = "default_disk_threshold")]
    pub disk_percent: f32,
}
fn default_poll_interval_ms() -> u64 {
    5000
}
fn default_cpu_threshold() -> f32 {
    90.0
}
fn default_mem_threshold() -> f32 {
    90.0
}
fn default_disk_threshold() -> f32 {
    90.0
}

/// 事故前歷史診斷數值(選用,不需要 Grafana/Prometheus)。啟用後,獨立輪詢
/// 執行緒每隔 poll_interval_ms 跑一次 commands(唯讀指令,例如 `docker
/// stats`/`nvidia-smi`/`free -m`),把「時間戳+指令+輸出」寫進一個 rolling
/// buffer 檔案,只保留最近 retention_mins 分鐘。事件觸發時,Store::record
/// 會把這段事故前的歷史數值附進 Incident,讓人在報告裡直接看到惡化過程的
/// 原始數字,不需要另外查儀表板或事後才想到要留證據。
#[derive(Debug, Deserialize, Clone)]
pub struct DiagnosticsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub commands: Vec<String>,
    #[serde(default = "default_diagnostics_poll_interval_ms")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_diagnostics_retention_mins")]
    pub retention_mins: u64,
}

impl Default for DiagnosticsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            commands: Vec::new(),
            poll_interval_ms: default_diagnostics_poll_interval_ms(),
            retention_mins: default_diagnostics_retention_mins(),
        }
    }
}

fn default_diagnostics_poll_interval_ms() -> u64 {
    30_000
}
fn default_diagnostics_retention_mins() -> u64 {
    10
}

/// AI 分級自主處置設定:偵測到事件後,交由 agent_service(OpenAI Agents SDK
/// 服務,見 [agent_service])依序執行 (1) 即時處置 + 根因初判 → 驗證 →
/// (2) 伺服器參數調整 → 驗證 → (3) 程式碼層級暫時修復(不自動 commit)。
/// 每一層的工具權限都被限制在該層該做的範圍內,避免 AI 越權動作 — 這裡的
/// 欄位只描述權限範圍,實際執行與權限檢查都發生在 agent_service。
#[derive(Debug, Deserialize, Clone)]
pub struct EscalationConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_verify_window_ms")]
    pub verify_window_ms: u64,
    /// 選填:驗證是否已修復的健康檢查指令,exit code 0 視為健康。
    /// 未設定時,採「等待驗證窗後樂觀視為已解決」的保守簡化邏輯。
    #[serde(default)]
    pub health_check_command: Option<String>,

    /// 第一層可執行的安全動作白名單,例如 "Bash(systemctl restart myapp)"
    /// (沿用舊的 Claude CLI allowedTools 語法,agent_service 會解析)。
    /// 不含 Edit,AI 在此層不能改程式碼。
    #[serde(default)]
    pub stage1_allowed_tools: Vec<String>,

    /// 第二層(參數調整)允許 AI 編輯的設定檔路徑,會傳給 agent_service
    /// 明確限制只能修改這些檔案。
    #[serde(default)]
    pub stage3_config_files: Vec<String>,

    /// 第三層(程式碼暫時修復)驗證用的測試指令,例如 "npm test" / "pytest"。
    #[serde(default)]
    pub stage4_test_command: Option<String>,

    /// 第一~三層執行用的模型/provider,留空則沿用 [orchestrator] 的設定
    /// (同一個 LiteLLM gateway),也可以個別覆寫成別的 provider。
    #[serde(default)]
    pub execution_model: Option<String>,
    #[serde(default)]
    pub execution_base_url: Option<String>,
    #[serde(default)]
    pub execution_api_key_env: Option<String>,

    /// stage1~3 每一層 `Runner.run()` 的 max_turns 上限。agent_service 端每一輪都是
    /// 把完整對話歷史重送一次給模型,turns 數愈高就愈接近二次方成長的 token 成本;
    /// 30 這個舊的硬上限對大多數事件都偏保守,預設砍到 12,仍留探索(list_dir/
    /// grep_files)加實際動作的空間。
    #[serde(default = "default_max_turns_per_stage")]
    pub max_turns_per_stage: u32,

    /// 單次 escalate() 呼叫(stage0 多模型分析 + stage1~3)累計可用的 token 預算,
    /// 用 Agents SDK 回報的 usage 加總比對,超過後跳過尚未執行的後續階段。
    /// 0 表示不限制。
    #[serde(default = "default_max_tokens_per_escalation")]
    pub max_tokens_per_escalation: u64,
}

impl Default for EscalationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            verify_window_ms: default_verify_window_ms(),
            health_check_command: None,
            stage1_allowed_tools: Vec::new(),
            stage3_config_files: Vec::new(),
            stage4_test_command: None,
            execution_model: None,
            execution_base_url: None,
            execution_api_key_env: None,
            max_turns_per_stage: default_max_turns_per_stage(),
            max_tokens_per_escalation: default_max_tokens_per_escalation(),
        }
    }
}

fn default_verify_window_ms() -> u64 {
    15000
}

fn default_max_turns_per_stage() -> u32 {
    12
}

fn default_max_tokens_per_escalation() -> u64 {
    1_500_000
}

/// `agent_service` 是取代 Claude Code CLI 的 Python 服務(OpenAI Agents SDK),
/// 判斷層與執行層都在這個服務裡,Rust 偵測層透過本機 HTTP 呼叫它。
#[derive(Debug, Deserialize, Clone)]
pub struct AgentServiceConfig {
    #[serde(default = "default_agent_service_url")]
    pub url: String,
    #[serde(default = "default_agent_service_timeout_secs")]
    pub timeout_secs: u64,
    /// 存放 agent_service bearer token 的環境變數名稱(選填)。設定後,呼叫
    /// agent_service 時會附上 `Authorization: Bearer <token>`;agent_service
    /// 端需以相同 token 設定 ARTEMIS_AGENT_SERVICE_TOKEN 才會生效。若
    /// agent_service 只綁定在 127.0.0.1,不設定也是安全的;若要對外綁定
    /// (例如 --host 0.0.0.0),務必設定這個欄位。
    #[serde(default)]
    pub token_env: Option<String>,
}

impl Default for AgentServiceConfig {
    fn default() -> Self {
        Self {
            url: default_agent_service_url(),
            timeout_secs: default_agent_service_timeout_secs(),
            token_env: None,
        }
    }
}

fn default_agent_service_url() -> String {
    "http://127.0.0.1:8787".into()
}
fn default_agent_service_timeout_secs() -> u64 {
    600
}

/// 多主機事件彙整(選用)。啟用後,每次 `Store::record` 都會盡力把這筆事件
/// (JSON + 已產生的 Markdown 報告)推到一個共用的 collector(可以就是
/// agent_service 本身,它同時提供 /escalate 與 /incidents),不論這台 host
/// 有沒有開 escalation。推送失敗只會記 log,絕不影響本機事件記錄。
#[derive(Debug, Deserialize, Clone)]
pub struct CentralConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_central_collector_url")]
    pub collector_url: String,
    #[serde(default = "default_central_host_id")]
    pub host_id: String,
    #[serde(default)]
    pub token_env: Option<String>,
}

impl Default for CentralConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            collector_url: default_central_collector_url(),
            host_id: default_central_host_id(),
            token_env: None,
        }
    }
}

fn default_central_collector_url() -> String {
    "http://127.0.0.1:8787".into()
}
fn default_central_host_id() -> String {
    hostname()
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .or_else(|| {
            std::process::Command::new("hostname")
                .output()
                .ok()
                .and_then(|o| String::from_utf8(o.stdout).ok())
                .map(|s| s.trim().to_string())
        })
        .unwrap_or_else(|| "unknown-host".to_string())
}

/// 多模型 orchestrator 設定:事件發生時,先呼叫 orchestrator model(透過
/// LiteLLM 的 OpenAI-compatible API)動態決定要派哪些分析 agent
/// (風險分析/安全性分析/快速修復分析/log 分析/根因分析),再彙整結果
/// 交給 healer 的分級執行(stage1~3,仍由 Claude Code CLI 實際執行檔案操作)。
#[derive(Debug, Deserialize, Clone)]
pub struct OrchestratorConfig {
    #[serde(default)]
    pub enabled: bool,
    /// LiteLLM (或其他 OpenAI-compatible gateway)的 base_url,例如 http://localhost:4000/v1
    #[serde(default = "default_orchestrator_base_url")]
    pub base_url: String,
    /// 存放 API key 的環境變數名稱(不把金鑰寫進設定檔)
    #[serde(default)]
    pub api_key_env: Option<String>,
    #[serde(default = "default_orchestrator_model")]
    pub model: String,
    #[serde(default = "default_orchestrator_timeout_secs")]
    pub timeout_secs: u64,
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            base_url: default_orchestrator_base_url(),
            api_key_env: None,
            model: default_orchestrator_model(),
            timeout_secs: default_orchestrator_timeout_secs(),
        }
    }
}

fn default_orchestrator_base_url() -> String {
    "http://localhost:4000/v1".into()
}
fn default_orchestrator_model() -> String {
    "your-orchestrator-model".into()
}
fn default_orchestrator_timeout_secs() -> u64 {
    60
}

/// 單一分析 agent 的模型設定。base_url/api_key_env 留空時,
/// 沿用 [orchestrator] 的設定(預設都走同一個 LiteLLM gateway),
/// 但也可以個別覆寫成別的 provider(例如某個角色改直連別家 API)。
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AgentRoleConfig {
    pub model: String,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub api_key_env: Option<String>,
}

impl Default for AgentRoleConfig {
    fn default() -> Self {
        Self {
            model: default_orchestrator_model(),
            base_url: None,
            api_key_env: None,
        }
    }
}

/// 五種可派工的分析 agent。orchestrator 會動態判斷這次事件要派哪幾個,
/// 不一定全部都會被呼叫。
#[derive(Debug, Deserialize, Clone, Default)]
pub struct AgentsConfig {
    #[serde(default)]
    pub risk_analysis: AgentRoleConfig,
    #[serde(default)]
    pub security_analysis: AgentRoleConfig,
    #[serde(default)]
    pub quick_fix_analysis: AgentRoleConfig,
    #[serde(default)]
    pub log_analysis: AgentRoleConfig,
    #[serde(default)]
    pub root_cause_analysis: AgentRoleConfig,
}

/// 遠端執行後端(選用),透過 [SessAnchor](https://github.com/) 的 `sanc` CLI 在
/// 設定好的裝置上執行指令,讓 stage1/stage3 的動作不侷限在跑 agent_service 的這台
/// 主機上。這是 agent_service 內部呼叫的 `sanc` 子行程,Rust 這裡只負責把設定轉發過
/// 去 — 目前的 sanc 版本(prototype 階段)還不保證 SSH 斷線後遠端任務會繼續執行,
/// 這個功能目前的價值是「留下可查的執行紀錄」與「不用每次都重新測試連線」,不是斷線續傳。
#[derive(Debug, Deserialize, Clone)]
pub struct RemoteConfig {
    #[serde(default)]
    pub enabled: bool,
    /// 對應 `sanc device add/pin` 設定好的裝置 ID。
    #[serde(default)]
    pub device_id: Option<String>,
    #[serde(default = "default_sanc_bin")]
    pub sanc_bin: String,
    /// 轉發給 `sanc --state-dir`,預設用 sanc 自己的 `$HOME/.local/state/sessanchor`。
    #[serde(default)]
    pub state_dir: Option<String>,
    #[serde(default = "default_remote_timeout_secs")]
    pub timeout_secs: u64,
    /// 第一層(stage1)呼叫 remote_exec 時的白名單,語法比照 stage1_allowed_tools。
    #[serde(default)]
    pub allowed_commands: Vec<String>,
}

impl Default for RemoteConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            device_id: None,
            sanc_bin: default_sanc_bin(),
            state_dir: None,
            timeout_secs: default_remote_timeout_secs(),
            allowed_commands: Vec::new(),
        }
    }
}

fn default_sanc_bin() -> String {
    "sanc".into()
}
fn default_remote_timeout_secs() -> u64 {
    120
}

/// 事件去重/冷卻設定(Milestone 1)。目前只描述參數,尚未有程式碼讀取
/// 這些欄位(dedup.rs 的邏輯還沒接進 Store/recorder,是下一步的工作)——
/// 這裡先讓設定檔可以帶這個表格並通過驗證,行為不變。
// Milestone 1 step 4 起 dedup.rs 才會實際讀取這些欄位(Store/recorder 尚未
// 接上);目前只需要能被解析與驗證,先窄範圍 allow 掉 dead_code 警告。
#[allow(dead_code)]
#[derive(Debug, Deserialize, Clone)]
pub struct IncidentsConfig {
    /// 同一指紋在這個秒數內的重複發生視為「同一事件」(只 append,不重新
    /// 升級處置)。設為 0 表示關閉去重,每次都當新事件。
    #[serde(default = "default_dedup_window_secs")]
    pub dedup_window_secs: u64,
    /// 同一指紋被判定為「事後又發生」(recurrence)時,距離上次升級處置
    /// 至少要經過這個秒數才會再次升級,否則標記為 suppressed("cooldown")。
    #[serde(default = "default_cooldown_secs")]
    pub cooldown_secs: u64,
    /// 事件超過這個秒數沒有再出現,視為已自然解決,可標記為 Resolved。
    /// 必須 ≥ dedup_window_secs(否則同一事件視窗內就先被判定解決,矛盾)。
    #[serde(default = "default_resolve_after_secs")]
    pub resolve_after_secs: u64,
    /// 全域 storm guard:每小時最多允許幾次升級處置(跨所有指紋共用同一個
    /// 額度),避免同一時間大量不同錯誤把 agent_service/LLM 打爆。
    /// 0 表示不限制(storm guard 停用)。
    #[serde(default = "default_max_escalations_per_hour")]
    pub max_escalations_per_hour: u32,
    /// 事件記錄佇列的最大長度(超過時視佇列使用方式而定,可能丟棄最舊或
    /// 拒絕新事件——由接上這個設定的呼叫端決定)。
    #[serde(default = "default_max_queue")]
    pub max_queue: usize,
    /// 同時等待中的升級處置(呼叫 agent_service)上限,避免單一 host 同時
    /// 對 agent_service 發起過多平行請求。
    #[serde(default = "default_max_pending_escalations")]
    pub max_pending_escalations: usize,
    /// 指紋/狀態表定期落地(rewrite)到磁碟的間隔秒數。
    #[serde(default = "default_rewrite_interval_secs")]
    pub rewrite_interval_secs: u64,
    /// 行程收到結束訊號時,等待進行中的升級處置完成的最長秒數。
    #[serde(default = "default_shutdown_grace_secs")]
    pub shutdown_grace_secs: u64,
}

impl Default for IncidentsConfig {
    fn default() -> Self {
        Self {
            dedup_window_secs: default_dedup_window_secs(),
            cooldown_secs: default_cooldown_secs(),
            resolve_after_secs: default_resolve_after_secs(),
            max_escalations_per_hour: default_max_escalations_per_hour(),
            max_queue: default_max_queue(),
            max_pending_escalations: default_max_pending_escalations(),
            rewrite_interval_secs: default_rewrite_interval_secs(),
            shutdown_grace_secs: default_shutdown_grace_secs(),
        }
    }
}

fn default_dedup_window_secs() -> u64 {
    300
}
fn default_cooldown_secs() -> u64 {
    1800
}
fn default_resolve_after_secs() -> u64 {
    3600
}
fn default_max_escalations_per_hour() -> u32 {
    6
}
fn default_max_queue() -> usize {
    1024
}
fn default_max_pending_escalations() -> usize {
    8
}
fn default_rewrite_interval_secs() -> u64 {
    10
}
fn default_shutdown_grace_secs() -> u64 {
    30
}

fn default_name() -> String {
    "unnamed-project".into()
}
fn default_cwd() -> String {
    ".".into()
}
fn default_true() -> bool {
    true
}
fn default_restart_delay_ms() -> u64 {
    2000
}
fn default_max_restarts() -> u32 {
    5
}
fn default_incidents_dir() -> String {
    "./incidents".into()
}
fn default_context_lines() -> usize {
    6
}
fn default_analyzer_script() -> String {
    "./analyzer/analyze.py".into()
}
fn default_error_patterns() -> Vec<String> {
    vec![
        r"Error:".into(),
        r"ERROR".into(),
        r"Uncaught".into(),
        r"UnhandledPromiseRejection".into(),
        r"FATAL".into(),
        r"panic:".into(),
        r"Traceback \(most recent call last\)".into(),
        r"thread '.*' panicked at".into(),
    ]
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("找不到設定檔:{}\n請先執行 `artemis init`", path.display()))?;
        let cfg: Config = toml::from_str(&raw).context("設定檔格式錯誤 (artemis.toml)")?;
        cfg.validate().context("設定檔驗證失敗 (artemis.toml)")?;
        Ok(cfg)
    }

    /// 驗證欄位間的邏輯限制(型別層級已經由 serde/toml 檢查過)。
    /// 目前只驗證 [incidents];其餘表格暫無跨欄位限制。
    pub fn validate(&self) -> Result<()> {
        if self.escalation.max_turns_per_stage < 1 {
            anyhow::bail!("[escalation] max_turns_per_stage 必須 >= 1");
        }
        let inc = &self.incidents;
        if inc.max_queue < 1 {
            anyhow::bail!("[incidents] max_queue 必須 >= 1");
        }
        if inc.max_pending_escalations < 1 {
            anyhow::bail!("[incidents] max_pending_escalations 必須 >= 1");
        }
        // max_escalations_per_hour = 0 表示「不限制」,是刻意支援的值。
        if inc.resolve_after_secs < inc.dedup_window_secs {
            anyhow::bail!(
                "[incidents] resolve_after_secs ({}) 必須 >= dedup_window_secs ({})",
                inc.resolve_after_secs,
                inc.dedup_window_secs
            );
        }
        Ok(())
    }

    pub const EXAMPLE: &'static str = r#"# artemis.toml — Agent 版 autoheal 設定檔

name = "my-service"

# 要監控的啟動指令(會被 artemis 接管執行與監看)
command = "node server.js"

# 執行目錄(相對於此設定檔)
cwd = "."

# 額外要監看的 log 檔案(除了指令本身的 stdout/stderr)
log_files = []

# 觸發事件記錄的錯誤特徵(正規表示式)
error_patterns = [
    "Error:",
    "ERROR",
    "Uncaught",
    "UnhandledPromiseRejection",
    "FATAL",
    "panic:",
    "Traceback \\(most recent call last\\)",
    "thread '.*' panicked at",
]

# 偵測到程序崩潰時是否自動重啟
auto_restart = true
restart_delay_ms = 2000
max_restarts = 5

# 事件記錄輸出目錄
incidents_dir = "./incidents"

# 根因分析時,錯誤行前後各取幾行原始碼作為上下文
context_lines = 6

# 根因分析交由 Python 腳本處理(以 uv 管理環境與依賴)
analyzer_script = "./analyzer/analyze.py"

# 系統資源監控(CPU/記憶體/磁碟)
[resources]
enabled = false
poll_interval_ms = 5000
cpu_percent = 90.0
memory_percent = 90.0
disk_percent = 90.0

# AI 分級自主處置(執行層是 agent_service,OpenAI Agents SDK 服務,見下方):
#   第一層 即時處置(僅限白名單指令)+ 根因初判
#   → 驗證 →
#   第二層 伺服器/應用參數調整(僅限指定設定檔)
#   → 驗證 →
#   第三層 程式碼層級暫時修復(可讀寫程式碼,但不自動 commit)
[escalation]
enabled = true
verify_window_ms = 15000
# health_check_command = "curl -sf http://localhost:3000/healthz"

stage1_allowed_tools = [
    # "Bash(systemctl restart myapp)",
    # "Bash(pkill -f myapp)",
]

stage3_config_files = [
    # "./config/app.toml",
]

# stage4_test_command = "npm test"

# execution_model = "your-execution-model"      # 未設定則沿用 [orchestrator].model
# execution_base_url = "http://localhost:4000/v1"
# execution_api_key_env = "LITELLM_API_KEY"

# stage1~3 每一層 Runner.run() 的 max_turns 上限(每輪都會重送完整對話歷史給模型,
# 調低可壓低 token 成本)。
max_turns_per_stage = 12
# 單次事件升級(stage0 多模型分析 + stage1~3)累計可用的 token 預算,超過後跳過
# 尚未執行的後續階段。0 表示不限制。
max_tokens_per_escalation = 1500000

# 取代 Claude Code CLI 的 Python 服務(agent_service/,OpenAI Agents SDK):
# 判斷層與執行層都在這裡執行,偵測到事件時 Rust 會呼叫這個本機 HTTP 服務。
# 啟動方式:cd agent_service && uv run uvicorn main:app --port 8787
[agent_service]
url = "http://127.0.0.1:8787"
timeout_secs = 600
# 若 agent_service 綁定在 127.0.0.1(預設)以外的位址,務必設定這個欄位並在
# agent_service 端匯出同名環境變數 ARTEMIS_AGENT_SERVICE_TOKEN,否則任何能
# 連到這個服務的人都可以叫它對這個 repo 執行任意 bash / 寫入任意檔案。
# token_env = "ARTEMIS_AGENT_SERVICE_TOKEN"

# 多主機事件彙整(選用):啟用後每筆事件都會推一份到共用的 collector
# (可以直接指到 agent_service,它同時提供 /escalate 與 /incidents),不論
# 這台 host 有沒有開 escalation。用於多伺服器/多專案時有一個共同查詢入口。
[central]
enabled = false
collector_url = "http://127.0.0.1:8787"
# host_id 預設用系統 hostname,多台機器建議明確指定以利辨識
# host_id = "prod-web-01"
# token_env = "ARTEMIS_AGENT_SERVICE_TOKEN"

# 多模型 agent harness:事件發生時,先由 orchestrator model 動態決定要派哪些
# 分析 agent(風險/安全性/快速修復/log/根因),再彙整成 stage1~3 執行時的參考依據。
# 透過 LiteLLM 的 OpenAI-compatible API 呼叫,不同角色可以指定不同模型,
# 也可以個別覆寫成不同的 provider。
[orchestrator]
enabled = false
base_url = "http://localhost:4000/v1"
# api_key_env = "LITELLM_API_KEY"
model = "your-orchestrator-model"
timeout_secs = 60

[agents.risk_analysis]
model = "your-risk-analysis-model"

[agents.security_analysis]
model = "your-security-analysis-model"
# base_url = "https://api.example-other-provider.com/v1"
# api_key_env = "OTHER_PROVIDER_API_KEY"

[agents.quick_fix_analysis]
model = "your-quick-fix-analysis-model"

[agents.log_analysis]
model = "your-log-analysis-model"

[agents.root_cause_analysis]
model = "your-root-cause-analysis-model"

# 遠端執行後端(選用):透過 SessAnchor(`sanc` CLI/MCP)讓 stage1/stage3 的
# remote_exec 工具可以在設定好的裝置上執行指令,並用 sanc 自己的 session/
# request_id 留下可查的執行紀錄(交接時不用重新確認 SSH 能不能連線)。
# 目前 sanc 還是 prototype 階段,不保證斷線後遠端任務會持續。
[remote]
enabled = false
# device_id = "prod-web-01"          # 需先用 `sanc device add` 設定好
# sanc_bin = "sanc"                  # 預設從 PATH 找,也可指定完整路徑
# state_dir = "/var/lib/sessanchor"  # 對應 sanc --state-dir,預設用 sanc 自己的預設值
timeout_secs = 120
allowed_commands = [
    # "Bash(systemctl restart myapp)",
]

# 事件去重/冷卻(Milestone 1)。目前這個表格的欄位還沒有程式碼在讀取
# (dedup.rs 尚未接進 Store/recorder),先讓設定檔可以帶著這些值並通過驗證,
# 之後接上後行為才會改變。
[incidents]
# 同一指紋在這個秒數內再次發生視為「同一事件」,只累加次數不重新升級處置。
# 設為 0 表示關閉去重,每次都當新事件。
dedup_window_secs = 300
# 事件被判定為「重新發生」(recurrence,例如之前已標記 Mitigated/Resolved)
# 時,距離上次升級處置至少要經過這個秒數才會再次升級,否則視為
# suppressed("cooldown")。
cooldown_secs = 1800
# 超過這個秒數沒有再出現的事件視為已自然解決。必須 >= dedup_window_secs。
resolve_after_secs = 3600
# 全域 storm guard:所有指紋共用,每小時最多允許幾次升級處置(呼叫
# agent_service),避免短時間大量不同錯誤把 LLM/agent_service 打爆。
# 0 表示不限制。
max_escalations_per_hour = 6
# 事件記錄佇列長度上限。
max_queue = 1024
# 同時等待中的升級處置(呼叫 agent_service)數量上限,注意 agent_service
# 的 HTTP timeout(見 [agent_service] timeout_secs)包含排在同一個
# 專案前面的升級處置的等待時間 —— 佇列愈滿,排在後面的請求愈可能連
# 排隊時間都算進自己的 timeout 裡而逾時。
max_pending_escalations = 8
# 指紋/狀態表定期落地到磁碟的間隔秒數。
rewrite_interval_secs = 10
# 收到結束訊號時,等待進行中升級處置完成的最長秒數。
shutdown_grace_secs = 30
"#;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incidents_defaults_when_table_absent() {
        let cfg: Config = toml::from_str(r#"command = "node app.js""#).expect("最小設定應可解析");
        assert_eq!(cfg.incidents.dedup_window_secs, 300);
        assert_eq!(cfg.incidents.cooldown_secs, 1800);
        assert_eq!(cfg.incidents.resolve_after_secs, 3600);
        assert_eq!(cfg.incidents.max_escalations_per_hour, 6);
        assert_eq!(cfg.incidents.max_queue, 1024);
        assert_eq!(cfg.incidents.max_pending_escalations, 8);
        assert_eq!(cfg.incidents.rewrite_interval_secs, 10);
        assert_eq!(cfg.incidents.shutdown_grace_secs, 30);
        cfg.validate().expect("預設值應通過驗證");
    }

    #[test]
    fn example_config_parses_and_validates() {
        let cfg: Config = toml::from_str(Config::EXAMPLE).expect("EXAMPLE 應可解析");
        cfg.validate().expect("EXAMPLE 應通過驗證");
    }

    #[test]
    fn every_repo_config_file_parses_and_validates() {
        // 掃過 repo 內每一份 configs/*.toml 與根目錄 artemis.toml(若存在),
        // 確保新增 [incidents] 欄位沒有讓既有設定檔壞掉。
        let mut checked = 0;
        for dir in ["configs", "."] {
            let entries = match fs::read_dir(dir) {
                Ok(e) => e,
                Err(_) => continue,
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) != Some("toml") {
                    continue;
                }
                if path.file_name().and_then(|n| n.to_str()) == Some("Cargo.toml") {
                    continue;
                }
                let raw = fs::read_to_string(&path)
                    .unwrap_or_else(|e| panic!("讀取 {} 失敗: {e}", path.display()));
                let cfg: Config = toml::from_str(&raw)
                    .unwrap_or_else(|e| panic!("{} 應可解析: {e}", path.display()));
                cfg.validate()
                    .unwrap_or_else(|e| panic!("{} 應通過驗證: {e}", path.display()));
                checked += 1;
            }
        }
        assert!(checked > 0, "應該至少掃到一份 .toml 設定檔");
    }

    #[test]
    fn invalid_max_queue_rejected() {
        let mut cfg: Config = toml::from_str(r#"command = "node app.js""#).unwrap();
        cfg.incidents.max_queue = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn invalid_max_pending_escalations_rejected() {
        let mut cfg: Config = toml::from_str(r#"command = "node app.js""#).unwrap();
        cfg.incidents.max_pending_escalations = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn resolve_after_secs_below_dedup_window_rejected() {
        let mut cfg: Config = toml::from_str(r#"command = "node app.js""#).unwrap();
        cfg.incidents.dedup_window_secs = 600;
        cfg.incidents.resolve_after_secs = 300;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn dedup_window_zero_is_allowed() {
        let mut cfg: Config = toml::from_str(r#"command = "node app.js""#).unwrap();
        cfg.incidents.dedup_window_secs = 0;
        cfg.incidents.resolve_after_secs = 0;
        assert!(cfg.validate().is_ok());
    }
}
