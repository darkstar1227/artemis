use crate::ai::{extract_json_block, run_claude};
use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};

/// 掃描目標 repo(package.json / pyproject.toml / Cargo.toml 做基礎偵測),
/// 再呼叫 Claude Code CLI(唯讀工具,不能執行 Bash/改檔案)實際讀一遍專案內容,
/// 由 AI 自己判斷合理的監控與處置設定,產生 artemis.toml 寫到
/// `<artemis_root>/configs/<repo-name>.toml`。回傳設定檔路徑與人類可讀的摘要,
/// 讓使用者在真的執行 `artemis watch` 之前可以先確認 AI 決定了哪些監控項目。
pub struct OnboardResult {
    pub config_path: PathBuf,
    pub summary: String,
    pub ai_assisted: bool,
}

pub fn onboard(repo: &Path, artemis_root: &Path) -> Result<OnboardResult> {
    let repo = repo
        .canonicalize()
        .with_context(|| format!("找不到目標專案路徑:{}", repo.display()))?;
    let name = repo
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("target-project")
        .to_string();

    let baseline = detect(&repo);
    let ai_decision = ai_refine(&repo, &name, &baseline);

    let configs_dir = artemis_root.join("configs");
    fs::create_dir_all(&configs_dir)
        .with_context(|| format!("無法建立設定目錄:{}", configs_dir.display()))?;
    let config_path = configs_dir.join(format!("{name}.toml"));
    if config_path.exists() {
        anyhow::bail!("設定檔已存在,請手動編輯或改名:{}", config_path.display());
    }

    let final_settings = FinalSettings::merge(&baseline, ai_decision.as_ref());
    let toml = render_toml(&name, &repo, &final_settings);
    fs::write(&config_path, toml)
        .with_context(|| format!("無法寫入設定檔:{}", config_path.display()))?;

    let summary = render_summary(&name, &repo, &final_settings, ai_decision.as_ref());

    Ok(OnboardResult {
        config_path,
        summary,
        ai_assisted: ai_decision.is_some(),
    })
}

struct Detection {
    command: Option<String>,
    stage4_test_command: Option<String>,
    project_kind: &'static str,
    notes: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct AiDecision {
    command: Option<String>,
    health_check_command: Option<String>,
    #[serde(default)]
    stage1_allowed_tools: Vec<String>,
    #[serde(default)]
    stage3_config_files: Vec<String>,
    stage4_test_command: Option<String>,
    #[serde(default)]
    resources_enabled: Option<bool>,
    summary: Option<String>,
}

struct FinalSettings {
    command: String,
    health_check_command: Option<String>,
    stage1_allowed_tools: Vec<String>,
    stage3_config_files: Vec<String>,
    stage4_test_command: Option<String>,
    resources_enabled: bool,
    project_kind: &'static str,
    notes: Vec<String>,
}

impl FinalSettings {
    fn merge(baseline: &Detection, ai: Option<&AiDecision>) -> Self {
        let command = ai
            .and_then(|a| a.command.clone())
            .or_else(|| baseline.command.clone())
            .unwrap_or_default();
        let stage4_test_command = ai
            .and_then(|a| a.stage4_test_command.clone())
            .or_else(|| baseline.stage4_test_command.clone());
        Self {
            command,
            health_check_command: ai.and_then(|a| a.health_check_command.clone()),
            stage1_allowed_tools: ai.map(|a| a.stage1_allowed_tools.clone()).unwrap_or_default(),
            stage3_config_files: ai.map(|a| a.stage3_config_files.clone()).unwrap_or_default(),
            stage4_test_command,
            resources_enabled: ai.and_then(|a| a.resources_enabled).unwrap_or(false),
            project_kind: baseline.project_kind,
            notes: baseline.notes.clone(),
        }
    }
}

fn detect(repo: &Path) -> Detection {
    if repo.join("package.json").exists() {
        return detect_node(repo);
    }
    if repo.join("pyproject.toml").exists() {
        return detect_python(repo);
    }
    if repo.join("Cargo.toml").exists() {
        return detect_rust(repo);
    }
    Detection {
        command: None,
        stage4_test_command: None,
        project_kind: "unknown",
        notes: vec!["未偵測到 package.json / pyproject.toml / Cargo.toml,請手動填寫 command。".into()],
    }
}

fn package_manager(repo: &Path) -> &'static str {
    if repo.join("pnpm-lock.yaml").exists() {
        "pnpm"
    } else if repo.join("yarn.lock").exists() {
        "yarn"
    } else if repo.join("bun.lockb").exists() {
        "bun"
    } else {
        "npm"
    }
}

fn detect_node(repo: &Path) -> Detection {
    let pm = package_manager(repo);
    let mut notes = vec![format!("偵測到 Node.js 專案,套件管理工具:{pm}")];

    let scripts: Value = fs::read_to_string(repo.join("package.json"))
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .and_then(|v| v.get("scripts").cloned())
        .unwrap_or(Value::Null);

    let has_script = |name: &str| scripts.get(name).and_then(|v| v.as_str()).is_some();
    let run = |script: &str| -> String {
        if pm == "npm" {
            format!("npm run {script}")
        } else {
            format!("{pm} run {script}")
        }
    };

    let command = if has_script("dev") {
        Some(run("dev"))
    } else if has_script("start") {
        Some(run("start"))
    } else {
        notes.push("package.json 沒有 dev/start script,command 需要手動填寫。".into());
        None
    };

    let mut checks = Vec::new();
    for s in ["typecheck", "lint", "test"] {
        if has_script(s) {
            checks.push(run(s));
        }
    }
    let stage4_test_command = if checks.is_empty() {
        None
    } else {
        Some(checks.join(" && "))
    };

    Detection {
        command,
        stage4_test_command,
        project_kind: "node",
        notes,
    }
}

fn detect_python(repo: &Path) -> Detection {
    let uses_uv = repo.join("uv.lock").exists();
    let mut notes = vec![format!(
        "偵測到 Python 專案(pyproject.toml){}",
        if uses_uv { ",使用 uv 管理" } else { "" }
    )];

    let entry_candidates = ["main.py", "app.py", "manage.py", "src/main.py"];
    let command = entry_candidates
        .iter()
        .find(|f| repo.join(f).exists())
        .map(|f| {
            if uses_uv {
                format!("uv run {f}")
            } else {
                format!("python3 {f}")
            }
        });
    if command.is_none() {
        notes.push("未找到常見的進入點檔案(main.py/app.py/manage.py),command 需要手動填寫。".into());
    }

    let stage4_test_command = Some(if uses_uv {
        "uv run pytest".to_string()
    } else {
        "pytest".to_string()
    });

    Detection {
        command,
        stage4_test_command,
        project_kind: "python",
        notes,
    }
}

fn detect_rust(_repo: &Path) -> Detection {
    Detection {
        command: Some("cargo run".to_string()),
        stage4_test_command: Some("cargo test".to_string()),
        project_kind: "rust",
        notes: vec!["偵測到 Rust 專案(Cargo.toml)".into()],
    }
}

/// 讓 AI 實際讀過專案(README/CLAUDE.md/package.json/env 範例/API 路由等),
/// 判斷合理的健康檢查指令、第一層安全動作白名單、第二層可調參數檔案。
/// 這一步只給 Read/Glob/Grep,不給 Bash/Edit/Write,AI 在掃描階段不能對專案做任何變更。
fn ai_refine(repo: &Path, name: &str, baseline: &Detection) -> Option<AiDecision> {
    let prompt = format!(
        "你要幫一個自動維運 agent(artemis)決定監控與自主處置設定。\n\
        目標專案路徑:{}\n專案名稱:{name}\n靜態偵測結果:專案類型={}, 猜測啟動指令={:?}, 猜測測試指令={:?}\n\n\
        請你實際讀一下這個專案(package.json / README / CLAUDE.md / .env.example / API 路由等),\
        判斷以下欄位並在最後輸出一個 JSON 區塊(你只能用 Read/Glob/Grep,不能執行任何指令或修改檔案):\n\
        - command: 本機啟動這個專案、適合被監控 stdout/stderr 的指令\n\
        - health_check_command: 一個可以判斷服務是否健康的 shell 指令(例如 curl 某個 health/首頁路由),\
          如果專案不是常駐服務就填 null\n\
        - stage1_allowed_tools: 陣列,列出偵測到問題時「立即處置」可以安全執行的指令,\
          每一項必須是 Claude Code CLI --allowedTools 認得的格式,也就是 \"Bash(實際指令)\"\
          (例如 \"Bash(rm -rf .next)\"、\"Bash(pkill -f myapp)\"),不要只給裸指令;\
          沒有適合的就給空陣列\n\
        - stage3_config_files: 陣列,列出可以在「參數調整」層被 AI 編輯的設定檔絕對路徑\
          (只能是設定/環境檔案,不要包含原始碼;沒有適合的就給空陣列)\n\
        - stage4_test_command: 修復程式碼後應該跑的測試/驗證指令\n\
        - resources_enabled: 這個專案是否適合開啟 CPU/記憶體/磁碟監控(布林值)\n\
        - summary: 用繁體中文,3-5 句話跟開發者說明你為什麼這樣設定、這個監控設定會涵蓋到什麼範圍、\
          什麼情況下 AI 才會真的去改設定檔或程式碼\n\n\
        最後請務必以下列格式的 JSON 區塊結尾(不要有其他文字在區塊內):\n\
        ```json\n\
        {{\"command\": \"...\", \"health_check_command\": null, \"stage1_allowed_tools\": [], \
        \"stage3_config_files\": [], \"stage4_test_command\": \"...\", \"resources_enabled\": false, \"summary\": \"...\"}}\n\
        ```",
        repo.display(),
        baseline.project_kind,
        baseline.command,
        baseline.stage4_test_command,
    );

    let raw = run_claude(
        "claude",
        None,
        repo,
        &prompt,
        &["Read".to_string(), "Glob".to_string(), "Grep".to_string()],
        &["Bash".to_string(), "Edit".to_string(), "Write".to_string()],
        "dontAsk",
    )
    .map_err(|e| eprintln!("[artemis] AI 專案分析失敗,改用靜態偵測結果:{e}"))
    .ok()?;

    let json = extract_json_block(&raw).or_else(|| {
        eprintln!("[artemis] AI 回應中沒有找到有效的 JSON 區塊,改用靜態偵測結果");
        None
    })?;

    serde_json::from_value(json)
        .map_err(|e| eprintln!("[artemis] AI 回應格式不符預期,改用靜態偵測結果:{e}"))
        .ok()
}

/// AI 判斷出來的字串(command、health_check_command 等)可能包含雙引號、反斜線,
/// 直接塞進 TOML basic string 會產生語法錯誤,寫入前一律要跳脫。
fn toml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            c => out.push(c),
        }
    }
    out
}

fn render_toml(name: &str, repo: &Path, s: &FinalSettings) -> String {
    let command_line = if s.command.is_empty() {
        "command = \"\" # TODO: 未能自動偵測,請手動填寫啟動指令".to_string()
    } else {
        format!("command = \"{}\"", toml_escape(&s.command))
    };
    let health_line = match &s.health_check_command {
        Some(h) => format!("health_check_command = \"{}\"", toml_escape(h)),
        None => "# health_check_command = \"\"".to_string(),
    };
    let test_line = match &s.stage4_test_command {
        Some(t) => format!("stage4_test_command = \"{}\"", toml_escape(t)),
        None => "# stage4_test_command = \"\"".to_string(),
    };
    let notes = s.notes.iter().map(|n| format!("# {n}")).collect::<Vec<_>>().join("\n");
    let stage1_list = if s.stage1_allowed_tools.is_empty() {
        "    # 尚未決定,請視情況手動補上".to_string()
    } else {
        s.stage1_allowed_tools
            .iter()
            .map(|t| format!("    \"{}\",", toml_escape(t)))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let stage3_list = if s.stage3_config_files.is_empty() {
        "    # 尚未決定,請視情況手動補上".to_string()
    } else {
        s.stage3_config_files
            .iter()
            .map(|t| format!("    \"{}\",", toml_escape(t)))
            .collect::<Vec<_>>()
            .join("\n")
    };

    let repo_escaped = toml_escape(&repo.display().to_string());
    let name_escaped = toml_escape(name);

    format!(
        r#"# 由 `artemis onboard {repo}` 自動產生(專案類型:{kind})
{notes}

name = "{name_escaped}"
{command_line}
cwd = "{repo_escaped}"

log_files = []
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

auto_restart = true
restart_delay_ms = 2000
max_restarts = 5

incidents_dir = "./incidents/{name}"
context_lines = 6
analyzer_script = "./analyzer/analyze.py"

[resources]
enabled = {resources_enabled}
poll_interval_ms = 5000
cpu_percent = 90.0
memory_percent = 90.0
disk_percent = 90.0

[escalation]
enabled = true
verify_window_ms = 15000
{health_line}

stage1_allowed_tools = [
{stage1_list}
]

stage3_config_files = [
{stage3_list}
]

{test_line}

# 取代 Claude Code CLI 的 Python 服務(agent_service/,OpenAI Agents SDK):
# 判斷層與執行層都在這裡執行,偵測到事件時 Rust 會呼叫這個本機 HTTP 服務。
# 啟動方式:cd agent_service && uv run uvicorn main:app --port 8787
[agent_service]
url = "http://127.0.0.1:8787"
timeout_secs = 600

# 多模型 agent harness(選用):啟用後,偵測到事件時會先呼叫 orchestrator model
# (透過 LiteLLM 的 OpenAI-compatible API)動態決定要派哪些分析 agent,
# 彙整結果後才交給上面的 stage1~3 執行。預設關閉,設定好 LiteLLM endpoint 與
# 模型名稱後再手動開啟。
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

[agents.quick_fix_analysis]
model = "your-quick-fix-analysis-model"

[agents.log_analysis]
model = "your-log-analysis-model"

[agents.root_cause_analysis]
model = "your-root-cause-analysis-model"
"#,
        repo = repo.display(),
        kind = s.project_kind,
        notes = notes,
        name = name,
        command_line = command_line,
        resources_enabled = s.resources_enabled,
        health_line = health_line,
        stage1_list = stage1_list,
        stage3_list = stage3_list,
        test_line = test_line,
    )
}

fn render_summary(name: &str, repo: &Path, s: &FinalSettings, ai: Option<&AiDecision>) -> String {
    let mut out = Vec::new();
    out.push(format!("專案:{name} ({})", repo.display()));
    out.push(format!("啟動指令:{}", if s.command.is_empty() { "(未偵測到,需手動填寫)" } else { &s.command }));
    out.push(format!(
        "健康檢查:{}",
        s.health_check_command.as_deref().unwrap_or("(未設定,將採樂觀驗證邏輯)")
    ));
    out.push(format!(
        "系統資源監控:{}",
        if s.resources_enabled { "啟用" } else { "未啟用" }
    ));
    out.push(format!(
        "第一層可執行的立即處置:{}",
        if s.stage1_allowed_tools.is_empty() {
            "(無,偵測到問題會直接跳過此層)".to_string()
        } else {
            s.stage1_allowed_tools.join("; ")
        }
    ));
    out.push(format!(
        "第二層可調整的設定檔:{}",
        if s.stage3_config_files.is_empty() {
            "(無,偵測到問題會直接跳過此層)".to_string()
        } else {
            s.stage3_config_files.join("; ")
        }
    ));
    out.push(format!(
        "第三層修復後驗證指令:{}",
        s.stage4_test_command.as_deref().unwrap_or("(未設定)")
    ));
    out.push(String::new());
    match ai.and_then(|a| a.summary.clone()) {
        Some(summary) => {
            out.push("AI 判斷理由:".to_string());
            out.push(summary);
        }
        None => {
            out.push("(AI 分析未成功執行,以上為純靜態偵測結果,建議手動檢查每一項設定。)".to_string());
        }
    }
    out.join("\n")
}
