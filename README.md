# Artemis

**Agent 版 autoheal** — 監控伺服器/專案的執行程序,即時偵測 crash、錯誤 log、系統資源異常,自動記錄成結構化事件,並由多模型 AI agent 分級判斷、嘗試修復,最後產出人類可讀的根因分析報告。

不是一套固定規則的重啟工具,而是一個會自己判斷「現在該做什麼處置」的 agent harness:先分析根因與風險,再決定要不要動手,動手的話又該從最保守的處置開始,一路到程式碼層級的暫時修復,每一步都會驗證是否已解決,絕不會自動 commit 或 push。

## 特色

- **三種偵測來源**:監控程序(crash / stdout·stderr 錯誤模式)、額外的 log 檔案、系統資源(CPU / 記憶體 / 磁碟)。
- **四層分級自主處置**:即時處置(安全的重啟/清理)→ 伺服器參數調整 → 程式碼層級暫時修復,每層都有嚴格的權限白名單,且每層之後都會驗證是否已解決,解決了就不會往下一層繼續。
- **多模型 orchestrator**:事件發生時先動態派遣風險分析、安全性分析、快速修復分析、log 分析、根因分析等 agent 進行判斷,再彙整出處置建議,不是每個事件都會跑滿全部流程。
- **任何 OpenAI-compatible provider**:透過 [LiteLLM](https://github.com/BerriAI/litellm) 或任何 OpenAI-compatible endpoint,不同角色、不同層級可以各自指定不同的模型與 provider。
- **可延伸到任何 repo**:`artemis onboard <repo>` 會實際掃描目標專案(README、CLAUDE.md、路由設定等),由 AI 判斷出安全的監控/處置設定並生成設定檔,而不是要你手刻。
- **不需要 Claude Code CLI**:判斷層與執行層都建立在 [OpenAI Agents SDK](https://github.com/openai/openai-agents-python) 上,自建檔案讀寫/bash 執行工具與分級權限白名單機制。

## 架構

```
┌─────────────────────────┐        HTTP (local)        ┌──────────────────────────┐
│   Rust 偵測/監控層         │ ──────────────────────────▶ │  agent_service (Python)   │
│   supervisor / watcher /  │   POST /escalate            │  OpenAI Agents SDK        │
│   resource → Store        │ ◀────────────────────────── │  Stage 0~3 判斷 + 執行     │
└─────────────────────────┘        EscalationReport       └──────────────────────────┘
            │
            ▼
   incidents/<id>.json  →  analyzer/ (stdlib-only) →  incidents/<id>.md 根因報告
```

- Rust 端(`src/`)是常駐的偵測/監控層,`artemis.toml` 是設定的唯一真相來源。偵測到事件時透過本機 HTTP 呼叫 `agent_service`。
- `agent_service/`(Python,`uv` 管理)是無狀態服務,負責判斷(orchestrator + 專科分析 agent)與執行(stage1~3 的實際檔案/bash 操作),每個 stage 的 Agent 只會拿到它該有的工具,再加上 in-tool 的白名單檢查(`agent_service/tools.py`)。
- `analyzer/`(stdlib-only Python script)負責把 JSON 事件轉成附上原始碼上下文與 `git blame` 的 Markdown 根因報告。

詳細的技術架構、資料流與各檔案職責見 [CLAUDE.md](CLAUDE.md)。

## 安裝需求

- Rust(`cargo`)
- Python 3.14+ 與 [`uv`](https://github.com/astral-sh/uv)
- 一個 OpenAI-compatible 的模型端點(例如自架 [LiteLLM](https://github.com/BerriAI/litellm) gateway,或直接用任一 provider 的 API)

## 快速開始

```bash
# 1. 建置
cargo build --release

# 2. 幫目標 repo 產生設定(AI 會實際讀過該 repo 再判斷監控/處置設定,並印出摘要供確認)
cargo run -- onboard <repo-path>

# 3. 啟動 agent_service(判斷 + 執行層,watch 時需要它在背景跑)
cd agent_service && uv run uvicorn main:app --port 8787

# 4. 另開一個 terminal,開始監控
cargo run -- watch --config configs/<repo-name>.toml

# 查看已記錄的事件
cargo run -- list
cargo run -- show <incident-id>
```

## 安全性

`agent_service` 會依請求對目標 repo 執行 bash 指令與檔案讀寫,預設只綁定在 `127.0.0.1`。若要對外開放(例如 `--host 0.0.0.0`),務必在 `[agent_service]` 設定 `token_env` 並在 `agent_service` 執行環境中設定對應的 `ARTEMIS_AGENT_SERVICE_TOKEN`,否則任何連得到這個 port 的人都能讓它對這個 repo 執行任意指令。每一層(stage1~3)也各自有獨立的權限範圍(bash 白名單 / 可編輯檔案清單 / 禁止 `git commit`·`git push`),細節見 [CLAUDE.md](CLAUDE.md#agent_service-agent_service)。

## 測試

```bash
cargo build                                   # Rust 端

cd agent_service
uv run python -c "import main"                # import/語法檢查
uv run python tests/test_mock_llm_tool_call_roundtrip.py   # 白名單內的工具呼叫可正確執行
uv run python tests/test_mock_llm_permission_denial.py     # 白名單外的工具呼叫會被拒絕
```
