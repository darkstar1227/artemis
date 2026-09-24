# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Artemis is an "autoheal" agent: it supervises a target process (and/or log files, and/or system
resources), detects runtime errors, records them as structured incidents, and drives a **staged AI
escalation pipeline** that tries increasingly invasive remediations before handing the incident to
a human developer as a Markdown root-cause report.

Detection/supervision is Rust (unchanged, still the always-on layer). Judgment **and** execution —
both the multi-model orchestrator analysis and the actual file/bash tool calls for stage1~3 — are
handled by a separate Python service (`agent_service/`, built on the **OpenAI Agents SDK**), which
the Rust side calls over local HTTP (`src/agent_client.rs`) when it records an incident. There is no
more Claude Code CLI in the escalation path — `agent_service` self-implements file read/edit/write
and bash execution as SDK tools, with its own per-stage permission whitelisting
(see [agent_service/tools.py](agent_service/tools.py)). Root-cause report rendering (source context
+ git blame) is a separate, stdlib-only Python script (`analyzer/`) that Rust still shells out to
per incident and talks to only through the JSON file on disk — that boundary is unchanged.

`onboard.rs` (repo scanning to generate a config) still uses headless Claude Code CLI in read-only
mode — that's a separate feature from the escalation pipeline and was intentionally left as-is.

## Commands

```bash
cargo build                        # build the artemis binary (target/debug/artemis)
cargo build --release
cargo run -- init                  # scaffold ./artemis.toml
cargo run -- onboard <repo-path>   # AI-assisted: scan a repo, generate configs/<name>.toml, print a
                                    # human-readable summary of the decided monitoring/healing setup
cargo run -- watch                 # start supervising per ./artemis.toml (or --config <path>)
cargo run -- list                  # list recorded incidents
cargo run -- show <id>             # print an incident's Markdown report

# agent_service (Python, OpenAI Agents SDK) — must be running before `watch` if
# [escalation].enabled = true, since Rust calls it over HTTP for every incident.
cd agent_service && uv run uvicorn main:app --port 8787

# Python analyzer (uv-managed, invoked automatically by the Rust store — rarely run by hand)
uv run --project analyzer analyzer/analyze.py incidents/<id>.json --project-root . --context-lines 6
```

There is no Rust test suite yet; validate `src/` changes by running `cargo build` and doing a manual
`watch` smoke test against a small script that crashes/prints an error pattern. CI (`.github/
workflows/rust.yml`) runs `cargo build`/`cargo test` on every push/PR.

`agent_service/` has a minimal smoke-test suite (`agent_service/tests/`) that spins up a fake
OpenAI-compatible `/chat/completions` server and drives the real `pipeline.escalate()` through it —
this is the only place the SDK's tool-calling round trip and the permission guards are actually
exercised against a real (mocked) LLM rather than just import-checked. Each test file is both a
plain runnable script (`if __name__ == "__main__"`) and pytest-discoverable (a thin `test_*()`
wrapper around the same `main()` coroutine), so `uv run python tests/test_foo.py` and `uv run
pytest tests/` do the same thing:

```bash
cd agent_service
uv sync --all-groups                                          # installs pytest/pylint dev deps too
uv run python -c "import main"                              # import/syntax check
uv run pytest tests/ -v                                      # runs all four smoke tests above
uv run pylint main.py tools.py agents_def.py pipeline.py schemas.py central.py  # lint (10/10 gate)
uv run uvicorn main:app --port 8787                          # then curl -X POST /escalate — see
                                                               # agent_client.rs for the request shape
```

CI (`.github/workflows/agent_service.yml`) runs the import check, `pytest`, and `pylint` on every
push/PR touching `agent_service/`. `pylint` is scoped to the service source files only — the test
scripts under `tests/` intentionally duplicate mock-server boilerplate across files (each one needs
to stay a self-contained, individually runnable script) and would otherwise trip
`duplicate-code`. `[tool.pylint]` in `agent_service/pyproject.toml` disables checks that fight this
codebase's own conventions (no docstrings; `except Exception` used deliberately in several places to
degrade gracefully rather than ever block the pipeline — see below).

Unreachable/failing model calls degrade gracefully rather than crashing the service — verify that
by pointing `execution_base_url`/`orchestrator.base_url` at a port nothing is listening on.

## Architecture

### Detection sources (src/supervisor.rs, src/watcher.rs, src/resource.rs)

Three independent producers of `Incident` records, each running on its own thread, all funneling
into `Store::record`:

- **supervisor.rs** — spawns `cfg.command` as a child process, tails its stdout/stderr live via
  `LiveScanner` (src/matcher.rs), and handles crash/restart with backoff (`auto_restart`,
  `max_restarts`, `restart_delay_ms`).
- **watcher.rs** — polls extra `log_files` from the end (no history replay), same `LiveScanner`.
- **resource.rs** — polls CPU/memory/disk via `sysinfo` on `resources.poll_interval_ms`, emits an
  incident when a threshold is crossed (edge-triggered, not repeated every poll).

`matcher.rs`'s `LiveScanner` is a per-stream state machine: a line matching `error_patterns` opens
an event; subsequent lines are consumed as stack frames as long as they match the Node.js
(`at fn (file:line:col)`) or Python (`File "...", line N, in fn`) frame formats; the first
non-frame line closes the event.

### Recording and the escalation pipeline (src/store.rs, src/agent_client.rs, agent_service/)

`Store::record(incident, cfg)` is the single choke point every incident source calls. It:

1. If `escalation.enabled`, calls `agent_client::escalate()`, which POSTs the incident plus the
   relevant slice of config (`[escalation]`, `[agent_service]`, `[orchestrator]`, `[agents.*]`) to
   `agent_service`'s `/escalate` endpoint over local HTTP, and attaches the resulting
   `EscalationReport` to the incident *before* serializing it. Rust stays the single source of truth
   for `artemis.toml` — every call is stateless on the Python side, config is forwarded per-request
   rather than duplicated into a second config file.
2. Writes `incidents/<id>.json`.
3. Shells out to `uv run --project <dir of analyzer_script> <analyzer_script> <json> --project-root
   <cfg.cwd> --context-lines <cfg.context_lines>` to render `incidents/<id>.md`. Analyzer failures
   are logged but never lose the raw JSON.

If `agent_service` is unreachable or returns an error, `agent_client::escalate()` returns `Err` and
`Store::record` logs it and proceeds with `incident.escalation = None` — a down/misconfigured
agent_service never blocks incident recording.

`agent_service` (`agent_service/pipeline.py::escalate()`) runs the full four-stage flow, all via the
**OpenAI Agents SDK** pointed at whatever OpenAI-compatible endpoint each role's config names
(LiteLLM gateway or any other provider — `agents_def.py::model_for()` builds a fresh
`OpenAIChatCompletionsModel`/`AsyncOpenAI` client per `base_url`/`api_key_env` pair, so a single
incident can fan out across multiple providers):

- **Stage 0 — multi-model orchestrator (optional)**, `pipeline.py::analyze()`. If
  `orchestrator.enabled`, an orchestrator agent first *dynamically* picks which specialist agents
  this incident needs from `risk_analysis`, `security_analysis`, `quick_fix_analysis`,
  `log_analysis`, `root_cause_analysis` — not every incident gets every agent. Selected agents run
  as **parallel** `Runner.run()` calls (`asyncio.gather`), each pure judgment (no tools — text in,
  JSON out). A second orchestrator call synthesizes all findings into a `Synthesis` (root cause
  hypothesis, risk level, `recommended_stage`, free-text notes per stage). If any step fails,
  `analyze()` returns `None` and everything below behaves exactly as if orchestrator were disabled —
  it never blocks the pipeline.
- If the synthesis says `recommended_stage == "none"`, escalation stops there — no stage1~3 agent
  runs at all.
- Otherwise the `Synthesis` (when present) is rendered into extra prompt context
  (`pipeline.py::synthesis_context`) and prepended to each stage's prompt below, so the stage agent
  picks up where the specialist agents left off instead of re-deriving root cause from scratch.

Execution (anything that touches files or processes) is done by **stage-specific Agent instances**
(`agents_def.py::build_stage_agent`), each built with only the tool functions it should have —
that's the permission model, replacing what Claude Code CLI's `--allowedTools`/`--disallowedTools`
used to provide. Tools are self-implemented in `agent_service/tools.py` (`read_file`, `edit_file`,
`write_file`, `run_bash`, `list_dir`, `grep_files`), all path-confined to `cwd`, with a
`StageContext` (the Agents SDK run context) carrying the finer-grained scoping each stage needs.
`list_dir`/`grep_files` are read-only exploration tools (grep skips `.git`/`node_modules`/`.venv`/
`__pycache__`/`target`) added so stage2/stage3 agents can locate the right file instead of having to
guess an exact path for `read_file` — both are capped (`LIST_DIR_MAX_ENTRIES`, `GREP_MAX_MATCHES`/
`GREP_MAX_CHARS` in `tools.py`) for the same context-budget reason as `READ_FILE_MAX_CHARS` below:

| Stage | Purpose | Tools given | In-tool guard |
|---|---|---|---|
| 1 — immediate disposition | safe mitigation (restart/cleanup) + draft root cause | `run_bash` (+ `remote_exec` if `[remote].enabled`) | `run_bash`/`remote_exec` reject anything not exactly matching `escalation.stage1_allowed_tools` / `remote.allowed_commands` |
| 2 — parameter tuning | adjust app/server config | `read_file`, `edit_file`, `list_dir`, `grep_files` | `edit_file`/`write_file` reject any path not in `escalation.stage3_config_files` |
| 3 — code-level temporary fix | edit source, run tests | `read_file`, `edit_file`, `write_file`, `run_bash`, `list_dir`, `grep_files` (+ `remote_exec` if `[remote].enabled`) | `run_bash`/`remote_exec` reject any command containing `git commit`/`git push` |

**Remote execution (optional, `[remote]`)**: `tools.py::remote_exec` shells out to the
[SessAnchor](https://github.com/) `sanc` CLI so stage1/stage3 can run commands against a configured
remote device instead of only `cfg.cwd` on the machine running `agent_service`. Each call reuses one
long-lived `sanc` session per device (`artemis-<device_id>`) and passes a fresh `--request-id` to
`sanc exec`, so the actual command/output is retrievable later via `sanc task <id>`/`sanc output <id>`
— that's the point: a maintainer taking over doesn't have to guess what already ran on that host or
re-verify SSH connectivity from scratch, they can just look up the session's history through `sanc`
directly. This is **not** disconnect-survival: the `sanc` build this was built against reports
`"ssh": false, "durable_tasks": false` in `sanc capabilities`, and `sanc exec --help` itself says "No
remote persistence on SSH loss yet" — if `agent_service` or the SSH connection drops mid-command, the
remote command does not keep running and resume; only the request-id/session bookkeeping survives for
later lookup. `sanc session create` is called best-effort before every `sanc exec` (a nonzero exit is
assumed to mean "session already exists" and ignored, matching this codebase's degrade-gracefully
philosophy elsewhere) — sanc itself doesn't expose an "already exists" check yet at the prototype
stage this was integrated against.

`run_stage()`'s `max_turns` is 30 (`pipeline.py`) to leave room for a `list_dir`/`grep_files`
exploration pass before the stage's actual read/edit/bash calls, while still being a hard cap so an
agent can't loop indefinitely.

Each stage is skipped if the previous stage's `verify_resolved()` check passed. `verify_resolved()`
(now in `pipeline.py`, run off the event loop via `asyncio.to_thread`) runs
`escalation.health_check_command` if set (exit 0 = healthy); **without a health check command it
optimistically assumes resolved after `verify_window_ms`** — this is a known simplification, always
set `health_check_command` for anything beyond a toy setup. Stage 3's diff (`git diff`, uncommitted)
is captured into the incident so the report shows exactly what changed — nothing is ever
auto-committed or pushed.

Each agent's structured decision is extracted from its final response by grabbing the **last**
fenced ` ```json ` block in the text (`pipeline.py::extract_json_block` — same convention the old
Rust `ai::extract_json_block` used). If an agent doesn't emit a parseable block, the stage still
records the raw response text but structured fields (`action_taken`, `reasoning`, etc.) are `None`.

### Onboarding a new repo (src/onboard.rs)

`artemis onboard <repo>` generalizes setup to "point at any repo": it does static detection
(`package.json`/`pyproject.toml`/`Cargo.toml` + lockfile sniffing) for a baseline, then calls Claude
Code CLI in **read-only** mode (`Read`/`Glob`/`Grep` only — `Bash`/`Edit`/`Write` explicitly
disallowed) to actually read the target repo's README/CLAUDE.md/.env.example/routes and *judge* the
safety-critical fields: `command`, `health_check_command`, `stage1_allowed_tools` (must be in
Claude's `Bash(...)` allowedTools syntax — the prompt says so explicitly), `stage3_config_files`
(deliberately excludes anything holding secrets), `stage4_test_command`, and whether resource
monitoring is worth enabling. The AI's `summary` field is printed back to the user as a Traditional
Chinese explanation of *why* each field was set that way — this is the human confirmation step
before anyone runs `artemis watch` against a real repo. Falls back to the static baseline (with a
stderr warning) if the AI call fails or returns unparsable JSON. Generated configs land in
`configs/<repo-name>.toml`; `onboard()` refuses to overwrite an existing one.

String values coming from the AI (command, health check, file paths) are run through
`onboard::toml_escape` before being written — they're free text and can contain `"`/`\`, which would
otherwise produce invalid TOML.

### Config (src/config.rs)

`artemis.toml` (scaffolded by `artemis init` from `Config::EXAMPLE`) is the single source of truth;
there are no env-var overrides — API keys for LiteLLM/providers are read from an env var *name*
configured in TOML (`api_key_env`), never stored in the file itself. Notable nesting: `[resources]`,
`[escalation]`, `[agent_service]`, `[central]`, `[orchestrator]` and `[agents.*]` are all optional
tables with their own defaults, so a minimal config only needs `command`. `[agent_service]` (`url`,
`timeout_secs`, optional `token_env`) is where the Python service's HTTP endpoint lives —
`token_env` names an env var holding a bearer token, only needed if `agent_service` is bound beyond
`127.0.0.1` (see the agent_service section below). `[central]` (`enabled`, `collector_url`,
`host_id`, optional `token_env`) is the multi-host incident push described below — independent of
`[escalation]`. `[escalation]` no longer has
`claude_bin`/`model` (those were Claude Code CLI-specific); it instead has optional
`execution_model`/`execution_base_url`/`execution_api_key_env` for stage1~3's model, falling back to
`[orchestrator]`'s settings when unset. `[remote]` (`enabled`, `device_id`, `sanc_bin`, `state_dir`,
`timeout_secs`, `allowed_commands`) configures the optional SessAnchor remote-execution backend
described above — disabled by default, and `remote_exec` isn't even added to a stage's tool list
unless `enabled` and `device_id` are both set.

### Incident shape (src/incident.rs)

`Incident` is the on-disk JSON schema — treat it as a stable contract between the Rust writer and
the Python readers (both `agent_service` and `analyzer/`). `Source` is an enum (`Process` or
`LogFile(String)`, the latter also used for the synthetic `"system-resources"` source from
resource.rs). `EscalationReport` holds an optional `multi_agent_analysis: Option<serde_json::Value>`
(deliberately untyped on the Rust side now — its shape is owned by `agent_service/schemas.py`'s
`Synthesis`, Rust just stores/forwards it) plus up to three `StageResult`s and
`final_resolved`/`code_diff`.

### agent_service (agent_service/)

Python, `uv`-managed, built on the **OpenAI Agents SDK**. Files:

- `schemas.py` — pydantic models for the `/escalate` request/response, mirroring the Rust structs
  above field-for-field so the JSON on both sides stays a straightforward 1:1 mapping.
- `tools.py` — the self-built `read_file`/`edit_file`/`write_file`/`run_bash`/`list_dir`/
  `grep_files`/`remote_exec` tool functions plus `StageContext` (the per-run permission scope) and the
  path/whitelist/git-commit guards described above.
- `agents_def.py` — builds `Agent`/`OpenAIChatCompletionsModel` instances from the request's config,
  one per judgment role and one per stage; `model_for()` is where per-role/per-stage
  `base_url`/`api_key_env` overrides turn into a distinct `AsyncOpenAI` client.
- `pipeline.py` — the actual Stage 0~3 flow (`escalate()`), `extract_json_block`/`incident_context`
  helpers, and `verify_resolved()`. `incident_context()` truncates the incident's raw output/frames
  (`INCIDENT_RAW_MAX_CHARS`/`INCIDENT_FRAMES_MAX`) before embedding it into every stage/judgment
  prompt — same rationale as `READ_FILE_MAX_CHARS` below, but this one matters more because a
  single incident's context gets re-embedded into several independent `Runner.run()` calls.
  It also renders `incident["diagnostics_history"]` (the Rust side's rolling pre-incident
  `DiagnosticSample` history, src/incident.rs), grouped by command with most-recent-first and capped
  at `INCIDENT_DIAGNOSTICS_MAX_CHARS`, so stage/judgment agents see resource trends leading up to the
  incident, not just the moment it fired. Missing/empty `diagnostics_history` is tolerated (older
  incidents, or Rust configs with diagnostics disabled).
- `main.py` — the FastAPI app (`GET /health`, `POST /escalate`).
- `tests/` — mock-LLM smoke tests (see Commands above); not part of the shipped service.

Run it with `cd agent_service && uv run uvicorn main:app --port 8787`. It's stateless — no local
config file, no persisted state — everything it needs arrives in the request body.

**Access control**: `/escalate` executes arbitrary bash and file writes against `cfg.cwd` on
whatever the request tells it to — safe by default only because the default bind address is
`127.0.0.1`. If `ARTEMIS_AGENT_SERVICE_TOKEN` is set in the environment `agent_service` runs in,
`/escalate` requires a matching `Authorization: Bearer <token>` header (checked with
`hmac.compare_digest`); `/health` is always open. Rust sends that header when `[agent_service]
token_env` names an env var it can read (`src/agent_client.rs`). **If you ever bind `agent_service`
beyond `127.0.0.1`** (e.g. `--host 0.0.0.0`), set this token on both sides — otherwise anyone who can
reach the port can make it run arbitrary commands against the target repo. `read_file` also caps
returned content at `tools.py::READ_FILE_MAX_CHARS` (20,000 chars, truncated with a marker) so a
large/binary file can't blow up an agent's context window or cost; `list_dir`/`grep_files` have
their own caps (`LIST_DIR_MAX_ENTRIES`, `GREP_MAX_MATCHES`/`GREP_MAX_CHARS`) for the same reason.
`grep_files` also re-resolves each candidate file before reading it, so a symlink inside `cwd`
pointing outside it is skipped rather than read.

`EscalateRequest.cwd` (`schemas.py`) is validated on every `/escalate` call (a pydantic
`field_validator`, rejected as HTTP 422): must be an absolute, existing directory, and not the
filesystem root — `cwd="/"` no longer defeats every tool's path confinement. If the environment
`agent_service` runs in sets `ARTEMIS_ALLOWED_CWD_ROOTS` (a `os.pathsep`-separated list of
directories), the resolved `cwd` must additionally be one of, or nested under, one of those roots;
unset, any absolute existing non-root directory is accepted (same as before this check existed).
`escalation.stage3_config_files` entries that are relative (e.g. `"./config/app.toml"`) are resolved
against the request's `cwd`, not `agent_service`'s own process cwd — a relative entry used to never
match and silently deny every stage2 edit.

### Multi-host / multi-project (src/central_client.rs, agent_service/central.py)

Each `artemis watch` process only supervises one `artemis.toml` (one project). To run many
projects/hosts: `artemis onboard <repo>` per project gives each one its own tailored
`configs/<name>.toml`; all of them can point `[agent_service]` at the *same* `agent_service`
instance since it's stateless per-request. For a single place to query incidents across every
host/project, set `[central] enabled = true` (+ `collector_url`, `host_id`, optional `token_env`)
— `Store::record` best-effort-pushes every recorded incident (JSON + rendered Markdown) to
`agent_service`'s `POST /incidents`, **independent of whether `escalation.enabled`** on that host; a
push failure only logs, never affects local recording (`central_client::push`, same
fire-and-forget philosophy as `agent_client::escalate`). `agent_service/central.py` persists pushes
to a local SQLite file (`agent_service/data/central.db`, stdlib `sqlite3`, no new dependency) and
`main.py` exposes `GET /incidents?host_id=&project=&limit=` and `GET
/incidents/{host_id}/{incident_id}` (both behind the same bearer-token auth as `/escalate`).

`Dockerfile` (repo root) builds the `artemis` binary; the runtime image also pre-provisions
`analyzer`'s pinned Python interpreter and warms its venv at build time (`uv python install 3.14 &&
uv run --project analyzer python3 -c ""`) so recording the first incident in a fresh container
doesn't pay a ~30MB interpreter download. `agent_service/Dockerfile` builds the Python service.
`docker-compose.example.yml` shows one shared `agent_service` + one `artemis watch` service per
project. Both Dockerfiles have matching `.dockerignore`s — without them, a host-built `.venv` gets
copied into the image with a broken interpreter symlink (harmless but forces a venv rebuild at
container start). `agent_service`'s port is **not** published to the host by default in the example
compose file — `artemis watch` containers reach it over the compose network at
`http://agent_service:8787`; `ARTEMIS_AGENT_SERVICE_TOKEN` is a required env var there (compose
fails fast if unset) since an unauthenticated `/escalate` is remote code execution against the
mounted project. If you need host access for debugging, uncomment the `ports` line and bind it to
`127.0.0.1` only, never publish it on `0.0.0.0`. Stage1~3 execution happens inside the
`agent_service` container, not `artemis watch`, so each project's directory must be mounted at the
*same* in-container path (`/projects/<name>` by convention) in **both** services — `agent_service`
read-write (stage2/3 edit files there), `artemis watch` may stay `:ro` since the analyzer only reads
source for context/`git blame`. A project's `cwd` in its `configs/<name>.toml` must equal that
shared in-container path. `ARTEMIS_ALLOWED_CWD_ROOTS` on `agent_service` whitelists which `cwd`
roots `EscalateRequest` will accept (`agent_service/schemas.py`); the example sets it to
`/projects:/app` — `/app` stays included because `configs/labmonitor_pi5.toml` and
`configs/spark4_comfy.toml` use `cwd = "/app"` for projects whose `command` only drives a Docker
container by name and never touches project files. `agent_service/Dockerfile` installs `git` and
sets `safe.directory '*'` — stage3 shells out to `git diff` directly in `cwd` to capture the code
diff, and a host-owned mounted repo would otherwise trip git's dubious-ownership check under the
container's root user.

### Python analyzer (analyzer/analyze.py)

Stateless script, stdlib-only, run fresh per incident via `uv run --project analyzer`. Reads the
incident JSON, resolves each stack frame against `--project-root`, pulls `git blame -L` for the
target line when the file is under git, and renders the Markdown report — including a full
walk-through of whichever escalation stages ran. `analyzer/pyproject.toml` has `[tool.uv] package =
false` since this is a script, not an importable package; add dependencies there if the analyzer
ever needs them.

---

## 強制：一定要派 Agent（預設，不是選項）

主 session **不准**自己搜碼、自己改碼、自己收工。使用者不應再口頭提醒。

**目的：保持主 session context 乾淨。** 搜尋結果、檔案內容、中間過程一律留在 agent context，主 session 只收整合後結論。能派就派，不要因為「看起來簡單」自己動手。

| 階段 | 必派（`.claude/agents/`） | model | 主 session 只做 |
|---|---|---|---|
| Discovery / 找檔 / 找根因 / 找既有實作 | `explorer` | claude-proxy-sonnet | 判斷要不要做、整合結論 |
| 跨檔／里程碑級設計（事件去重、非同步佇列、權限模型重做等） | `planner`（唯讀） | opus | 選方案、拆任務 |
| 實作 / bug fix / 多檔改動 | `executor` | claude-proxy-sonnet | 給範圍與驗收條件 |
| 做完驗證 | `verifier`（獨立、跨模型） | claude-proxy-gpt-5.6-terra | 最終決策 |
| 涉及 auth／API route／機密處理（`agent_service` 的 bearer-token 檢查、`src/agent_client.rs`、`[remote]` 執行、path confinement 等） | `security-review`（唯讀） | claude-proxy-gpt-5.6-terra | 整合結論 |
| 多個 agent 結果整合 | `summarizer` | claude-proxy-gemini-pro | 最終決策 |
| 文件 / CHANGELOG / Commit | `tech-writer`（唯一負責 `git commit` 的角色） | claude-proxy-gemini-3.6-flash-high | — |

內建 agent（`general-purpose`/`Explore`）只在專案 agent 尚未載入時當 fallback，且一律指定 `model: "sonnet"`，不可繼承主 session 的 Opus。

**不是例外：** 改動很小、根因已對上、主 session 剛讀過檔、單檔 bug fix。**唯一可自己動手：** typo／單行明顯修正，且回覆須寫為什麼沒派。

---

## Commit Message（Conventional Commits）

```
{type}({scope}): {簡述}
```

type：`feat` / `fix` / `docs` / `style` / `refactor` / `test` / `chore`

- 一次 commit 聚焦一件事，避免混合無關改動
- Commit 前執行 `cargo build`（Rust 側）與 `uv run pylint` / `uv run pytest`（`agent_service/` 側有改動時），確保無錯誤

### Commit 作者標記

每次 commit 訊息末尾依當前 session 的 attribution 指示附上共同作者標記（見 session 系統指示，目前為 Claude 本身的標記，而非 labreport 慣例的人類 email 標記）。
