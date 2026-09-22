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
`watch` smoke test against a small script that crashes/prints an error pattern. `agent_service/` has
a minimal smoke-test suite (`agent_service/tests/`, plain scripts, no pytest dependency) that spins
up a fake OpenAI-compatible `/chat/completions` server and drives the real `pipeline.escalate()`
through it — this is the only place the SDK's tool-calling round trip and the permission guards are
actually exercised against a real (mocked) LLM rather than just import-checked:

```bash
cd agent_service
uv run python -c "import main"                              # import/syntax check
uv run python tests/test_mock_llm_tool_call_roundtrip.py    # whitelisted tool call executes correctly
uv run python tests/test_mock_llm_permission_denial.py      # non-whitelisted tool call is rejected
uv run uvicorn main:app --port 8787                          # then curl -X POST /escalate — see
                                                               # agent_client.rs for the request shape
```

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
`write_file`, `run_bash`), all path-confined to `cwd`, with a `StageContext` (the Agents SDK run
context) carrying the finer-grained scoping each stage needs:

| Stage | Purpose | Tools given | In-tool guard |
|---|---|---|---|
| 1 — immediate disposition | safe mitigation (restart/cleanup) + draft root cause | `run_bash` only | `run_bash` rejects anything not exactly matching `escalation.stage1_allowed_tools` |
| 2 — parameter tuning | adjust app/server config | `read_file`, `edit_file` | `edit_file`/`write_file` reject any path not in `escalation.stage3_config_files` |
| 3 — code-level temporary fix | edit source, run tests | `read_file`, `edit_file`, `write_file`, `run_bash` | `run_bash` rejects any command containing `git commit`/`git push` |

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
`[escalation]`, `[agent_service]`, `[orchestrator]` and `[agents.*]` are all optional tables with
their own defaults, so a minimal config only needs `command`. `[agent_service]` (`url`,
`timeout_secs`, optional `token_env`) is where the Python service's HTTP endpoint lives —
`token_env` names an env var holding a bearer token, only needed if `agent_service` is bound beyond
`127.0.0.1` (see the agent_service section below). `[escalation]` no longer has
`claude_bin`/`model` (those were Claude Code CLI-specific); it instead has optional
`execution_model`/`execution_base_url`/`execution_api_key_env` for stage1~3's model, falling back to
`[orchestrator]`'s settings when unset.

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
- `tools.py` — the self-built `read_file`/`edit_file`/`write_file`/`run_bash` tool functions plus
  `StageContext` (the per-run permission scope) and the path/whitelist/git-commit guards described
  above.
- `agents_def.py` — builds `Agent`/`OpenAIChatCompletionsModel` instances from the request's config,
  one per judgment role and one per stage; `model_for()` is where per-role/per-stage
  `base_url`/`api_key_env` overrides turn into a distinct `AsyncOpenAI` client.
- `pipeline.py` — the actual Stage 0~3 flow (`escalate()`), `extract_json_block`/`incident_context`
  helpers, and `verify_resolved()`.
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
large/binary file can't blow up an agent's context window or cost.

### Python analyzer (analyzer/analyze.py)

Stateless script, stdlib-only, run fresh per incident via `uv run --project analyzer`. Reads the
incident JSON, resolves each stack frame against `--project-root`, pulls `git blame -L` for the
target line when the file is under git, and renders the Markdown report — including a full
walk-through of whichever escalation stages ran. `analyzer/pyproject.toml` has `[tool.uv] package =
false` since this is a script, not an importable package; add dependencies there if the analyzer
ever needs them.
