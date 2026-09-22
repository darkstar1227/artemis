# Artemis

**Agent-based autoheal** — monitor server/project execution processes, detect crashes and error logs and system resource anomalies in real-time, automatically record them as structured incidents, and have multi-model AI agents make tiered judgments and attempt repairs, ultimately producing human-readable root-cause analysis reports.

Not a fixed-rule restart tool, but an agent harness that can judge for itself "what should be done now": first analyze root cause and risk, then decide whether to act, and if acting should start from the most conservative remedy and proceed all the way to temporary code-level fixes, with verification after each step to check if resolved, and never auto-commit or push.

## Features

- **Three detection sources**: process monitoring (crash / stdout·stderr error patterns), additional log files, system resources (CPU / memory / disk).
- **Four-tier staged autonomous remediation**: immediate disposition (safe restart/cleanup) → server parameter tuning → code-level temporary fixes, each tier has strict permission whitelisting, and verification occurs after each tier to check if resolved; if resolved it will not proceed to the next tier.
- **Multi-model orchestrator**: when an incident occurs, dynamically dispatch risk analysis, security analysis, quick fix analysis, log analysis, root cause analysis and other agents to make judgments, then synthesize remediation recommendations; not every incident runs through the complete pipeline.
- **Any OpenAI-compatible provider**: via [LiteLLM](https://github.com/BerriAI/litellm) or any OpenAI-compatible endpoint, different roles and tiers can each specify different models and providers.
- **Extensible to any repo**: `artemis onboard <repo>` will actually scan the target project (README, CLAUDE.md, route configurations, etc.), have AI determine safe monitoring/remediation settings and generate config files, rather than requiring you to hand-code them.
- **No Claude Code CLI needed**: both the judgment and execution layers are built on the [OpenAI Agents SDK](https://github.com/openai/openai-agents-python), with self-built file read/write/bash execution tools and tiered permission whitelisting mechanisms.

## Architecture

```
┌─────────────────────────┐        HTTP (local)        ┌──────────────────────────┐
│   Rust detection/        │ ──────────────────────────▶ │  agent_service (Python)   │
│   monitoring layer        │   POST /escalate            │  OpenAI Agents SDK        │
│   supervisor/watcher/     │ ◀────────────────────────── │  Stage 0~3 judgment +     │
│   resource → Store        │        EscalationReport      │  execution                │
└─────────────────────────┘                             └──────────────────────────┘
            │
            ▼
   incidents/<id>.json  →  analyzer/ (stdlib-only) →  incidents/<id>.md root-cause report
```

- The Rust side (`src/`) is the resident detection/monitoring layer; `artemis.toml` is the single source of truth for configuration. When an incident is detected, it calls `agent_service` over local HTTP.
- `agent_service/` (Python, managed by `uv`) is a stateless service responsible for judgment (orchestrator + specialist analysis agents) and execution (actual file/bash operations in stage1~3); each stage's Agent only receives the tools it should have, plus in-tool whitelisting checks (`agent_service/tools.py`).
- `analyzer/` (stdlib-only Python script) is responsible for converting JSON incidents into Markdown root-cause reports with source code context and `git blame`.

See [CLAUDE.md](CLAUDE.md) for detailed technical architecture, data flow, and file responsibilities.

## Installation Requirements

- Rust (`cargo`)
- Python 3.14+ and [`uv`](https://github.com/astral-sh/uv)
- An OpenAI-compatible model endpoint (e.g., self-hosted [LiteLLM](https://github.com/BerriAI/litellm) gateway, or any provider's API)

## Quick Start

```bash
# 1. Build
cargo build --release

# 2. Generate configuration for target repo (AI will read the repo and determine monitoring/remediation settings, then print a summary for confirmation)
cargo run -- onboard <repo-path>

# 3. Start agent_service (judgment + execution layer, it needs to run in the background during watch)
cd agent_service && uv run uvicorn main:app --port 8787

# 4. Open another terminal and start monitoring
cargo run -- watch --config configs/<repo-name>.toml

# View recorded incidents
cargo run -- list
cargo run -- show <incident-id>
```

## Multi-host / Multi-project / Docker

A single `artemis watch` process only monitors one `artemis.toml` (= one project). To manage multiple servers/projects simultaneously:

1. For each project, first use `artemis onboard <repo-path>` to generate its own `configs/<name>.toml` (which is a tailored monitoring/remediation setting for that project, i.e., an independent agent team).
2. All projects share the **same** `agent_service` (it is stateless by nature; judgment/execution logic depends entirely on request content).
3. To query incidents from all hosts and projects in one place, add `[central]` (`enabled = true`, pointing to the same `agent_service`) in each project's configuration — each time `Store::record` records an incident, it also pushes a copy over, regardless of whether that project has `escalation` enabled, and push failures never affect local recording. Query methods: `GET /incidents?host_id=&project=`, `GET /incidents/{host_id}/{incident_id}`.

Docker: `Dockerfile` (repo root) builds the `artemis` monitoring binary; `agent_service/Dockerfile` builds the judgment/execution/synthesis service. `docker-compose.example.yml` demonstrates a shared `agent_service` + one `artemis watch` container per project:

```bash
docker build -t artemis .
docker build -t artemis-agent-service ./agent_service
cp docker-compose.example.yml docker-compose.yml   # Copy, then modify based on number of projects
docker compose up -d
```

## Security

`agent_service` will execute bash commands and read/write files on the target repo based on requests; by default it only binds to `127.0.0.1`. If you need to expose it externally (e.g., `--host 0.0.0.0`), you must set `token_env` in `[agent_service]` and set the corresponding `ARTEMIS_AGENT_SERVICE_TOKEN` in the `agent_service` execution environment; otherwise anyone who can reach this port can make it execute arbitrary commands on this repo. Each tier (stage1~3) also has its own independent permission scope (bash whitelist / editable file list / prohibition of `git commit`·`git push`); see [CLAUDE.md](CLAUDE.md#agent_service-agent_service) for details.

## Testing

```bash
cargo build                                   # Rust side

cd agent_service
uv sync --all-groups                          # installs pytest/pylint dev deps
uv run python -c "import main"                # import/syntax check
uv run pytest tests/ -v                       # mock-LLM smoke tests
uv run pylint main.py tools.py agents_def.py pipeline.py schemas.py central.py  # lint
```

CI runs `cargo build`/`cargo test` (`.github/workflows/rust.yml`) and the `agent_service` checks
above (`.github/workflows/agent_service.yml`) on every push/PR.
