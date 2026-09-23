"""Integration smoke test for the remote_exec tool (SessAnchor/`sanc` backend):
scripts a mock LLM that has stage1 call remote_exec once (forcing fallthrough),
then has stage3 call remote_exec again, and drives it through the real
Agent/Runner/tool stack. `sanc` itself is not assumed to be installed/configured
in CI, so `subprocess.run` is monkeypatched to fake `sanc session create` /
`sanc exec` and assert the real tool builds the expected argv (session id,
request-id, command) rather than actually shelling out. Run with:

    uv run python tests/test_mock_llm_remote_exec.py
"""

import asyncio
import json
import os
import sys
import tempfile
import threading
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

mock_app = FastAPI()
SEEN_TOOL_RESULTS: list[str] = []
SANC_CALLS: list[list[str]] = []


@mock_app.post("/v1/chat/completions")
async def chat(req: Request):
    body = await req.json()
    messages = body.get("messages", [])
    tool_names = {t["function"]["name"] for t in body.get("tools", [])}
    tool_msgs = [m for m in messages if m.get("role") == "tool"]
    for m in tool_msgs:
        if m.get("content") not in SEEN_TOOL_RESULTS:
            SEEN_TOOL_RESULTS.append(m.get("content"))

    is_stage3 = "list_dir" in tool_names

    if is_stage3:
        if len(tool_msgs) == 0:
            call = {
                "id": "call_stage3_remote",
                "type": "function",
                "function": {
                    "name": "remote_exec",
                    "arguments": json.dumps({"command": "systemctl status myapp"}),
                },
            }
            return _tool_call_response(body, call)

        final_text = (
            "\n```json\n"
            '{"action_taken": "checked remote status via remote_exec", "reasoning": "ok", '
            '"test_result": "not_run"}\n```'
        )
        return _final_response(body, final_text)

    # stage1: first try a non-whitelisted command (should be denied), then the
    # whitelisted one, then report unresolved so the pipeline falls through.
    if len(tool_msgs) == 0:
        call = {
            "id": "call_stage1_remote_denied",
            "type": "function",
            "function": {"name": "remote_exec", "arguments": json.dumps({"command": "rm -rf /"})},
        }
        return _tool_call_response(body, call)
    if len(tool_msgs) == 1:
        call = {
            "id": "call_stage1_remote",
            "type": "function",
            "function": {
                "name": "remote_exec",
                "arguments": json.dumps({"command": "systemctl restart myapp"}),
            },
        }
        return _tool_call_response(body, call)

    final_text = (
        "\n```json\n"
        '{"action_taken": "restarted via remote_exec", "reasoning": "stage1 remote no-op, forcing fallthrough"}\n```'
    )
    return _final_response(body, final_text)


def _tool_call_response(body: dict, call: dict) -> dict:
    return {
        "id": "mock",
        "object": "chat.completion",
        "created": 0,
        "model": body.get("model", "mock"),
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": None, "tool_calls": [call]},
                "finish_reason": "tool_calls",
            }
        ],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    }


def _final_response(body: dict, text: str) -> dict:
    return {
        "id": "mock-final",
        "object": "chat.completion",
        "created": 0,
        "model": body.get("model", "mock"),
        "choices": [{"index": 0, "message": {"role": "assistant", "content": text}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    }


def run_mock_server():
    config = uvicorn.Config(mock_app, host="127.0.0.1", port=8802, log_level="warning")
    server = uvicorn.Server(config)
    asyncio.run(server.serve())


class _FakeCompleted:
    def __init__(self, argv: list[str]):
        self.returncode = 0
        if argv[1:3] == ["session", "create"]:
            self.stdout = "session created (fake)\n"
        else:
            self.stdout = "fake remote stdout\n"
        self.stderr = ""


def _fake_sanc(ctx, *args):
    argv = [ctx.sanc_bin, *args]
    SANC_CALLS.append(argv)
    return _FakeCompleted(argv)


async def main():
    import tools

    real_sanc = tools._sanc  # noqa: SLF001 - test-only monkeypatch, restored below
    tools._sanc = _fake_sanc  # noqa: SLF001
    try:
        t = threading.Thread(target=run_mock_server, daemon=True)
        t.start()
        await asyncio.sleep(1)

        from schemas import (
            AgentRoleConfig,
            AgentsConfig,
            EscalateRequest,
            EscalationSettings,
            OrchestratorConfig,
            RemoteConfig,
        )
        from pipeline import escalate

        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)

            req = EscalateRequest(
                incident={
                    "id": "smoke-remote-001",
                    "project": "demo",
                    "source": "Process",
                    "message": "boom",
                    "frames": [],
                    "raw": "Traceback...",
                },
                cwd=str(d),
                escalation=EscalationSettings(
                    verify_window_ms=100,
                    health_check_command="exit 1",  # forces "unresolved" so stage1 falls through to stage3
                    stage1_allowed_tools=[],
                    stage3_config_files=[],
                    execution_base_url="http://127.0.0.1:8802/v1",
                ),
                orchestrator=OrchestratorConfig(enabled=False),
                agents=AgentsConfig(
                    risk_analysis=AgentRoleConfig(model="mock"),
                    security_analysis=AgentRoleConfig(model="mock"),
                    quick_fix_analysis=AgentRoleConfig(model="mock"),
                    log_analysis=AgentRoleConfig(model="mock"),
                    root_cause_analysis=AgentRoleConfig(model="mock"),
                ),
                remote=RemoteConfig(
                    enabled=True,
                    device_id="test-device",
                    allowed_commands=["Bash(systemctl restart myapp)"],
                ),
            )

            report = await escalate(req)
            print("=== tool results seen ===")
            for r in SEEN_TOOL_RESULTS:
                print(repr(r))
            print("=== sanc argv seen ===")
            for c in SANC_CALLS:
                print(c)

            assert report.stage3_code_fix is not None, "stage3 did not run"
            assert any("device=test-device" in r for r in SEEN_TOOL_RESULTS), "remote_exec result missing"
            assert any("request_id=" in r for r in SEEN_TOOL_RESULTS), "request_id missing from remote_exec output"

            create_calls = [c for c in SANC_CALLS if c[1:3] == ["session", "create"]]
            exec_calls = [c for c in SANC_CALLS if "exec" in c]
            assert create_calls, "sanc session create was never invoked"
            assert exec_calls, "sanc exec was never invoked"
            assert any("systemctl restart myapp" in c for c in exec_calls[0]), "stage1 command not forwarded to sanc exec"
            assert any(
                "systemctl status myapp" in c for c in exec_calls[-1]
            ), "stage3 command not forwarded to sanc exec"

            assert any(
                "[權限拒絕]" in r for r in SEEN_TOOL_RESULTS
            ), "non-whitelisted stage1 remote_exec command ('rm -rf /') was not denied"
            assert not any("rm -rf" in c for c in exec_calls), "denied command reached sanc exec"

            print("\nPASS: remote_exec reachable from stage1/stage3, forwards request-id, and enforces whitelisting")
    finally:
        tools._sanc = real_sanc  # noqa: SLF001


def test_remote_exec():
    """pytest entry point — same coroutine the script runs via __main__."""
    asyncio.run(main())


if __name__ == "__main__":
    asyncio.run(main())
