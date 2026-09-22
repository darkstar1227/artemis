"""Integration smoke test for the exploration tools (list_dir/grep_files) added
to stage3: scripts a mock LLM that, once it sees the stage3 tool set, calls
list_dir then grep_files then answers, and drives it through the real
Agent/Runner/tool stack against a real temp project directory to prove both
tools are actually reachable/wired for stage3 and return real (not stubbed)
results. stage1 (only has run_bash) is scripted to run a whitelisted no-op and
report unresolved, so the pipeline falls through to stage3. Run with:

    uv run python tests/test_mock_llm_explore_tools.py
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
                "id": "call_1",
                "type": "function",
                "function": {"name": "list_dir", "arguments": json.dumps({"path": "."})},
            }
        elif len(tool_msgs) == 1:
            call = {
                "id": "call_2",
                "type": "function",
                "function": {
                    "name": "grep_files",
                    "arguments": json.dumps({"pattern": "raise ValueError", "path": "."}),
                },
            }
        else:
            call = None

        if call is not None:
            return _tool_call_response(body, call)

        final_text = (
            "探索完成。\n```json\n"
            '{"action_taken": "none", "reasoning": "explored via list_dir/grep_files", '
            '"root_cause_hypothesis": "found in b.py", "test_result": "not_run"}\n```'
        )
        return _final_response(body, final_text)

    # stage1: run a harmless whitelisted bash no-op, then report unresolved.
    if len(tool_msgs) == 0:
        call = {
            "id": "call_stage1",
            "type": "function",
            "function": {"name": "run_bash", "arguments": json.dumps({"command": "echo noop"})},
        }
        return _tool_call_response(body, call)

    final_text = (
        "\n```json\n"
        '{"action_taken": "none", "reasoning": "stage1 no-op, forcing fallthrough"}\n```'
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
    config = uvicorn.Config(mock_app, host="127.0.0.1", port=8801, log_level="warning")
    server = uvicorn.Server(config)
    asyncio.run(server.serve())


async def main():
    t = threading.Thread(target=run_mock_server, daemon=True)
    t.start()
    await asyncio.sleep(1)

    from schemas import (
        AgentRoleConfig,
        AgentsConfig,
        EscalateRequest,
        EscalationSettings,
        OrchestratorConfig,
    )
    from pipeline import escalate

    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        (d / "sub").mkdir()
        (d / "sub" / "b.py").write_text('def handler():\n    raise ValueError("boom")\n')

        req = EscalateRequest(
            incident={
                "id": "smoke-explore-001",
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
                stage1_allowed_tools=["Bash(echo noop)"],
                stage3_config_files=[],
                execution_base_url="http://127.0.0.1:8801/v1",
            ),
            orchestrator=OrchestratorConfig(enabled=False),
            agents=AgentsConfig(
                risk_analysis=AgentRoleConfig(model="mock"),
                security_analysis=AgentRoleConfig(model="mock"),
                quick_fix_analysis=AgentRoleConfig(model="mock"),
                log_analysis=AgentRoleConfig(model="mock"),
                root_cause_analysis=AgentRoleConfig(model="mock"),
            ),
        )

        report = await escalate(req)
        print("=== stage3 raw_response ===")
        print(report.stage3_code_fix.raw_response if report.stage3_code_fix else None)
        print("=== tool results seen ===")
        for r in SEEN_TOOL_RESULTS:
            print(repr(r))

        assert report.stage3_code_fix is not None, "stage3 did not run"
        assert any("sub/" in r for r in SEEN_TOOL_RESULTS), "list_dir result missing/wrong"
        assert any("b.py" in r and "ValueError" in r for r in SEEN_TOOL_RESULTS), "grep_files result missing/wrong"
        print("\nPASS: stage3 agent successfully used list_dir and grep_files against a real temp project")


if __name__ == "__main__":
    asyncio.run(main())
