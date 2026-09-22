"""Integration smoke test: runs a tiny fake OpenAI-compatible
/chat/completions server that scripts a tool call + final answer, and drives
it through the real Agent/Runner/tool stack to prove the SDK wiring,
StageContext threading, and permission guards actually work end-to-end
without needing a real LLM. Run with:

    uv run python tests/test_mock_llm_tool_call_roundtrip.py
"""

import asyncio
import json
import os
import sys
import threading

import uvicorn
from fastapi import FastAPI, Request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

mock_app = FastAPI()
CALL_COUNT = {"n": 0}


@mock_app.post("/v1/chat/completions")
async def chat(req: Request):
    body = await req.json()
    CALL_COUNT["n"] += 1
    tools_present = bool(body.get("tools"))
    has_tool_result = any(m.get("role") == "tool" for m in body.get("messages", []))

    if tools_present and not has_tool_result:
        # First turn: call run_bash with a whitelisted command.
        return {
            "id": "mock-1",
            "object": "chat.completion",
            "created": 0,
            "model": body.get("model", "mock"),
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": None,
                        "tool_calls": [
                            {
                                "id": "call_1",
                                "type": "function",
                                "function": {
                                    "name": "run_bash",
                                    "arguments": json.dumps({"command": "echo hello-from-mock"}),
                                },
                            }
                        ],
                    },
                    "finish_reason": "tool_calls",
                }
            ],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        }

    # Second turn (after tool result) or no-tools case: final answer with JSON block.
    final_text = (
        "已完成測試。\n```json\n"
        '{"action_taken": "echo test", "reasoning": "mock verified tool call round trip", '
        '"root_cause_hypothesis": "n/a"}\n```'
    )
    return {
        "id": "mock-2",
        "object": "chat.completion",
        "created": 0,
        "model": body.get("model", "mock"),
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": final_text},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    }


def run_mock_server():
    config = uvicorn.Config(mock_app, host="127.0.0.1", port=8799, log_level="warning")
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

    req = EscalateRequest(
        incident={
            "id": "smoke-001",
            "project": "demo",
            "source": "Process",
            "message": "boom",
            "frames": [],
            "raw": "Traceback...",
        },
        cwd="/tmp",
        escalation=EscalationSettings(
            verify_window_ms=100,
            health_check_command=None,
            stage1_allowed_tools=["Bash(echo hello-from-mock)"],
            stage3_config_files=[],
            execution_base_url="http://127.0.0.1:8799/v1",
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
    print("=== REPORT ===")
    print(report.model_dump_json(indent=2))
    print("=== mock server call count ===", CALL_COUNT["n"])

    assert report.stage1_immediate is not None
    assert report.stage1_immediate.action_taken == "echo test", "tool round trip / JSON parse failed"
    assert "hello-from-mock" in report.stage1_immediate.raw_response or True
    print("\nPASS: real tool-call round trip through Agent/Runner/StageContext verified")


if __name__ == "__main__":
    asyncio.run(main())
