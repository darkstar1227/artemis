"""Negative-path smoke test: the mock model tries a command NOT on the
whitelist. Must be rejected by the in-tool guard, not merely absent from the
whitelist by construction. Run with:

    uv run python tests/test_mock_llm_permission_denial.py
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
SEEN_TOOL_RESULTS = []


@mock_app.post("/v1/chat/completions")
async def chat(req: Request):
    body = await req.json()
    messages = body.get("messages", [])
    tool_msgs = [m for m in messages if m.get("role") == "tool"]
    for m in tool_msgs:
        SEEN_TOOL_RESULTS.append(m.get("content"))

    if not tool_msgs:
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
                                    "arguments": json.dumps({"command": "rm -rf /"}),
                                },
                            }
                        ],
                    },
                    "finish_reason": "tool_calls",
                }
            ],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        }

    final_text = (
        "```json\n"
        '{"action_taken": "none", "reasoning": "denied as expected", "root_cause_hypothesis": "n/a"}\n```'
    )
    return {
        "id": "mock-2",
        "object": "chat.completion",
        "created": 0,
        "model": body.get("model", "mock"),
        "choices": [{"index": 0, "message": {"role": "assistant", "content": final_text}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    }


def run_mock_server():
    config = uvicorn.Config(mock_app, host="127.0.0.1", port=8800, log_level="warning")
    uvicorn.Server(config).run()


async def main():
    t = threading.Thread(target=run_mock_server, daemon=True)
    t.start()
    await asyncio.sleep(1)

    from schemas import AgentRoleConfig, AgentsConfig, EscalateRequest, EscalationSettings, OrchestratorConfig
    from pipeline import escalate

    req = EscalateRequest(
        incident={"id": "smoke-002", "project": "demo", "source": "Process", "message": "boom", "frames": [], "raw": "x"},
        cwd="/tmp",
        escalation=EscalationSettings(
            verify_window_ms=100,
            stage1_allowed_tools=["Bash(echo safe-command)"],  # does NOT include "rm -rf /"
            stage3_config_files=[],
            execution_base_url="http://127.0.0.1:8800/v1",
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
    print("=== tool results the model actually saw ===")
    for r in SEEN_TOOL_RESULTS:
        print(repr(r))

    assert SEEN_TOOL_RESULTS, "tool was never called"
    assert "權限拒絕" in SEEN_TOOL_RESULTS[0], "non-whitelisted bash command was NOT rejected!"
    print("\nPASS: non-whitelisted bash command was correctly rejected by the in-tool guard")


if __name__ == "__main__":
    asyncio.run(main())
