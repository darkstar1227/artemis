"""Integration smoke tests for the token-usage-reduction changes
(pipeline.py::TokenBudget/max_turns_per_stage):

  1. Budget exhaustion — a mock LLM that reports a huge `usage.total_tokens`
     on stage1's very first response, combined with a small
     `max_tokens_per_escalation`, must make escalate() skip stage2/stage3
     entirely (they never even call the mock server) and mark
     report.budget_exhausted / report.tokens_used accordingly, instead of
     silently continuing to spend more tokens.
  2. max_turns_per_stage — a mock LLM that always asks for another tool call
     (never emits a final answer) must be cut off once max_turns_per_stage is
     hit rather than the old hardcoded 30, so run_stage degrades gracefully
     (records a "[錯誤] ... 執行失敗" raw_response) instead of looping.
  3. Regression: stage2 edits a file via edit_file, then its own usage pushes
     the budget over the limit before stage3 gets a chance to run. escalate()
     must still capture `git diff` before returning from the "budget
     exhausted before stage3" early-return — stage2's on-disk edit must not
     be silently dropped from report.code_diff just because stage3 never ran.

Run with:
    uv run python tests/test_mock_llm_token_budget.py
    uv run pytest tests/test_mock_llm_token_budget.py -v
"""

import asyncio
import json
import os
import subprocess
import sys
import tempfile
import threading
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ---------------------------------------------------------------------------
# Scenario 1: budget exhaustion
# ---------------------------------------------------------------------------

budget_app = FastAPI()
BUDGET_CALL_COUNT = {"n": 0}
HUGE_USAGE = 5_000_000


@budget_app.post("/v1/chat/completions")
async def budget_chat(req: Request):
    body = await req.json()
    BUDGET_CALL_COUNT["n"] += 1
    final_text = (
        "\n```json\n"
        '{"action_taken": "none", "reasoning": "stage ran, reports huge usage on purpose"}\n```'
    )
    return {
        "id": "mock-budget",
        "object": "chat.completion",
        "created": 0,
        "model": body.get("model", "mock"),
        "choices": [
            {"index": 0, "message": {"role": "assistant", "content": final_text}, "finish_reason": "stop"}
        ],
        # 刻意回報一個超大 usage,模擬單一階段就把整個 escalate() 的 token 預算燒光。
        "usage": {"prompt_tokens": HUGE_USAGE // 2, "completion_tokens": HUGE_USAGE // 2, "total_tokens": HUGE_USAGE},
    }


def run_budget_mock_server():
    config = uvicorn.Config(budget_app, host="127.0.0.1", port=8803, log_level="warning")
    uvicorn.Server(config).run()


async def budget_main():
    t = threading.Thread(target=run_budget_mock_server, daemon=True)
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
            "id": "smoke-budget-001",
            "project": "demo",
            "source": "Process",
            "message": "boom",
            "frames": [],
            "raw": "Traceback...",
        },
        cwd="/tmp",
        escalation=EscalationSettings(
            verify_window_ms=100,
            health_check_command="exit 1",  # 強制回報未解決,才會繼續往 stage2/stage3 走
            stage1_allowed_tools=["Bash(echo noop)"],
            stage3_config_files=["cfg.toml"],  # 讓 stage2 在預算沒用完的情況下本來會被跑
            execution_base_url="http://127.0.0.1:8803/v1",
            max_tokens_per_escalation=1000,  # 遠小於 mock 回報的 HUGE_USAGE
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
    print("=== REPORT (budget) ===")
    print(report.model_dump_json(indent=2))
    print("=== mock server call count ===", BUDGET_CALL_COUNT["n"])

    assert report.stage1_immediate is not None and report.stage1_immediate.ran, "stage1 應該正常跑過一次"
    assert report.tokens_used is not None and report.tokens_used >= HUGE_USAGE, "tokens_used 應該累計到 stage1 的 usage"
    assert report.budget_exhausted is True, "budget_exhausted 應該被標記"
    assert report.stage2_parameter is not None and report.stage2_parameter.ran is False, (
        "stage2 應該因為預算用盡被跳過,而不是真的執行"
    )
    assert report.stage3_code_fix is not None and report.stage3_code_fix.ran is False, (
        "stage3 應該因為預算用盡被跳過,而不是真的執行"
    )
    # 只有 stage1 真的打了一次 mock server;stage2/stage3 被跳過的話根本不會再呼叫它。
    assert BUDGET_CALL_COUNT["n"] == 1, f"stage2/stage3 不應該再呼叫 LLM,實際呼叫次數={BUDGET_CALL_COUNT['n']}"
    print("\nPASS: token budget exhaustion after stage1 skips stage2/stage3 without calling the LLM again")


# ---------------------------------------------------------------------------
# Scenario 2: max_turns_per_stage is respected (not the old hardcoded 30)
# ---------------------------------------------------------------------------

turns_app = FastAPI()
TURNS_CALL_COUNT = {"n": 0}


@turns_app.post("/v1/chat/completions")
async def turns_chat(req: Request):
    body = await req.json()
    TURNS_CALL_COUNT["n"] += 1
    # 每一輪都要求再呼叫一次 run_bash,永遠不給最終答案,逼 Runner.run() 撞到
    # max_turns 上限(而不是自然結束)。
    call = {
        "id": f"call_{TURNS_CALL_COUNT['n']}",
        "type": "function",
        "function": {"name": "run_bash", "arguments": json.dumps({"command": "echo noop"})},
    }
    return {
        "id": "mock-turns",
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


def run_turns_mock_server():
    config = uvicorn.Config(turns_app, host="127.0.0.1", port=8804, log_level="warning")
    uvicorn.Server(config).run()


async def turns_main():
    t = threading.Thread(target=run_turns_mock_server, daemon=True)
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
            "id": "smoke-turns-001",
            "project": "demo",
            "source": "Process",
            "message": "boom",
            "frames": [],
            "raw": "Traceback...",
        },
        cwd="/tmp",
        escalation=EscalationSettings(
            verify_window_ms=100,
            health_check_command=None,  # 沒有健康檢查時樂觀視為已解決,stage1 跑完就結束
            stage1_allowed_tools=["Bash(echo noop)"],
            stage3_config_files=[],
            execution_base_url="http://127.0.0.1:8804/v1",
            max_turns_per_stage=3,  # 遠低於舊的硬編碼 30,mock 又永遠不給最終答案
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
    print("=== REPORT (max_turns) ===")
    print(report.model_dump_json(indent=2))
    print("=== mock server call count ===", TURNS_CALL_COUNT["n"])

    assert report.stage1_immediate is not None
    # max_turns 被撞到時 Runner.run() 會拋 MaxTurnsExceeded,run_stage 的既有
    # except Exception 分支接住它,raw_response 會是那個「執行失敗」的錯誤字串,
    # 而不是解析出 action_taken 的正常 JSON。
    assert "執行失敗" in report.stage1_immediate.raw_response, "應該因撞到 max_turns_per_stage 而中止"
    # 3 turns 上限,不應該讓 mock server 被打到舊的 30 輪那麼多次。
    assert TURNS_CALL_COUNT["n"] <= 4, f"呼叫次數應該被 max_turns_per_stage 限制住,實際={TURNS_CALL_COUNT['n']}"
    print("\nPASS: max_turns_per_stage (not the old hardcoded 30) actually bounds Runner.run()")


# ---------------------------------------------------------------------------
# Scenario 3: regression — stage2 edits a file, then budget runs out before
# stage3 runs; report.code_diff must still capture stage2's on-disk edit
# (see escalate()::_capture_diff — previously the "budget exhausted before
# stage3" early-return skipped diff capture entirely).
# ---------------------------------------------------------------------------

diff_app = FastAPI()
DIFF_CALL_COUNT = {"n": 0}
DIFF_HUGE_USAGE = 2_000_000


@diff_app.post("/v1/chat/completions")
async def diff_chat(req: Request):
    body = await req.json()
    DIFF_CALL_COUNT["n"] += 1
    tool_names = {t["function"]["name"] for t in body.get("tools", [])}
    tool_msgs = [m for m in body.get("messages", []) if m.get("role") == "tool"]
    is_stage2 = "edit_file" in tool_names

    if is_stage2 and not tool_msgs:
        # 第一輪:呼叫 edit_file 真的把檔案改掉,usage 先報一個小數字,不觸發
        # _BudgetAbortHooks(這時候累計用量還沒超過預算)。
        call = {
            "id": "call_edit",
            "type": "function",
            "function": {
                "name": "edit_file",
                "arguments": json.dumps(
                    {"path": "cfg.toml", "old_text": "timeout = 5", "new_text": "timeout = 30"}
                ),
            },
        }
        return {
            "id": "mock-diff-1",
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

    # 第二輪(stage2 的最終回答,或 stage1 唯一的一輪):usage 累計後超過
    # max_tokens_per_escalation,對 stage2 而言會被 _BudgetAbortHooks 中止;
    # 對 stage1(沒有 edit_file 工具)而言就是一次普通的小 usage 最終回答。
    total_tokens = DIFF_HUGE_USAGE if is_stage2 else 2
    final_text = (
        "\n```json\n"
        '{"action_taken": "adjust timeout", "reasoning": "stage ran and edited a file"}\n```'
    )
    return {
        "id": "mock-diff-2",
        "object": "chat.completion",
        "created": 0,
        "model": body.get("model", "mock"),
        "choices": [
            {"index": 0, "message": {"role": "assistant", "content": final_text}, "finish_reason": "stop"}
        ],
        "usage": {
            "prompt_tokens": total_tokens // 2,
            "completion_tokens": total_tokens // 2,
            "total_tokens": total_tokens,
        },
    }


def run_diff_mock_server():
    config = uvicorn.Config(diff_app, host="127.0.0.1", port=8805, log_level="warning")
    uvicorn.Server(config).run()


async def diff_main():
    t = threading.Thread(target=run_diff_mock_server, daemon=True)
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
        (d / "cfg.toml").write_text("timeout = 5\n")
        subprocess.run(["git", "init", "-q"], cwd=d, check=True)
        subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=d, check=True)
        subprocess.run(["git", "config", "user.name", "test"], cwd=d, check=True)
        subprocess.run(["git", "add", "-A"], cwd=d, check=True)
        subprocess.run(["git", "commit", "-q", "-m", "init"], cwd=d, check=True)

        req = EscalateRequest(
            incident={
                "id": "smoke-diff-001",
                "project": "demo",
                "source": "Process",
                "message": "boom",
                "frames": [],
                "raw": "Traceback...",
            },
            cwd=str(d),
            escalation=EscalationSettings(
                verify_window_ms=100,
                health_check_command="exit 1",  # 強制回報未解決,才會繼續往 stage2 走
                stage1_allowed_tools=["Bash(echo noop)"],
                stage3_config_files=["cfg.toml"],
                execution_base_url="http://127.0.0.1:8805/v1",
                max_tokens_per_escalation=100,  # 遠小於 stage2 回報的 DIFF_HUGE_USAGE
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
        print("=== REPORT (diff capture regression) ===")
        print(report.model_dump_json(indent=2))
        print("=== cfg.toml on disk ===")
        print((d / "cfg.toml").read_text())

        assert (d / "cfg.toml").read_text() == "timeout = 30\n", "stage2 應該真的把檔案改掉"
        assert report.stage2_parameter is not None and report.stage2_parameter.ran, "stage2 應該有跑過(不是被跳過)"
        assert report.budget_exhausted is True, "budget_exhausted 應該被標記"
        assert report.stage3_code_fix is not None and report.stage3_code_fix.ran is False, (
            "stage3 應該因為預算用盡被跳過"
        )
        assert report.code_diff is not None and "timeout = 30" in report.code_diff, (
            "stage2 已經寫到磁碟的修改必須出現在 report.code_diff,即使 stage3 因為預算用盡從未執行"
        )
        print("\nPASS: git diff is captured even when budget runs out before stage3 (regression fixed)")


async def main():
    await budget_main()
    await turns_main()
    await diff_main()


def test_budget_exhaustion_skips_later_stages():
    """pytest entry point — same coroutine the script runs via __main__."""
    asyncio.run(budget_main())


def test_max_turns_per_stage_respected():
    """pytest entry point — same coroutine the script runs via __main__."""
    asyncio.run(turns_main())


def test_diff_captured_when_budget_exhausted_before_stage3():
    """pytest entry point — same coroutine the script runs via __main__."""
    asyncio.run(diff_main())


if __name__ == "__main__":
    asyncio.run(main())
