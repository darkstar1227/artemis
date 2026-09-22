"""Builds Agent/Model instances from the per-request config sent by Rust.

Every role/stage can point at a different OpenAI-compatible endpoint (LiteLLM
gateway or any other provider) — this is what lets a single incident fan out
across multiple providers, not just multiple models on one gateway.
"""

from __future__ import annotations

import os
from functools import lru_cache

from agents import Agent, AsyncOpenAI, ModelSettings, OpenAIChatCompletionsModel

from schemas import AgentRoleConfig, EscalationSettings, OrchestratorConfig
from tools import StageContext, edit_file, grep_files, list_dir, read_file, run_bash, write_file

JUDGMENT_ROLE_PROMPTS = {
    "risk_analysis": "你是風險分析專家 agent。針對給定的事件,評估這次修復動作可能造成的風險" \
        "(資料遺失、服務中斷、安全性衝擊等),並給出 risk_level(low/medium/high)。",
    "security_analysis": "你是安全性分析專家 agent。針對給定的事件,判斷是否有安全性疑慮" \
        "(例如敏感資訊外洩、權限問題、注入攻擊風險),並給出 risk_level(low/medium/high)。",
    "quick_fix_analysis": "你是快速修復分析專家 agent。針對給定的事件,提出最快能讓服務恢復" \
        "運作的處置方式,並評估這個處置本身的風險等級(risk_level)。",
    "log_analysis": "你是 log 分析專家 agent。針對給定的事件與原始輸出,整理出關鍵的異常" \
        "訊息與時間序列線索,協助後續根因分析。",
    "root_cause_analysis": "你是根因分析專家 agent。針對給定的事件、堆疊與原始輸出,推論最可能" \
        "的根本原因,並給出你對這個判斷的信心程度作為 risk_level(low=不確定/high=高度確定)。",
}


def _client_for(base_url: str, api_key_env: str | None) -> AsyncOpenAI:
    api_key = os.environ.get(api_key_env) if api_key_env else None
    return AsyncOpenAI(base_url=base_url, api_key=api_key or "not-needed")


@lru_cache(maxsize=64)
def _cached_client(base_url: str, api_key_env: str | None) -> AsyncOpenAI:
    return _client_for(base_url, api_key_env)


def model_for(model_name: str, base_url: str, api_key_env: str | None) -> OpenAIChatCompletionsModel:
    client = _cached_client(base_url, api_key_env)
    return OpenAIChatCompletionsModel(model=model_name, openai_client=client)


def build_judgment_agent(role: str, role_cfg: AgentRoleConfig, orch: OrchestratorConfig) -> Agent:
    base_url = role_cfg.base_url or orch.base_url
    api_key_env = role_cfg.api_key_env or orch.api_key_env
    return Agent(
        name=role,
        instructions=JUDGMENT_ROLE_PROMPTS[role]
        + "\n\n最後請務必以下列格式的 JSON 區塊結尾(不要有其他文字在區塊內):\n"
        '```json\n{"summary": "...", "risk_level": "low|medium|high"}\n```',
        model=model_for(role_cfg.model, base_url, api_key_env),
        model_settings=ModelSettings(temperature=0.2),
    )


def build_orchestrator_dispatch_agent(orch: OrchestratorConfig) -> Agent:
    roles = ", ".join(JUDGMENT_ROLE_PROMPTS.keys())
    return Agent(
        name="orchestrator_dispatch",
        instructions=(
            "你是多模型 agent harness 的 orchestrator。根據事件內容,動態決定這次需要"
            f"派出哪些分析 agent(可選:{roles}),不是每個事件都需要全部派出。\n\n"
            "最後請務必以下列格式的 JSON 區塊結尾:\n"
            '```json\n{"selected_agents": ["..."], "dispatch_reasoning": "..."}\n```'
        ),
        model=model_for(orch.model, orch.base_url, orch.api_key_env),
        model_settings=ModelSettings(temperature=0.1),
    )


def build_orchestrator_synthesis_agent(orch: OrchestratorConfig) -> Agent:
    return Agent(
        name="orchestrator_synthesis",
        instructions=(
            "你是多模型 agent harness 的 orchestrator。以下會給你各專家 agent 的分析結果,"
            "請彙整成一份最終判斷,決定建議的處置層級 recommended_stage:\n"
            "- \"none\": 不需要任何處置\n"
            "- \"stage1\": 只需要安全的立即處置(重啟/清理等)\n"
            "- \"stage2\": 需要調整伺服器/應用參數\n"
            "- \"stage3\": 需要程式碼層級的修復\n\n"
            "最後請務必以下列格式的 JSON 區塊結尾:\n"
            '```json\n{"root_cause_hypothesis": "...", "risk_level": "low|medium|high", '
            '"recommended_stage": "none|stage1|stage2|stage3", '
            '"notes_for_stage1": "...", "notes_for_stage2": "...", "notes_for_stage3": "..."}\n```'
        ),
        model=model_for(orch.model, orch.base_url, orch.api_key_env),
        model_settings=ModelSettings(temperature=0.1),
    )


STAGE_JSON_CONTRACT = (
    "最後請務必以下列格式的 JSON 區塊結尾(不要有其他文字在區塊內):\n"
    '```json\n{"action_taken": "...", "reasoning": "...", "root_cause_hypothesis": "...", '
    '"files_changed": ["..."], "test_result": "pass|fail|not_run"}\n```'
)


def build_stage_agent(stage: str, esc: EscalationSettings, orch: OrchestratorConfig) -> Agent:
    model_name = esc.execution_model or orch.model
    base_url = esc.execution_base_url or orch.base_url
    api_key_env = esc.execution_api_key_env or orch.api_key_env
    model = model_for(model_name, base_url, api_key_env)

    if stage == "stage1_immediate":
        tools = [run_bash]
        extra = "你只能使用白名單內的 Bash 指令做安全動作(例如重啟/清理暫存),絕對不能修改任何程式碼。"
    elif stage == "stage2_parameter":
        tools = [read_file, edit_file, list_dir, grep_files]
        extra = (
            "你只能編輯明確允許的設定檔,不要修改其他任何檔案或執行指令。"
            "若不確定設定檔的確切路徑或相關程式碼位置,可以用 list_dir/grep_files 先探索,"
            "不要用臆測的路徑直接呼叫 read_file/edit_file。"
        )
    else:  # stage3_code_fix
        tools = [read_file, edit_file, write_file, run_bash, list_dir, grep_files]
        extra = (
            "你可以讀寫任何相關檔案、執行指令來驗證修復,但絕對不要執行 git commit 或 git push。"
            "若不確定根因所在的確切檔案,可以先用 list_dir/grep_files 探索專案結構與相關程式碼,"
            "不要用臆測的路徑直接呼叫 read_file。"
        )

    return Agent(
        name=stage,
        instructions=(
            "你是伺服器/專案的自動維運 agent,正在執行分級自主處置的其中一層。\n"
            + extra
            + "\n\n"
            + STAGE_JSON_CONTRACT
        ),
        tools=tools,
        model=model,
        model_settings=ModelSettings(temperature=0.2),
    )
