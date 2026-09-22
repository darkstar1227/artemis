"""Full escalation pipeline: Stage 0 (multi-model orchestrator dispatch +
synthesis) followed by Stage 1~3 staged execution with verification between
each step. This replaces src/orchestrator.rs + src/healer.rs — both judgment
and execution now go through the OpenAI Agents SDK instead of shelling out
to Claude Code CLI.
"""

from __future__ import annotations

import asyncio
import json
import re
import subprocess
import time
from pathlib import Path

from agents import Runner

from agents_def import (
    JUDGMENT_ROLE_PROMPTS,
    build_judgment_agent,
    build_orchestrator_dispatch_agent,
    build_orchestrator_synthesis_agent,
    build_stage_agent,
)
from schemas import (
    AgentFinding,
    EscalateRequest,
    EscalationReport,
    StageResult,
    Synthesis,
)
from tools import StageContext

_JSON_BLOCK_RE = re.compile(r"```json(.*?)```", re.DOTALL)

# 事件內容會被嵌進「每一個」stage/judgment agent 的 prompt 裡(stage0 分析 + stage1~3
# 執行,各自都是獨立的 Runner.run 呼叫),沒有上限的話一個巨大的原始輸出或超長堆疊會
# 讓每一次呼叫都重複付出同樣的 context/token 成本。上限與 tools.py::READ_FILE_MAX_CHARS
# 採同樣量級,同樣附上可辨識的截斷標記而不是靜默丟資料。
INCIDENT_RAW_MAX_CHARS = 20000
INCIDENT_FRAMES_MAX = 50


def _truncate(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars]}\n\n[已截斷,原始長度 {len(text)} 字元,只顯示前 {max_chars} 字元]"


def extract_json_block(text: str) -> dict | None:
    """Grab the LAST fenced ```json block in the text — same convention the
    Rust side used (healer::extract_json_block / ai::extract_json_block)."""
    blocks = _JSON_BLOCK_RE.findall(text)
    if not blocks:
        return None
    try:
        return json.loads(blocks[-1].strip())
    except json.JSONDecodeError:
        return None


def incident_context(incident: dict) -> str:
    frames = incident.get("frames") or []
    frame_lines = "\n".join(f"  - {f.get('raw', '')}" for f in frames[:INCIDENT_FRAMES_MAX])
    if len(frames) > INCIDENT_FRAMES_MAX:
        frame_lines += f"\n  ...(還有 {len(frames) - INCIDENT_FRAMES_MAX} 行堆疊/事件內容,已截斷)"
    raw = _truncate(incident.get("raw", "") or "", INCIDENT_RAW_MAX_CHARS)
    return (
        f"專案:{incident.get('project')}\n"
        f"來源:{incident.get('source')}\n"
        f"錯誤訊息:{incident.get('message')}\n"
        f"堆疊/相關內容:\n{frame_lines}\n"
        f"原始輸出:\n{raw}"
    )


async def run_judgment_agent(role: str, req: EscalateRequest) -> AgentFinding:
    role_cfg = getattr(req.agents, role)
    agent = build_judgment_agent(role, role_cfg, req.orchestrator)
    prompt = f"以下是事件內容:\n\n{incident_context(req.incident)}"
    try:
        result = await asyncio.wait_for(
            Runner.run(agent, prompt), timeout=req.orchestrator.timeout_secs
        )
        raw = result.final_output or ""
    except Exception as e:  # noqa: BLE001 - degrade gracefully, never block the pipeline
        raw = f"[錯誤] {role} 呼叫失敗:{e}"

    parsed = extract_json_block(raw) or {}
    return AgentFinding(
        role=role,
        summary=parsed.get("summary") or raw[:2000],
        risk_level=parsed.get("risk_level"),
        raw_response=raw,
    )


async def analyze(req: EscalateRequest) -> Synthesis | None:
    """Stage 0: dynamic dispatch → parallel judgment → synthesis. Returns
    None on any failure so the caller can gracefully fall back to stage1
    running cold, exactly like the old orchestrator::analyze() did."""
    if not req.orchestrator.enabled:
        return None

    try:
        dispatch_agent = build_orchestrator_dispatch_agent(req.orchestrator)
        dispatch_prompt = f"以下是事件內容:\n\n{incident_context(req.incident)}"
        dispatch_result = await asyncio.wait_for(
            Runner.run(dispatch_agent, dispatch_prompt), timeout=req.orchestrator.timeout_secs
        )
        dispatch_parsed = extract_json_block(dispatch_result.final_output or "")
        if not dispatch_parsed:
            return None
        selected = [r for r in dispatch_parsed.get("selected_agents", []) if r in JUDGMENT_ROLE_PROMPTS]
        if not selected:
            return None

        findings = await asyncio.gather(*(run_judgment_agent(role, req) for role in selected))

        synth_agent = build_orchestrator_synthesis_agent(req.orchestrator)
        findings_text = "\n".join(
            f"- [{f.role}] risk_level={f.risk_level or 'unknown'}: {f.summary}" for f in findings
        )
        synth_prompt = (
            f"事件內容:\n\n{incident_context(req.incident)}\n\n各專家 agent 的分析結果:\n{findings_text}"
        )
        synth_result = await asyncio.wait_for(
            Runner.run(synth_agent, synth_prompt), timeout=req.orchestrator.timeout_secs
        )
        synth_parsed = extract_json_block(synth_result.final_output or "")
        if not synth_parsed:
            return None

        return Synthesis(
            selected_agents=selected,
            dispatch_reasoning=dispatch_parsed.get("dispatch_reasoning"),
            findings=list(findings),
            root_cause_hypothesis=synth_parsed.get("root_cause_hypothesis"),
            risk_level=synth_parsed.get("risk_level"),
            recommended_stage=synth_parsed.get("recommended_stage", "stage1"),
            notes_for_stage1=synth_parsed.get("notes_for_stage1"),
            notes_for_stage2=synth_parsed.get("notes_for_stage2"),
            notes_for_stage3=synth_parsed.get("notes_for_stage3"),
        )
    except Exception:  # noqa: BLE001 - orchestrator is optional, never blocks the pipeline
        return None


def synthesis_context(synthesis: Synthesis | None, stage_notes: str | None) -> str:
    if not synthesis or not synthesis.selected_agents:
        return ""
    findings = "\n".join(
        f"  - [{f.role}] risk_level={f.risk_level or 'unknown'}: {f.summary}" for f in synthesis.findings
    )
    notes = f"此階段重點提示:{stage_notes}" if stage_notes else ""
    return (
        "\n\n以下是多模型 agent harness 已經做過的分析,請直接參考,不用重新分析:\n"
        f"根因初判:{synthesis.root_cause_hypothesis or '(無)'}\n"
        f"整體風險等級:{synthesis.risk_level or 'unknown'}\n"
        f"各專家 agent 意見:\n{findings}\n{notes}"
    )


def parse_stage(stage: str, raw: str) -> StageResult:
    parsed = extract_json_block(raw) or {}
    return StageResult(
        stage=stage,
        ran=True,
        action_taken=parsed.get("action_taken"),
        reasoning=parsed.get("reasoning"),
        root_cause_hypothesis=parsed.get("root_cause_hypothesis"),
        files_changed=parsed.get("files_changed") or [],
        test_result=parsed.get("test_result"),
        raw_response=raw,
    )


def verify_resolved(req: EscalateRequest) -> bool:
    time.sleep(req.escalation.verify_window_ms / 1000)
    check_cmd = req.escalation.health_check_command
    if not check_cmd:
        return True
    try:
        result = subprocess.run(check_cmd, shell=True, cwd=req.cwd, timeout=30)
        return result.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


async def run_stage(stage: str, req: EscalateRequest, prompt: str, stage_ctx: StageContext) -> str:
    agent = build_stage_agent(stage, req.escalation, req.orchestrator)
    # stage2/stage3 現在多了 list_dir/grep_files 可以先探索再動手,比純粹用 read_file
    # 猜路徑多花 1~2 輪,30 留一點餘裕但仍是硬上限,避免 agent 無止盡繞圈。
    timeout = max(req.orchestrator.timeout_secs, 180)
    try:
        result = await asyncio.wait_for(
            Runner.run(agent, prompt, context=stage_ctx, max_turns=30), timeout=timeout
        )
        return result.final_output or ""
    except Exception as e:  # noqa: BLE001
        return f"[錯誤] {stage} 執行失敗:{e}"


async def escalate(req: EscalateRequest) -> EscalationReport:
    report = EscalationReport()
    cwd = Path(req.cwd)

    synthesis = await analyze(req)
    report.multi_agent_analysis = synthesis

    if synthesis and synthesis.recommended_stage == "none":
        report.final_resolved = True
        return report

    # ---- Stage 1:即時處置 + 根因初判 ----
    extra = synthesis_context(synthesis, synthesis.notes_for_stage1 if synthesis else None)
    stage1_prompt = (
        f"以下是剛偵測到的事件:\n\n{incident_context(req.incident)}{extra}\n\n"
        "請判斷現在最適合的「立即處置」是什麼(例如重啟服務、清理暫存等安全動作),"
        "只能使用允許的 Bash 指令白名單來執行,絕對不要修改任何程式碼。"
        "若判斷不需要任何動作,action_taken 請填 \"none\"。"
        "同時對這個事件做初步根因分析(若上面已經有多模型分析結果,直接沿用並視需要補充即可)。"
    )
    stage1_ctx = StageContext(
        cwd=cwd, stage="stage1_immediate", bash_whitelist=req.escalation.stage1_allowed_tools
    )
    raw1 = await run_stage("stage1_immediate", req, stage1_prompt, stage1_ctx)
    stage1 = parse_stage("stage1_immediate", raw1)
    resolved_after_1 = await asyncio.to_thread(verify_resolved, req)
    stage1.verified_resolved = resolved_after_1
    report.stage1_immediate = stage1

    if resolved_after_1:
        report.final_resolved = True
        return report

    # ---- Stage 2:伺服器/應用參數調整 ----
    if req.escalation.stage3_config_files:
        files_list = ", ".join(req.escalation.stage3_config_files)
        extra = synthesis_context(synthesis, synthesis.notes_for_stage2 if synthesis else None)
        stage2_prompt = (
            f"第一層的立即處置沒有解決問題,以下是事件內容:\n\n{incident_context(req.incident)}{extra}\n\n"
            f"前一層的處置與理由:{stage1.reasoning!r}\n\n"
            "請判斷是否需要調整伺服器/應用程式的設定參數來解決此問題(例如逾時時間、"
            "連線池大小、記憶體限制等)。你只能編輯以下設定檔,不要修改其他任何檔案:\n"
            f"{files_list}\n\n若判斷不需要調整,action_taken 請填 \"none\"。"
        )
        stage2_ctx = StageContext(
            cwd=cwd, stage="stage2_parameter", editable_files=req.escalation.stage3_config_files
        )
        raw2 = await run_stage("stage2_parameter", req, stage2_prompt, stage2_ctx)
        stage2 = parse_stage("stage2_parameter", raw2)
        resolved_after_2 = await asyncio.to_thread(verify_resolved, req)
        stage2.verified_resolved = resolved_after_2
        report.stage2_parameter = stage2

        if resolved_after_2:
            report.final_resolved = True
            return report

    # ---- Stage 3:程式碼層級暫時修復(不自動 commit)----
    test_note = (
        f"修復後請執行測試指令驗證:`{req.escalation.stage4_test_command}`,並回報 test_result 為 pass/fail/not_run。"
        if req.escalation.stage4_test_command
        else "沒有設定測試指令,請自行以靜態檢查方式確認修改合理,test_result 填 not_run。"
    )
    extra = synthesis_context(synthesis, synthesis.notes_for_stage3 if synthesis else None)
    stage3_prompt = (
        f"前面的即時處置與參數調整都沒有解決問題,以下是完整事件內容:\n\n{incident_context(req.incident)}{extra}\n\n"
        "請深入分析根因,並直接在程式碼中寫入暫時修復(可以讀寫任何相關檔案),"
        "但絕對不要執行任何 git commit 或 git push,修改完就停止,交由開發者審查。\n"
        f"{test_note}"
    )
    stage3_ctx = StageContext(cwd=cwd, stage="stage3_code_fix", forbid_git_commit_push=True)
    raw3 = await run_stage("stage3_code_fix", req, stage3_prompt, stage3_ctx)
    stage3 = parse_stage("stage3_code_fix", raw3)
    resolved_after_3 = await asyncio.to_thread(verify_resolved, req)
    stage3.verified_resolved = resolved_after_3
    report.final_resolved = resolved_after_3
    report.stage3_code_fix = stage3

    diff = subprocess.run(
        ["git", "-C", req.cwd, "diff"], capture_output=True, text=True
    )
    if diff.stdout.strip():
        report.code_diff = diff.stdout

    return report
