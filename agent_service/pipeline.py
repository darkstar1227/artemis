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

from dataclasses import dataclass

from agents import RunHooks, Runner

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
# 診斷歷史(diagnostics_history,見 src/incident.rs::DiagnosticSample)一樣會被
# 嵌進每個 stage/judgment prompt,採同樣的截斷慣例。
INCIDENT_DIAGNOSTICS_MAX_CHARS = 6000
INCIDENT_DIAGNOSTICS_PER_COMMAND_MAX = 5

# 前一個 stage 交接給下一個 stage 的摘要(action_taken/reasoning/檔案清單)上限,
# 同樣是控制 token 成本的截斷慣例——這段內容本來就是給 agent「不用重新探索」的
# 提示,沒必要無上限地全部塞進去。
HANDOFF_MAX_CHARS = 2000


@dataclass
class TokenBudget:
    """單次 escalate() 呼叫(stage0 多模型分析 + stage1~3)累計 token 用量,跨越
    這次呼叫裡「每一個」Runner.run()(dispatch/judgment/synthesis/stage1~3)。
    limit <= 0 表示不限制。每個呼叫點自己在 Runner.run() 成功回傳後呼叫
    add(usage.total_tokens),下一個階段開始前呼叫 exhausted() 決定要不要跳過。"""

    limit: int
    used: int = 0

    def add(self, tokens: int) -> None:
        if tokens > 0:
            self.used += tokens

    def exhausted(self) -> bool:
        return self.limit > 0 and self.used >= self.limit


class _BudgetExceeded(Exception):
    """從 RunHooks callback 拋出,中止正在跑的 Runner.run()(見 _BudgetAbortHooks/
    run_stage)。Agents SDK 的 hook 例外會直接從 Runner.run() 傳出來,run_stage
    接住它並轉成一個「部分完成」的 StageResult,而不是讓整個 escalate() 失敗。"""


class _BudgetAbortHooks(RunHooks):
    """stage1~3 的中途 token 預算執行:這幾層可能跑很多輪/很多次工具呼叫,只在
    「階段開始前」檢查預算,擋不住單一階段自己就把整個 escalate() 的預算燒光。
    on_llm_end 在每次 LLM 呼叫結束後、該輪的工具呼叫真正執行前觸發,在這裡拋例外
    可以讓中止點落在乾淨的邊界上,而不是卡在工具呼叫中間。orchestrator/judgment
    (run_judgment_agent/analyze)都只是單次短呼叫,不值得為它們也掛 hooks,
    交給呼叫前的 budget.exhausted() 判斷就夠了。"""

    def __init__(self, budget: TokenBudget) -> None:
        self.budget = budget
        self.last_seen_total = 0

    async def on_llm_end(self, context, agent, response) -> None:  # noqa: ARG002 - SDK 介面固定簽名
        self.last_seen_total = context.usage.total_tokens
        if self.budget.limit > 0 and self.budget.used + self.last_seen_total > self.budget.limit:
            raise _BudgetExceeded(
                f"累計 token 用量 {self.budget.used + self.last_seen_total} 已超過預算 "
                f"{self.budget.limit},中止此階段"
            )


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


def _diagnostics_section(incident: dict) -> str:
    """Render incident["diagnostics_history"] (Vec<DiagnosticSample> on the
    Rust side, src/incident.rs ~40-47) grouped by command, most recent
    samples first, so the stage/judgment agents can see pre-incident resource
    trends without every sample from every poll blowing up the prompt."""
    samples = incident.get("diagnostics_history") or []
    if not samples:
        return ""

    by_command: dict[str, list[dict]] = {}
    for s in samples:
        by_command.setdefault(s.get("command", ""), []).append(s)

    lines: list[str] = []
    for command, cmd_samples in by_command.items():
        cmd_samples = sorted(cmd_samples, key=lambda s: s.get("ts_ms", 0), reverse=True)
        lines.append(f"  指令:{command}")
        for s in cmd_samples[:INCIDENT_DIAGNOSTICS_PER_COMMAND_MAX]:
            lines.append(
                f"    - ts_ms={s.get('ts_ms')} exit_code={s.get('exit_code')} "
                f"output={s.get('output', '')!r}"
            )
        if len(cmd_samples) > INCIDENT_DIAGNOSTICS_PER_COMMAND_MAX:
            lines.append(
                f"    ...(還有 {len(cmd_samples) - INCIDENT_DIAGNOSTICS_PER_COMMAND_MAX} "
                "筆較舊的樣本,已省略)"
            )

    section = "\n".join(lines)
    return "\n事故前診斷歷史:\n" + _truncate(section, INCIDENT_DIAGNOSTICS_MAX_CHARS)


def _lifecycle_line(incident: dict) -> str:
    """Milestone 1 的 lifecycle 欄位(src/incident.rs)——occurrence_count/
    first_seen/last_seen/severity/recurrence_of。舊事件 JSON 沒有這些欄位時
    整段省略,不影響既有行為。"""
    occurrence_count = incident.get("occurrence_count")
    if occurrence_count is None:
        return ""
    line = (
        f"發生次數:{occurrence_count}"
        f"(首次:{incident.get('first_seen') or '?'},最近一次:{incident.get('last_seen') or '?'})\n"
    )
    if incident.get("severity"):
        line += f"嚴重程度:{incident['severity']}\n"
    if incident.get("recurrence_of"):
        line += f"重複發生自:{incident['recurrence_of']}\n"
    return line


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
        f"{_lifecycle_line(incident)}"
        f"堆疊/相關內容:\n{frame_lines}\n"
        f"原始輸出:\n{raw}"
        f"{_diagnostics_section(incident)}"
    )


async def run_judgment_agent(role: str, req: EscalateRequest, budget: TokenBudget) -> AgentFinding:
    role_cfg = getattr(req.agents, role)
    agent = build_judgment_agent(role, role_cfg, req.orchestrator)
    prompt = f"以下是事件內容:\n\n{incident_context(req.incident)}"
    try:
        result = await asyncio.wait_for(
            Runner.run(agent, prompt), timeout=req.orchestrator.timeout_secs
        )
        budget.add(result.context_wrapper.usage.total_tokens)
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


async def analyze(req: EscalateRequest, budget: TokenBudget) -> Synthesis | None:
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
        budget.add(dispatch_result.context_wrapper.usage.total_tokens)
        dispatch_parsed = extract_json_block(dispatch_result.final_output or "")
        if not dispatch_parsed:
            return None
        selected = [r for r in dispatch_parsed.get("selected_agents", []) if r in JUDGMENT_ROLE_PROMPTS]
        if not selected:
            return None

        findings = await asyncio.gather(*(run_judgment_agent(role, req, budget) for role in selected))

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
        budget.add(synth_result.context_wrapper.usage.total_tokens)
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


def handoff_context(prev_stage: StageResult | None, prev_ctx: StageContext | None) -> str:
    """前一個 stage 執行完後,交給下一個 stage 的精簡摘要——只帶 action_taken/
    reasoning 與「讀過/改過/grep 過哪些路徑」(路徑本身,不含內容,見
    tools.py::StageContext.files_touched),讓下一個 stage 不用再重新 list_dir/
    grep_files/read_file 一輪去找前一個 stage 已經定位過的檔案。截斷到
    HANDOFF_MAX_CHARS,同樣是控制每個 stage prompt 的 token 成本。"""
    if prev_stage is None:
        return ""
    files = ", ".join(prev_ctx.files_touched) if prev_ctx and prev_ctx.files_touched else "(無)"
    block = (
        f"\n\n前一階段({prev_stage.stage})的處置摘要,請直接參考,除非必要不要重新讀取已經看過的檔案:\n"
        f"處置動作:{prev_stage.action_taken or '(無)'}\n"
        f"理由:{prev_stage.reasoning or '(無)'}\n"
        f"已檢視/修改過的檔案:{files}\n"
    )
    return _truncate(block, HANDOFF_MAX_CHARS)


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


async def run_stage(
    stage: str, req: EscalateRequest, prompt: str, stage_ctx: StageContext, budget: TokenBudget
) -> tuple[str, bool]:
    """回傳 (raw_response, budget_exhausted_mid_stage)。budget_exhausted_mid_stage
    為 True 時,raw_response 是中止當下的部分回應(通常沒有結尾的 JSON 區塊),
    escalate() 會把它當作 partial 結果記錄,並標記 report.budget_exhausted。"""
    agent = build_stage_agent(stage, req.escalation, req.orchestrator, req.remote)
    # stage2/stage3 現在多了 list_dir/grep_files 可以先探索再動手,比純粹用 read_file
    # 猜路徑多花 1~2 輪,max_turns_per_stage(預設 12,見 [escalation])留一點餘裕但
    # 仍是硬上限,避免 agent 無止盡繞圈——同時也是壓低 token 成本的第一道閘門,
    # 因為 Runner.run() 每一輪都會把目前為止的完整對話歷史重送給模型。
    timeout = max(req.orchestrator.timeout_secs, 180)
    abort_hooks = _BudgetAbortHooks(budget)
    try:
        result = await asyncio.wait_for(
            Runner.run(
                agent,
                prompt,
                context=stage_ctx,
                max_turns=req.escalation.max_turns_per_stage,
                hooks=abort_hooks,
            ),
            timeout=timeout,
        )
        budget.add(result.context_wrapper.usage.total_tokens)
        return result.final_output or "", False
    except _BudgetExceeded:
        # hook 已經把這次 run 累計到中止當下的 usage 記在 abort_hooks.last_seen_total
        # 裡(context_wrapper 本身隨著例外一起消失,拿不到了),在這裡補進 budget。
        budget.add(abort_hooks.last_seen_total)
        return (
            f"[已中止] {stage} 因為累計 token 用量超過 [escalation] max_tokens_per_escalation "
            f"預算而提早中止,以下是中止前的部分執行紀錄(可能沒有結尾的 JSON 區塊)。",
            True,
        )
    except Exception as e:  # noqa: BLE001
        return f"[錯誤] {stage} 執行失敗:{e}", False


def _mark_budget_exhausted(report: EscalationReport, stage: str, reason: str) -> StageResult:
    """後面還沒跑到的階段因為預算已經用完而整層跳過(不是中途中止,見 run_stage
    的 _BudgetExceeded 分支)——記一筆 ran=False 的 StageResult 說明原因,而不是
    悄悄漏掉不記錄。"""
    report.budget_exhausted = True
    return StageResult(stage=stage, ran=False, reasoning=reason, raw_response=reason)


@dataclass
class _EscalateState:
    """單次 escalate() 呼叫裡,每個 _run_stageN 都要用到的共用狀態,打包成一個
    物件單純是為了不讓 _run_stage1/2/3 的參數列愈疊愈長(pylint too-many-
    arguments)——不是新概念,內容跟 escalate() 局部變數一一對應。"""

    req: EscalateRequest
    cwd: Path
    synthesis: Synthesis | None
    budget: TokenBudget
    report: EscalationReport


async def _run_stage1(state: _EscalateState) -> tuple[StageResult, StageContext]:
    # stage1 一律先跑,即便預算在 stage0 就已經用完——事件才剛發生,完全不做任何
    # 立即處置不符合這支 agent 的存在目的。_BudgetAbortHooks 仍然會在中途把它砍掉。
    req, synthesis = state.req, state.synthesis
    extra = synthesis_context(synthesis, synthesis.notes_for_stage1 if synthesis else None)
    prompt = (
        f"以下是剛偵測到的事件:\n\n{incident_context(req.incident)}{extra}\n\n"
        "請判斷現在最適合的「立即處置」是什麼(例如重啟服務、清理暫存等安全動作),"
        "只能使用允許的 Bash 指令白名單來執行,絕對不要修改任何程式碼。"
        "若判斷不需要任何動作,action_taken 請填 \"none\"。"
        "同時對這個事件做初步根因分析(若上面已經有多模型分析結果,直接沿用並視需要補充即可)。"
    )
    ctx = StageContext(
        cwd=state.cwd,
        stage="stage1_immediate",
        bash_whitelist=req.escalation.stage1_allowed_tools,
        remote_enabled=req.remote.enabled,
        remote_device=req.remote.device_id,
        sanc_bin=req.remote.sanc_bin,
        remote_state_dir=req.remote.state_dir,
        remote_timeout_secs=req.remote.timeout_secs,
        remote_allowed_commands=req.remote.allowed_commands,
    )
    raw, budget_hit = await run_stage("stage1_immediate", req, prompt, ctx, state.budget)
    stage = parse_stage("stage1_immediate", raw)
    state.report.budget_exhausted = state.report.budget_exhausted or budget_hit
    stage.verified_resolved = await asyncio.to_thread(verify_resolved, req)
    return stage, ctx


async def _run_stage2(
    state: _EscalateState, prev_stage: StageResult, prev_ctx: StageContext
) -> tuple[StageResult, StageContext]:
    req, synthesis = state.req, state.synthesis
    files_list = ", ".join(req.escalation.stage3_config_files)
    extra = synthesis_context(synthesis, synthesis.notes_for_stage2 if synthesis else None)
    prompt = (
        f"第一層的立即處置沒有解決問題,以下是事件內容:\n\n{incident_context(req.incident)}{extra}"
        f"{handoff_context(prev_stage, prev_ctx)}\n\n"
        "請判斷是否需要調整伺服器/應用程式的設定參數來解決此問題(例如逾時時間、"
        "連線池大小、記憶體限制等)。你只能編輯以下設定檔,不要修改其他任何檔案:\n"
        f"{files_list}\n\n若判斷不需要調整,action_taken 請填 \"none\"。"
    )
    ctx = StageContext(
        cwd=state.cwd, stage="stage2_parameter", editable_files=req.escalation.stage3_config_files
    )
    raw, budget_hit = await run_stage("stage2_parameter", req, prompt, ctx, state.budget)
    stage = parse_stage("stage2_parameter", raw)
    state.report.budget_exhausted = state.report.budget_exhausted or budget_hit
    stage.verified_resolved = await asyncio.to_thread(verify_resolved, req)
    return stage, ctx


async def _run_stage3(state: _EscalateState, prev_stage: StageResult, prev_ctx: StageContext) -> StageResult:
    req, synthesis = state.req, state.synthesis
    test_note = (
        f"修復後請執行測試指令驗證:`{req.escalation.stage4_test_command}`,並回報 test_result 為 pass/fail/not_run。"
        if req.escalation.stage4_test_command
        else "沒有設定測試指令,請自行以靜態檢查方式確認修改合理,test_result 填 not_run。"
    )
    extra = synthesis_context(synthesis, synthesis.notes_for_stage3 if synthesis else None)
    prompt = (
        f"前面的即時處置與參數調整都沒有解決問題,以下是完整事件內容:\n\n{incident_context(req.incident)}{extra}"
        f"{handoff_context(prev_stage, prev_ctx)}\n\n"
        "請深入分析根因,並直接在程式碼中寫入暫時修復(可以讀寫任何相關檔案),"
        "但絕對不要執行任何 git commit 或 git push,修改完就停止,交由開發者審查。\n"
        f"{test_note}"
    )
    ctx = StageContext(
        cwd=state.cwd,
        stage="stage3_code_fix",
        forbid_git_commit_push=True,
        remote_enabled=req.remote.enabled,
        remote_device=req.remote.device_id,
        sanc_bin=req.remote.sanc_bin,
        remote_state_dir=req.remote.state_dir,
        remote_timeout_secs=req.remote.timeout_secs,
    )
    raw, budget_hit = await run_stage("stage3_code_fix", req, prompt, ctx, state.budget)
    stage = parse_stage("stage3_code_fix", raw)
    state.report.budget_exhausted = state.report.budget_exhausted or budget_hit
    stage.verified_resolved = await asyncio.to_thread(verify_resolved, req)
    return stage


def _capture_diff(req: EscalateRequest, report: EscalationReport) -> None:
    """stage2(edit_file/write_file 對 stage3_config_files)與 stage3(可讀寫任何
    檔案)都可能在磁碟上留下未 commit 的變更——只要其中任一層「已經跑過」,不管
    escalate() 從哪個 return 點結束,都要在結束前把 git diff 補進 report,
    否則 stage2 已經寫好的修改會留在磁碟上,但報告卻顯示沒有任何 code_diff。"""
    diff = subprocess.run(["git", "-C", req.cwd, "diff"], capture_output=True, text=True)
    if diff.stdout.strip():
        report.code_diff = diff.stdout


async def escalate(req: EscalateRequest) -> EscalationReport:
    report = EscalationReport()
    budget = TokenBudget(limit=req.escalation.max_tokens_per_escalation)

    synthesis = await analyze(req, budget)
    report.multi_agent_analysis = synthesis

    if synthesis and synthesis.recommended_stage == "none":
        report.final_resolved = True
        report.tokens_used = budget.used
        return report

    state = _EscalateState(req=req, cwd=Path(req.cwd), synthesis=synthesis, budget=budget, report=report)

    stage1, stage1_ctx = await _run_stage1(state)
    report.stage1_immediate = stage1
    if stage1.verified_resolved:
        # stage1 只有 run_bash(白名單)可用,沒有 edit_file/write_file,理論上
        # 不會留下需要 diff 的變更,這裡不用特地補 _capture_diff()。
        report.final_resolved = True
        report.tokens_used = budget.used
        return report

    # 上一個「跑過」的 stage 的結果/context 會被帶到下一層當作 handoff(見
    # handoff_context),讓後面的 stage 不用重新探索前一層已經找過的檔案。
    prev_stage, prev_ctx = stage1, stage1_ctx

    # ---- Stage 2:伺服器/應用參數調整 ----
    if req.escalation.stage3_config_files:
        if budget.exhausted():
            report.stage2_parameter = _mark_budget_exhausted(
                report, "stage2_parameter", "token 預算已用盡(max_tokens_per_escalation),跳過此階段"
            )
        else:
            stage2, stage2_ctx = await _run_stage2(state, prev_stage, prev_ctx)
            report.stage2_parameter = stage2
            prev_stage, prev_ctx = stage2, stage2_ctx
            if stage2.verified_resolved:
                report.final_resolved = True
                _capture_diff(req, report)
                report.tokens_used = budget.used
                return report

    # ---- Stage 3:程式碼層級暫時修復(不自動 commit)----
    if budget.exhausted():
        report.stage3_code_fix = _mark_budget_exhausted(
            report, "stage3_code_fix", "token 預算已用盡(max_tokens_per_escalation),跳過此階段"
        )
        # 走到這裡代表 stage2 有機會已經跑過(見上面的 if req.escalation.stage3_config_files
        # 區塊)並且可能已經 edit_file/write_file 過——即使 stage3 因為預算用盡被跳過,
        # 已經寫到磁碟的 stage2 變更還是要被記進 code_diff,不能因為提早 return 而漏掉。
        _capture_diff(req, report)
        report.tokens_used = budget.used
        return report

    stage3 = await _run_stage3(state, prev_stage, prev_ctx)
    report.final_resolved = stage3.verified_resolved or False
    report.stage3_code_fix = stage3

    _capture_diff(req, report)
    report.tokens_used = budget.used
    return report
