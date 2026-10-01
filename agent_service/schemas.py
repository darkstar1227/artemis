"""HTTP wire contract between Rust (src/agent_client.rs) and this service.

Field names deliberately mirror the Rust structs (src/incident.rs, src/config.rs)
so the JSON on both sides stays a straightforward 1:1 mapping.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


class AgentRoleConfig(BaseModel):
    model: str
    base_url: Optional[str] = None
    api_key_env: Optional[str] = None


class OrchestratorConfig(BaseModel):
    enabled: bool = False
    base_url: str = "http://localhost:4000/v1"
    api_key_env: Optional[str] = None
    model: str = "your-orchestrator-model"
    timeout_secs: int = 60


class AgentsConfig(BaseModel):
    risk_analysis: AgentRoleConfig
    security_analysis: AgentRoleConfig
    quick_fix_analysis: AgentRoleConfig
    log_analysis: AgentRoleConfig
    root_cause_analysis: AgentRoleConfig


class RemoteConfig(BaseModel):
    """Remote-execution backend via SessAnchor (`sanc`) — lets stage1/stage3
    run commands against a configured remote host instead of only local `cwd`.
    Kept separate from [escalation] since it's an optional add-on, not part
    of the core staged-remediation contract."""

    enabled: bool = False
    device_id: Optional[str] = None
    sanc_bin: str = "sanc"
    state_dir: Optional[str] = None
    timeout_secs: int = 120
    # stage1 呼叫 remote_exec 時比照 stage1_allowed_tools 的白名單語法
    # ("Bash(實際指令)"),空清單 = 這個階段不能用 remote_exec。
    allowed_commands: list[str] = Field(default_factory=list)


class EscalationSettings(BaseModel):
    """The subset of [escalation] that governs stage1~3 execution + verification."""

    verify_window_ms: int = 15000
    health_check_command: Optional[str] = None
    stage1_allowed_tools: list[str] = Field(default_factory=list)
    stage3_config_files: list[str] = Field(default_factory=list)
    stage4_test_command: Optional[str] = None
    # 執行層(stage1~3)使用的模型/provider。未設定 execution_* 時退回 orchestrator 的設定。
    execution_model: Optional[str] = None
    execution_base_url: Optional[str] = None
    execution_api_key_env: Optional[str] = None
    # stage1~3 每一層 Runner.run() 的 max_turns 上限——每一輪都會把完整對話歷史
    # 重送給模型,調低能壓低 token 成本(見 pipeline.py::run_stage)。
    max_turns_per_stage: int = 12
    # 單次 escalate() 呼叫(stage0 分析 + stage1~3)累計可用的 token 預算,
    # 0 表示不限制(見 pipeline.py::TokenBudget)。
    max_tokens_per_escalation: int = 1_500_000

    @field_validator("max_turns_per_stage")
    @classmethod
    def _validate_max_turns(cls, v: int) -> int:
        if v < 1:
            raise ValueError(f"max_turns_per_stage 必須 >= 1,收到:{v}")
        return v


class DiagnosticSample(BaseModel):
    """Mirrors src/incident.rs::DiagnosticSample — documents the shape of
    entries inside incident["diagnostics_history"]. incident itself stays an
    untyped dict (see EscalateRequest below), so this model is not used to
    parse/validate incident content; it's here purely so the contract with
    the Rust side stays legible from this file, matching this module's
    field-for-field mirroring convention."""

    ts_ms: int
    command: str
    output: str
    exit_code: int


class EscalateRequest(BaseModel):
    incident: dict[str, Any]
    cwd: str
    escalation: EscalationSettings
    orchestrator: OrchestratorConfig
    agents: AgentsConfig
    remote: RemoteConfig = Field(default_factory=RemoteConfig)

    @field_validator("cwd")
    @classmethod
    def _validate_cwd(cls, v: str) -> str:
        """Reject an unconfined cwd before it ever reaches tools.py's
        path-confinement checks — those only guard paths *relative to* cwd,
        so a cwd of "/" (or any non-directory / nonexistent path) would
        defeat confinement entirely rather than just misbehave."""
        p = Path(v)
        if not p.is_absolute():
            raise ValueError(f"cwd 必須是絕對路徑:{v}")
        resolved = p.resolve()
        if not resolved.exists():
            raise ValueError(f"cwd 不存在:{v}")
        if not resolved.is_dir():
            raise ValueError(f"cwd 必須是目錄:{v}")
        if resolved == resolved.parent:
            raise ValueError(f"cwd 不可以是檔案系統根目錄:{v}")

        allowed_roots = os.environ.get("ARTEMIS_ALLOWED_CWD_ROOTS")
        if allowed_roots:
            roots = [Path(r).resolve() for r in allowed_roots.split(os.pathsep) if r]
            if roots and not any(
                resolved == root or root in resolved.parents for root in roots
            ):
                raise ValueError(
                    f"cwd 不在 ARTEMIS_ALLOWED_CWD_ROOTS 允許的範圍內:{v}"
                )
        return str(resolved)


class StageResult(BaseModel):
    stage: str
    ran: bool = True
    action_taken: Optional[str] = None
    reasoning: Optional[str] = None
    root_cause_hypothesis: Optional[str] = None
    files_changed: list[str] = Field(default_factory=list)
    test_result: Optional[str] = None
    raw_response: str = ""
    verified_resolved: Optional[bool] = None


class AgentFinding(BaseModel):
    role: str
    summary: str
    risk_level: Optional[str] = None
    raw_response: str = ""


class Synthesis(BaseModel):
    selected_agents: list[str] = Field(default_factory=list)
    dispatch_reasoning: Optional[str] = None
    findings: list[AgentFinding] = Field(default_factory=list)
    root_cause_hypothesis: Optional[str] = None
    risk_level: Optional[str] = None
    recommended_stage: str = "stage1"
    notes_for_stage1: Optional[str] = None
    notes_for_stage2: Optional[str] = None
    notes_for_stage3: Optional[str] = None


class EscalationReport(BaseModel):
    multi_agent_analysis: Optional[Synthesis] = None
    stage1_immediate: Optional[StageResult] = None
    stage2_parameter: Optional[StageResult] = None
    stage3_code_fix: Optional[StageResult] = None
    final_resolved: bool = False
    code_diff: Optional[str] = None
    # 這次 escalate() 呼叫(stage0 分析 + stage1~3)累計消耗的 token 數,見
    # pipeline.py::TokenBudget。None 表示尚未統計過(理論上不會發生,保留
    # 是為了跟 Rust 側 Option<u64> 的預設語意一致)。
    tokens_used: Optional[int] = None
    # 是否因為觸及 escalation.max_tokens_per_escalation 而提早跳過/中止某個階段。
    budget_exhausted: bool = False


class IncidentPush(BaseModel):
    """Body of POST /incidents — a host's best-effort push of one recorded
    incident to the central collector, independent of whether escalation is
    enabled on that host."""

    host_id: str
    project: str
    incident: dict[str, Any]
    report_markdown: Optional[str] = None


class IncidentSummary(BaseModel):
    host_id: str
    project: str
    incident_id: str
    message: str
    timestamp: str
    final_resolved: Optional[bool] = None
    received_at: str
    # Milestone 1 lifecycle fields (src/incident.rs) — derived from the stored
    # incident_json at read time (see central.py::list_incidents), not their
    # own DB columns, so old rows pushed before this milestone still list
    # fine with these simply coming back as None.
    status: Optional[str] = None
    severity: Optional[str] = None
    occurrence_count: Optional[int] = None
    last_seen: Optional[str] = None
    fingerprint: Optional[str] = None
