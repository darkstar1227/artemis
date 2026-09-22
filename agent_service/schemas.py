"""HTTP wire contract between Rust (src/agent_client.rs) and this service.

Field names deliberately mirror the Rust structs (src/incident.rs, src/config.rs)
so the JSON on both sides stays a straightforward 1:1 mapping.
"""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


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


class EscalateRequest(BaseModel):
    incident: dict[str, Any]
    cwd: str
    escalation: EscalationSettings
    orchestrator: OrchestratorConfig
    agents: AgentsConfig


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
