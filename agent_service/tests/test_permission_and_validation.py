"""Unit-level tests for the code-audit fixes (no mock LLM needed — these
exercise the pure helper functions/pydantic validators directly):

  1. stage2 editable-files path resolution (tools.py::_check_editable) —
     relative config-file entries must resolve against ctx.cwd, not the
     agent_service process's own cwd.
  2. grep_files symlink escape (tools.py::_grep_scan) — a symlink inside the
     tree pointing outside cwd must be skipped, not read.
  3. EscalateRequest.cwd validation (schemas.py) — "/" and a nonexistent path
     must be rejected.
  4. diagnostics_history rendered into incident_context() (pipeline.py).
  5. Milestone 1 lifecycle fields (occurrence_count/severity/recurrence_of)
     rendered into incident_context() (pipeline.py), tolerant of absence.

Run with:
    uv run python tests/test_permission_and_validation.py
    uv run pytest tests/test_permission_and_validation.py -v
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _check_stage2_relative_path() -> None:
    from tools import PermissionDenied, StageContext, _check_editable

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp).resolve()
        (root / "config").mkdir()
        (root / "config" / "app.toml").write_text("x = 1\n")

        ctx = StageContext(cwd=root, stage="stage2_parameter", editable_files=["./config/app.toml"])

        # Allowed: the same file the config names, addressed the same way an
        # agent would (relative to cwd).
        _check_editable(ctx, "config/app.toml")  # must not raise

        # Denied: a file the config never named.
        (root / "config" / "other.toml").write_text("y = 1\n")
        try:
            _check_editable(ctx, "config/other.toml")
        except PermissionDenied:
            pass
        else:
            raise AssertionError("expected PermissionDenied for a file not in editable_files")

    print("PASS: stage2 relative editable_files entry resolves against ctx.cwd")


def _check_grep_symlink_escape() -> None:
    from tools import StageContext, _grep_scan

    with tempfile.TemporaryDirectory() as outer_tmp, tempfile.TemporaryDirectory() as project_tmp:
        outer = Path(outer_tmp).resolve()
        project = Path(project_tmp).resolve()

        secret = outer / "secret.txt"
        secret.write_text("TOP_SECRET_TOKEN=abc123\n")

        link = project / "escape_link"
        link.symlink_to(secret)

        normal = project / "normal.txt"
        normal.write_text("TOP_SECRET_TOKEN=should-not-matter\nordinary line\n")

        ctx = StageContext(cwd=project, stage="stage3_code_fix")
        result = _grep_scan(ctx, "TOP_SECRET_TOKEN", ".")

        assert "normal.txt" in result, f"expected normal.txt match, got: {result!r}"
        assert "escape_link" not in result, f"symlink escape was NOT skipped: {result!r}"

    print("PASS: grep_files skips a symlink that escapes cwd")


def _check_cwd_validation() -> None:
    from pydantic import ValidationError

    from schemas import AgentRoleConfig, AgentsConfig, EscalateRequest, EscalationSettings, OrchestratorConfig

    def _build(cwd: str) -> None:
        EscalateRequest(
            incident={"id": "x", "project": "p", "source": "Process", "message": "m", "frames": [], "raw": ""},
            cwd=cwd,
            escalation=EscalationSettings(),
            orchestrator=OrchestratorConfig(enabled=False),
            agents=AgentsConfig(
                risk_analysis=AgentRoleConfig(model="mock"),
                security_analysis=AgentRoleConfig(model="mock"),
                quick_fix_analysis=AgentRoleConfig(model="mock"),
                log_analysis=AgentRoleConfig(model="mock"),
                root_cause_analysis=AgentRoleConfig(model="mock"),
            ),
        )

    for bad_cwd in ("/", "/this/path/almost/certainly/does/not/exist/artemis-test"):
        try:
            _build(bad_cwd)
        except ValidationError:
            pass
        else:
            raise AssertionError(f"expected cwd={bad_cwd!r} to be rejected")

    with tempfile.TemporaryDirectory() as tmp:
        _build(tmp)  # must not raise

    print("PASS: EscalateRequest.cwd rejects '/' and a nonexistent path, accepts a real dir")


def _check_diagnostics_in_context() -> None:
    from pipeline import incident_context

    incident = {
        "id": "x",
        "project": "p",
        "source": "Process",
        "message": "boom",
        "frames": [],
        "raw": "",
        "diagnostics_history": [
            {"ts_ms": 1000, "command": "docker stats", "output": "cpu=10%", "exit_code": 0},
            {"ts_ms": 2000, "command": "docker stats", "output": "cpu=90%", "exit_code": 0},
        ],
    }
    ctx = incident_context(incident)
    assert "docker stats" in ctx, "diagnostics command not rendered into incident_context"
    assert "cpu=90%" in ctx, "diagnostics output not rendered into incident_context"

    # Absence must be tolerated, not raise.
    incident_context({"id": "y", "project": "p", "source": "Process", "message": "m", "frames": [], "raw": ""})

    print("PASS: diagnostics_history is rendered into incident_context() and absence is tolerated")


def _check_lifecycle_fields_in_context() -> None:
    from pipeline import incident_context

    incident = {
        "id": "x",
        "project": "p",
        "source": "Process",
        "message": "boom",
        "frames": [],
        "raw": "",
        "occurrence_count": 5,
        "first_seen": "2026-01-01T00:00:00Z",
        "last_seen": "2026-01-01T00:10:00Z",
        "severity": "critical",
        "recurrence_of": "20251231T000000-11112222",
    }
    ctx = incident_context(incident)
    assert "發生次數:5" in ctx, "occurrence_count 未渲染進 incident_context"
    assert "2026-01-01T00:00:00Z" in ctx and "2026-01-01T00:10:00Z" in ctx
    assert "critical" in ctx, "severity 未渲染進 incident_context"
    assert "20251231T000000-11112222" in ctx, "recurrence_of 未渲染進 incident_context"

    # 舊事件 JSON 沒有這些欄位時必須容忍,不能拋例外或印出多餘內容。
    old_ctx = incident_context(
        {"id": "y", "project": "p", "source": "Process", "message": "m", "frames": [], "raw": ""}
    )
    assert "發生次數" not in old_ctx, "缺少 occurrence_count 時不應該印出發生次數這一行"

    print("PASS: lifecycle fields (occurrence_count/severity/recurrence_of) render into incident_context()")


def _check_central_list_incidents_derives_lifecycle_fields() -> None:
    """central.list_incidents() must read status/severity/occurrence_count/
    last_seen/fingerprint back out of the stored incident_json (no dedicated
    DB column for any of them) — and tolerate a row pushed before Milestone 1
    whose incident_json has none of these fields."""
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["ARTEMIS_CENTRAL_DB"] = str(Path(tmp) / "central-test.db")
        import importlib

        import central as central_module

        importlib.reload(central_module)  # pick up the env var override above
        from schemas import IncidentPush

        central_module.record(
            IncidentPush(
                host_id="host1",
                project="demo",
                incident={
                    "id": "inc-new",
                    "message": "boom",
                    "timestamp": "2026-01-01T00:00:00Z",
                    "status": "mitigated",
                    "severity": "critical",
                    "occurrence_count": 5,
                    "last_seen": "2026-01-01T00:10:00Z",
                    "fingerprint": "deadbeefcafef00d",
                },
            )
        )
        central_module.record(
            IncidentPush(
                host_id="host1",
                project="demo",
                incident={"id": "inc-old", "message": "legacy", "timestamp": "2024-01-01T00:00:00Z"},
            )
        )

        summaries = {s.incident_id: s for s in central_module.list_incidents(host_id="host1")}

        new = summaries["inc-new"]
        assert new.status == "mitigated"
        assert new.severity == "critical"
        assert new.occurrence_count == 5
        assert new.last_seen == "2026-01-01T00:10:00Z"
        assert new.fingerprint == "deadbeefcafef00d"

        old = summaries["inc-old"]
        assert old.status is None and old.severity is None and old.occurrence_count is None
        assert old.last_seen is None and old.fingerprint is None

    print("PASS: central.list_incidents() derives lifecycle fields from incident_json, tolerates absence")


def main() -> None:
    _check_stage2_relative_path()
    _check_grep_symlink_escape()
    _check_cwd_validation()
    _check_diagnostics_in_context()
    _check_lifecycle_fields_in_context()
    _check_central_list_incidents_derives_lifecycle_fields()


def test_stage2_relative_path():
    _check_stage2_relative_path()


def test_grep_symlink_escape():
    _check_grep_symlink_escape()


def test_cwd_validation():
    _check_cwd_validation()


def test_diagnostics_in_context():
    _check_diagnostics_in_context()


def test_lifecycle_fields_in_context():
    _check_lifecycle_fields_in_context()


def test_central_list_incidents_derives_lifecycle_fields():
    _check_central_list_incidents_derives_lifecycle_fields()


if __name__ == "__main__":
    main()
