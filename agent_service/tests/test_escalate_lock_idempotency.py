"""Milestone 1 step 7: per-cwd serialization + idempotency for POST /escalate
(main.py). No LLM needed — pipeline.escalate is monkeypatched with a fake
coroutine that records start/end timestamps and sleeps briefly, so these
tests exercise only the locking/caching logic added around it.

Covers:
  (a) two concurrent same-cwd different-id calls do NOT overlap
  (b) two different cwds DO overlap
  (c) same (cwd, id) concurrent -> fake called once, both get the same report
  (d) same (cwd, id) after completion -> fake not called again, replay header
  (e) a failing call is not cached -> retry re-runs the fake
  (f) same incident id but different cwd -> runs separately (not deduped)
  (g) exception-path eviction is identity-checked: if the cache entry for a
      key was replaced (e.g. LRU-evicted then re-inserted by a duplicate
      request) before the original run fails, the failure handler must not
      delete the newer entry

Run with:
    uv run python tests/test_escalate_lock_idempotency.py
    uv run pytest tests/test_escalate_lock_idempotency.py -v
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx2 as httpx  # transitive dep (openai-agents -> httpx2); httpx-compatible API


def _incident(incident_id: str | None) -> dict:
    incident: dict = {
        "project": "p",
        "source": "Process",
        "message": "boom",
        "frames": [],
        "raw": "",
    }
    if incident_id is not None:
        incident["id"] = incident_id
    return incident


def _body(cwd: str, incident_id: str | None) -> dict:
    return {
        "incident": _incident(incident_id),
        "cwd": cwd,
        "escalation": {},
        "orchestrator": {"enabled": False},
        "agents": {
            "risk_analysis": {"model": "mock"},
            "security_analysis": {"model": "mock"},
            "quick_fix_analysis": {"model": "mock"},
            "log_analysis": {"model": "mock"},
            "root_cause_analysis": {"model": "mock"},
        },
    }


def _make_fake_escalate(calls: list, *, sleep_s: float = 0.2, fail_ids: set[str] | None = None):
    fail_ids = fail_ids or set()

    async def fake_escalate(req):
        record = {"cwd": req.cwd, "id": req.incident.get("id"), "start": time.monotonic()}
        calls.append(record)
        await asyncio.sleep(sleep_s)
        record["end"] = time.monotonic()
        if req.incident.get("id") in fail_ids:
            raise RuntimeError("simulated pipeline failure")
        from schemas import EscalationReport

        return EscalationReport(final_resolved=True)

    return fake_escalate


def _intervals_overlap(a: dict, b: dict) -> bool:
    return a["start"] < b["end"] and b["start"] < a["end"]


async def _check_same_cwd_serialized() -> None:
    import main

    calls: list = []
    original = main.escalate
    main.escalate = _make_fake_escalate(calls)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            transport = httpx.ASGITransport(app=main.app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                r1, r2 = await asyncio.gather(
                    client.post("/escalate", json=_body(tmp, "id-1")),
                    client.post("/escalate", json=_body(tmp, "id-2")),
                )
        assert r1.status_code == 200, r1.text
        assert r2.status_code == 200, r2.text
        assert len(calls) == 2
        assert not _intervals_overlap(calls[0], calls[1]), f"same-cwd calls overlapped: {calls}"
    finally:
        main.escalate = original

    print("PASS: two same-cwd calls (different ids) run strictly one after another")


async def _check_different_cwd_concurrent() -> None:
    import main

    calls: list = []
    original = main.escalate
    main.escalate = _make_fake_escalate(calls, sleep_s=0.3)
    try:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            transport = httpx.ASGITransport(app=main.app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                r1, r2 = await asyncio.gather(
                    client.post("/escalate", json=_body(tmp_a, "id-a")),
                    client.post("/escalate", json=_body(tmp_b, "id-b")),
                )
        assert r1.status_code == 200, r1.text
        assert r2.status_code == 200, r2.text
        assert len(calls) == 2
        assert _intervals_overlap(calls[0], calls[1]), f"different-cwd calls did NOT overlap: {calls}"
    finally:
        main.escalate = original

    print("PASS: two different-cwd calls run concurrently")


async def _check_same_key_inflight_dedup() -> None:
    import main

    calls: list = []
    original = main.escalate
    main.escalate = _make_fake_escalate(calls, sleep_s=0.3)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            transport = httpx.ASGITransport(app=main.app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                r1, r2 = await asyncio.gather(
                    client.post("/escalate", json=_body(tmp, "dup-1")),
                    client.post("/escalate", json=_body(tmp, "dup-1")),
                )
        assert r1.status_code == 200, r1.text
        assert r2.status_code == 200, r2.text
        assert len(calls) == 1, f"expected fake_escalate called once, got {len(calls)}"
        assert r1.json() == r2.json()
    finally:
        main.escalate = original

    print("PASS: concurrent duplicate (cwd, id) is deduped to a single pipeline run")


async def _check_replay_after_completion() -> None:
    import main

    calls: list = []
    original = main.escalate
    main.escalate = _make_fake_escalate(calls, sleep_s=0.05)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            transport = httpx.ASGITransport(app=main.app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                r1 = await client.post("/escalate", json=_body(tmp, "replay-1"))
                assert r1.status_code == 200, r1.text
                assert "X-Artemis-Idempotent-Replay" not in r1.headers

                r2 = await client.post("/escalate", json=_body(tmp, "replay-1"))
        assert r2.status_code == 200, r2.text
        assert len(calls) == 1, f"expected fake_escalate not re-run, got {len(calls)} calls"
        assert r2.headers.get("X-Artemis-Idempotent-Replay") == "true"
        assert r1.json() == r2.json()
    finally:
        main.escalate = original

    print("PASS: replaying a completed (cwd, id) returns the cached report with the replay header")


async def _check_failure_not_cached() -> None:
    import main

    calls: list = []
    original = main.escalate
    main.escalate = _make_fake_escalate(calls, sleep_s=0.05, fail_ids={"boom-1"})
    try:
        with tempfile.TemporaryDirectory() as tmp:
            transport = httpx.ASGITransport(app=main.app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                try:
                    await client.post("/escalate", json=_body(tmp, "boom-1"))
                    raise AssertionError("expected the simulated pipeline failure to propagate")
                except RuntimeError:
                    pass

                # retry after the failed run: the fake must be invoked again (not cached).
                main.escalate = _make_fake_escalate(calls, sleep_s=0.05)
                r2 = await client.post("/escalate", json=_body(tmp, "boom-1"))
        assert r2.status_code == 200, r2.text
        assert len(calls) == 2, f"expected fake_escalate invoked again after failure, got {len(calls)}"
    finally:
        main.escalate = original

    print("PASS: a failed run is not cached, retry re-runs the pipeline")


async def _check_same_id_different_cwd_not_deduped() -> None:
    import main

    calls: list = []
    original = main.escalate
    main.escalate = _make_fake_escalate(calls, sleep_s=0.05)
    try:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            transport = httpx.ASGITransport(app=main.app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                r1 = await client.post("/escalate", json=_body(tmp_a, "shared-id"))
                r2 = await client.post("/escalate", json=_body(tmp_b, "shared-id"))
        assert r1.status_code == 200, r1.text
        assert r2.status_code == 200, r2.text
        assert len(calls) == 2, f"expected the same incident id under different cwds to run separately, got {len(calls)}"
    finally:
        main.escalate = original

    print("PASS: same incident id under different cwds is not deduped")


async def _check_exception_path_identity_checked_eviction() -> None:
    import main
    from schemas import (
        AgentRoleConfig,
        AgentsConfig,
        EscalateRequest,
        EscalationReport,
        EscalationSettings,
        OrchestratorConfig,
    )

    async def failing_escalate(_req):
        raise RuntimeError("simulated pipeline failure")

    async def other_escalate(_req):
        return EscalationReport(final_resolved=True)

    original = main.escalate
    main.escalate = failing_escalate
    try:
        with tempfile.TemporaryDirectory() as tmp:
            resolved_cwd = str(Path(tmp).resolve())
            key = (resolved_cwd, "race-1")

            req = EscalateRequest(
                incident={
                    "id": "race-1",
                    "project": "p",
                    "source": "Process",
                    "message": "m",
                    "frames": [],
                    "raw": "",
                },
                cwd=tmp,
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

            # The run that will fail — scheduled but not yet awaited.
            task_orig = asyncio.ensure_future(main._run_pipeline_locked(req, resolved_cwd, key))
            main._idempotency_cache[key] = main._CacheEntry(task=task_orig, created_at=time.monotonic())

            # Simulate: that entry got LRU-evicted, then a duplicate request
            # came in and inserted a fresh entry/task under the same key —
            # all before task_orig's failure handler ever runs.
            task_new = asyncio.ensure_future(other_escalate(req))
            new_entry = main._CacheEntry(task=task_new, created_at=time.monotonic())
            main._idempotency_cache[key] = new_entry

            try:
                await task_orig
                raise AssertionError("expected task_orig to raise")
            except RuntimeError:
                pass

            await task_new  # drain, avoid an "exception never retrieved" style warning

            assert main._idempotency_cache.get(key) is new_entry, (
                "the failing run's exception handler evicted a newer entry for the same key"
            )
    finally:
        main.escalate = original
        main._idempotency_cache.clear()

    print("PASS: exception-path eviction only removes the entry that actually belongs to the failing run")


async def main_async() -> None:
    await _check_same_cwd_serialized()
    await _check_different_cwd_concurrent()
    await _check_same_key_inflight_dedup()
    await _check_replay_after_completion()
    await _check_failure_not_cached()
    await _check_same_id_different_cwd_not_deduped()
    await _check_exception_path_identity_checked_eviction()


def test_same_cwd_serialized():
    asyncio.run(_check_same_cwd_serialized())


def test_different_cwd_concurrent():
    asyncio.run(_check_different_cwd_concurrent())


def test_same_key_inflight_dedup():
    asyncio.run(_check_same_key_inflight_dedup())


def test_replay_after_completion():
    asyncio.run(_check_replay_after_completion())


def test_failure_not_cached():
    asyncio.run(_check_failure_not_cached())


def test_same_id_different_cwd_not_deduped():
    asyncio.run(_check_same_id_different_cwd_not_deduped())


def test_exception_path_identity_checked_eviction():
    asyncio.run(_check_exception_path_identity_checked_eviction())


if __name__ == "__main__":
    asyncio.run(main_async())
