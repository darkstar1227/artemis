"""Local HTTP service the Rust detection layer (src/agent_client.rs) calls
into when it records an incident, instead of shelling out to `claude -p`.
Rust keeps ownership of artemis.toml; every call is stateless and carries
all the config it needs in the request body.
"""

from __future__ import annotations

import asyncio
import hmac
import logging
import os
import time
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response

import central
from pipeline import escalate
from schemas import EscalateRequest, EscalationReport, IncidentPush, IncidentSummary

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("artemis-agent-service")

app = FastAPI(title="artemis-agent-service")

# 若設定了 ARTEMIS_AGENT_SERVICE_TOKEN,/escalate 就會要求 Authorization: Bearer
# <token>。這是選用的 — 只要服務只綁定在 127.0.0.1(預設),不設定也不會有問題;
# 但只要用 --host 0.0.0.0 或任何方式對外開放,就務必設定這個 token,否則任何能
# 連到這個服務的人都可以叫它對這個 repo 執行任意 bash / 寫入任意檔案。
_EXPECTED_TOKEN = os.environ.get("ARTEMIS_AGENT_SERVICE_TOKEN")

if not _EXPECTED_TOKEN:
    logger.warning(
        "ARTEMIS_AGENT_SERVICE_TOKEN 未設定,/escalate 端點沒有任何存取控制 — "
        "務必確認這個服務只綁定在 127.0.0.1,不要對外開放。"
    )


def _require_auth(authorization: str | None = Header(default=None)) -> None:
    if not _EXPECTED_TOKEN:
        return
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = authorization[len("Bearer ") :]
    if not hmac.compare_digest(token, _EXPECTED_TOKEN):
        raise HTTPException(status_code=401, detail="invalid bearer token")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# 同一個 cwd 的序列化 + idempotency(Milestone 1 step 7)。
#
# 序列化:stage2/stage3 會直接寫檔、跑 git diff,兩個對同一個 cwd 的
# /escalate 要求平行跑會互相干擾,所以用「每個 cwd 一把 asyncio.Lock」把
# 它們排成依序執行;不同 cwd 之間彼此獨立、可以平行跑。
#
# Idempotency:Rust 那端逾時重送同一個 incident 是預期行為(見
# src/agent_client.rs),用 (resolved_cwd, incident_id) 當 key 快取「正在跑
# /已跑完」的那個 Future,重送時直接等同一個結果,不會讓 pipeline 重跑一次
# (stage3 可能已經改過檔案,重跑一次代價不小)。快取用有界 LRU + TTL,避免
# 長時間運行的服務累積無限多筆記錄。cwd 一定要放進 key 裡,否則某個呼叫端
# 只要猜到別的專案的 incident id 就能拿到那份報告。
# ---------------------------------------------------------------------------

_IDEMPOTENCY_MAX_ENTRIES = 256
_IDEMPOTENCY_TTL_SECONDS = 3600  # 1 小時

_cwd_locks: dict[str, asyncio.Lock] = {}
_idempotency_cache: "OrderedDict[tuple[str, str], _CacheEntry]" = OrderedDict()
# 對正在跑的 task 保留一個強參照,避免它被排程之後、還沒被任何人 await 完就被 GC 掉
# (例如 idempotency cache 因為 LRU 滿了把 entry 擠掉,但 task 其實還在跑)。
_background_tasks: set[asyncio.Task] = set()


@dataclass
class _CacheEntry:
    task: "asyncio.Task[EscalationReport]"
    created_at: float


def _get_cwd_lock(resolved_cwd: str) -> asyncio.Lock:
    lock = _cwd_locks.get(resolved_cwd)
    if lock is None:
        lock = asyncio.Lock()
        _cwd_locks[resolved_cwd] = lock
    return lock


def _prune_idempotency_cache() -> None:
    now = time.monotonic()
    expired = [
        key
        for key, entry in _idempotency_cache.items()
        if entry.task.done() and now - entry.created_at > _IDEMPOTENCY_TTL_SECONDS
    ]
    for key in expired:
        del _idempotency_cache[key]
    # 還是超過上限的話(例如全部都還在 in-flight),就照 LRU 擠掉最舊的一筆 —
    # 只是讓它離開快取,不會取消還在跑的 task。
    while len(_idempotency_cache) > _IDEMPOTENCY_MAX_ENTRIES:
        _idempotency_cache.popitem(last=False)


async def _run_pipeline_locked(
    req: EscalateRequest, resolved_cwd: str, cache_key: tuple[str, str] | None
) -> EscalationReport:
    async with _get_cwd_lock(resolved_cwd):
        try:
            return await escalate(req)
        except Exception:
            # 失敗不快取 — 讓下一次重送真的重跑一次,而不是一直回同一個錯誤。
            # 用 identity 檢查而不是單純 pop(key):這個 entry 有可能已經因為
            # LRU 被擠出快取,然後又被另一個重複的請求重新塞回同一個 key(新的
            # task),這種情況下絕對不能把那筆「新的、還在跑/已成功」的 entry
            # 誤刪成這次失敗的犧牲品。
            if cache_key is not None:
                current_task = asyncio.current_task()
                entry = _idempotency_cache.get(cache_key)
                if entry is not None and entry.task is current_task:
                    del _idempotency_cache[cache_key]
            raise


@app.post("/escalate", response_model=EscalationReport, dependencies=[Depends(_require_auth)])
async def escalate_endpoint(req: EscalateRequest, response: Response) -> EscalationReport:
    logger.info("escalate: incident=%s cwd=%s", req.incident.get("id"), req.cwd)
    resolved_cwd = str(Path(req.cwd).resolve())
    incident_id = req.incident.get("id")
    cache_key: tuple[str, str] | None = (resolved_cwd, incident_id) if isinstance(incident_id, str) else None

    if cache_key is not None:
        _prune_idempotency_cache()
        existing = _idempotency_cache.get(cache_key)
        if existing is not None:
            _idempotency_cache.move_to_end(cache_key)
            if existing.task.done():
                response.headers["X-Artemis-Idempotent-Replay"] = "true"
            # shield:即使這個要求被取消(例如客戶端斷線),也不能連帶取消
            # 其他人正在等的、或已經跑完快取住的那個共用 task。
            report = await asyncio.shield(existing.task)
            logger.info(
                "escalate done (idempotent replay): incident=%s final_resolved=%s",
                incident_id,
                report.final_resolved,
            )
            return report

    task = asyncio.ensure_future(_run_pipeline_locked(req, resolved_cwd, cache_key))
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)

    if cache_key is not None:
        _idempotency_cache[cache_key] = _CacheEntry(task=task, created_at=time.monotonic())
        _prune_idempotency_cache()

    report = await asyncio.shield(task)
    logger.info(
        "escalate done: incident=%s final_resolved=%s", incident_id, report.final_resolved
    )
    return report


# ---------------------------------------------------------------------------
# 多主機事件彙整(central collector)。與上面的 escalation pipeline 無關 —
# 這裡純粹是讓多台 host 各自跑的 `artemis watch` 有一個共同的地方可以查詢
# 所有專案的事件,不論該 host 有沒有開 escalation。見 src/central_client.rs。
# ---------------------------------------------------------------------------


@app.post("/incidents", dependencies=[Depends(_require_auth)])
async def push_incident(push: IncidentPush) -> dict:
    central.record(push)
    return {"status": "ok"}


@app.get("/incidents", response_model=list[IncidentSummary], dependencies=[Depends(_require_auth)])
async def list_incidents_endpoint(
    host_id: str | None = Query(default=None),
    project: str | None = Query(default=None),
    limit: int = Query(default=100, le=1000),
) -> list[IncidentSummary]:
    return central.list_incidents(host_id=host_id, project=project, limit=limit)


@app.get("/incidents/{host_id}/{incident_id}", dependencies=[Depends(_require_auth)])
async def get_incident_endpoint(host_id: str, incident_id: str) -> dict:
    result = central.get_incident(host_id, incident_id)
    if result is None:
        raise HTTPException(status_code=404, detail="incident not found")
    return result
