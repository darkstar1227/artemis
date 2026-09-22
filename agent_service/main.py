"""Local HTTP service the Rust detection layer (src/agent_client.rs) calls
into when it records an incident, instead of shelling out to `claude -p`.
Rust keeps ownership of artemis.toml; every call is stateless and carries
all the config it needs in the request body.
"""

from __future__ import annotations

import hmac
import logging
import os

from fastapi import Depends, FastAPI, Header, HTTPException

from pipeline import escalate
from schemas import EscalateRequest, EscalationReport

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


@app.post("/escalate", response_model=EscalationReport, dependencies=[Depends(_require_auth)])
async def escalate_endpoint(req: EscalateRequest) -> EscalationReport:
    logger.info("escalate: incident=%s cwd=%s", req.incident.get("id"), req.cwd)
    report = await escalate(req)
    logger.info(
        "escalate done: incident=%s final_resolved=%s", req.incident.get("id"), report.final_resolved
    )
    return report
