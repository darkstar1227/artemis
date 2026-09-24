"""Central multi-host incident collector: SQLite-backed store for incidents
pushed by any number of `artemis watch` instances (see src/central_client.rs).
Each host pushes independently of whether escalation is enabled there —
this is purely "give me one place to see incidents across every host/project",
not part of the escalation pipeline.
"""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from schemas import IncidentPush, IncidentSummary

DB_PATH = Path(os.environ.get("ARTEMIS_CENTRAL_DB", Path(__file__).parent / "data" / "central.db"))

_SCHEMA = """
CREATE TABLE IF NOT EXISTS incidents (
    host_id TEXT NOT NULL,
    incident_id TEXT NOT NULL,
    project TEXT NOT NULL,
    message TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    final_resolved INTEGER,
    incident_json TEXT NOT NULL,
    report_markdown TEXT,
    received_at TEXT NOT NULL,
    PRIMARY KEY (host_id, incident_id)
);
CREATE INDEX IF NOT EXISTS idx_incidents_project ON incidents(project);
CREATE INDEX IF NOT EXISTS idx_incidents_received_at ON incidents(received_at);
"""


@contextmanager
def _conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executescript(_SCHEMA)
        yield conn
        conn.commit()
    finally:
        conn.close()


def record(push: IncidentPush) -> None:
    incident_id = str(push.incident.get("id", ""))
    message = str(push.incident.get("message", ""))
    timestamp = str(push.incident.get("timestamp", ""))
    final_resolved = None
    escalation = push.incident.get("escalation")
    if isinstance(escalation, dict):
        final_resolved = 1 if escalation.get("final_resolved") else 0

    with _conn() as conn:
        conn.execute(
            """INSERT INTO incidents
               (host_id, incident_id, project, message, timestamp, final_resolved,
                incident_json, report_markdown, received_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(host_id, incident_id) DO UPDATE SET
                 project=excluded.project, message=excluded.message,
                 timestamp=excluded.timestamp, final_resolved=excluded.final_resolved,
                 incident_json=excluded.incident_json, report_markdown=excluded.report_markdown,
                 received_at=excluded.received_at""",
            (
                push.host_id,
                incident_id,
                push.project,
                message,
                timestamp,
                final_resolved,
                json.dumps(push.incident),
                push.report_markdown,
                datetime.now(timezone.utc).isoformat(),
            ),
        )


def list_incidents(
    host_id: str | None = None, project: str | None = None, limit: int = 100
) -> list[IncidentSummary]:
    query = (
        "SELECT host_id, project, incident_id, message, timestamp, final_resolved, "
        "received_at, incident_json FROM incidents"
    )
    clauses = []
    params: list[str] = []
    if host_id:
        clauses.append("host_id = ?")
        params.append(host_id)
    if project:
        clauses.append("project = ?")
        params.append(project)
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY received_at DESC LIMIT ?"
    params.append(str(limit))

    with _conn() as conn:
        rows = conn.execute(query, params).fetchall()

    summaries = []
    for r in rows:
        # Milestone 1 lifecycle fields aren't their own DB columns — they're
        # read back out of the stored incident_json at query time, so rows
        # pushed by a pre-milestone host (no incident_json fields at all)
        # just come back as None instead of erroring.
        try:
            incident = json.loads(r[7])
        except (json.JSONDecodeError, TypeError):
            incident = {}
        summaries.append(
            IncidentSummary(
                host_id=r[0],
                project=r[1],
                incident_id=r[2],
                message=r[3],
                timestamp=r[4],
                final_resolved=bool(r[5]) if r[5] is not None else None,
                received_at=r[6],
                status=incident.get("status"),
                severity=incident.get("severity"),
                occurrence_count=incident.get("occurrence_count"),
                last_seen=incident.get("last_seen"),
                fingerprint=incident.get("fingerprint"),
            )
        )
    return summaries


def get_incident(host_id: str, incident_id: str) -> dict | None:
    with _conn() as conn:
        row = conn.execute(
            "SELECT incident_json, report_markdown FROM incidents WHERE host_id = ? AND incident_id = ?",
            (host_id, incident_id),
        ).fetchone()
    if row is None:
        return None
    return {"incident": json.loads(row[0]), "report_markdown": row[1]}
