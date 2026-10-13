"""
The SQLite store (schema.sql): deployments, KPI samples, incidents and
everything that happens to them.

    from nifi_healer.store.db import get_store
    store = get_store()
    store.create_incident(...)

One connection, shared by the poller and the agent threads behind a
lock. The default rollback journal, not WAL: on CAI the project files
are network storage (EFS), where WAL's shared-memory index is unsafe.
Writes are small and infrequent, so the lock is never contended long.

Rows come back as plain dicts, with each `*_json` column also decoded
into the same name without the suffix (`signals_json` -> `signals`).
"""

import json
import os
import sqlite3
import threading
import uuid
from datetime import datetime, timezone

import config

SCHEMA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema.sql")

# An open incident blocks another for the same process group.
OPEN_STATUSES = ("NEW", "DIAGNOSING", "PENDING_REVIEW", "APPLYING", "VERIFYING")

# RESOLVED: the fix measured IMPROVED. UNRESOLVED: applied, but the
# verdict was anything else. DRY_RUN: approved while disarmed.
CLOSED_STATUSES = ("RESOLVED", "UNRESOLVED", "REJECTED", "NO_ACTION", "DRY_RUN", "FAILED")


def _encode(value):
    return None if value is None else json.dumps(value, sort_keys=True, default=str)


def _row(row) -> dict | None:
    if row is None:
        return None

    out = dict(row)

    for key in list(out):
        if key.endswith("_json"):
            text = out[key]
            out[key[:-5]] = json.loads(text) if text else None

    return out


def _columns(fields: dict) -> dict:
    """Encode the JSON columns: callers pass `signals=`, the table has signals_json."""

    out = {}

    for key, value in fields.items():
        if key in _JSON_FIELDS:
            out[f"{key}_json"] = _encode(value)

        else:
            out[key] = value

    return out


_JSON_FIELDS = {"alerts", "signals", "action", "citations", "validation", "edits", "argv",
                "result", "details"}


class Store:
    def __init__(self, path: str | None = None):
        self.path = path or config.DB_PATH
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False,
                                    isolation_level=None)
        self.conn.row_factory = sqlite3.Row

        with open(SCHEMA_FILE) as handle:
            self.conn.executescript(handle.read())

    # -- plumbing -------------------------------------------------------

    def _all(self, sql: str, params=()) -> list:
        with self.lock:
            return [_row(r) for r in self.conn.execute(sql, params).fetchall()]

    def _one(self, sql: str, params=()) -> dict | None:
        with self.lock:
            return _row(self.conn.execute(sql, params).fetchone())

    def _insert(self, table: str, fields: dict) -> int:
        fields = _columns(fields)
        names = ", ".join(fields)
        marks = ", ".join("?" for _ in fields)

        with self.lock:
            cursor = self.conn.execute(f"INSERT INTO {table} ({names}) VALUES ({marks})",
                                       tuple(fields.values()))

            return cursor.lastrowid

    def _update(self, table: str, key: str, value, fields: dict) -> None:
        fields = _columns(fields)

        if not fields:
            return

        assignments = ", ".join(f"{name} = ?" for name in fields)

        with self.lock:
            self.conn.execute(f"UPDATE {table} SET {assignments} WHERE {key} = ?",
                              tuple(fields.values()) + (value,))

    # -- deployments and samples -----------------------------------------

    def upsert_deployment(self, crn: str, pack: str, facts: dict, seen_at: float) -> None:
        fields = _columns({
            "crn": crn, "pack": pack, "name": facts.get("name"), "state": facts.get("state"),
            "flow_version": facts.get("flow_version"),
            "flow_version_crn": facts.get("flow_version_crn"),
            "cluster_size": facts.get("cluster_size"), "node_count": facts.get("node_count"),
            "alerts": facts.get("alerts"), "seen_at": seen_at,
        })
        names = ", ".join(fields)
        marks = ", ".join("?" for _ in fields)
        updates = ", ".join(f"{n} = excluded.{n}" for n in fields if n != "crn")

        with self.lock:
            self.conn.execute(
                f"INSERT INTO deployments ({names}) VALUES ({marks}) "
                f"ON CONFLICT (crn) DO UPDATE SET {updates}", tuple(fields.values()))

    def deployments(self) -> list:
        return self._all("SELECT * FROM deployments ORDER BY pack")

    def deployment(self, crn: str) -> dict | None:
        return self._one("SELECT * FROM deployments WHERE crn = ?", (crn,))

    def note_flow_version(self, crn: str, pack: str, flow_crn: str, version, seen_at: float,
                          source: str = "external", incident_id: str = "",
                          comments: str = "") -> None:
        """Record a version the first time it is seen; an earlier record wins."""

        with self.lock:
            self.conn.execute(
                "INSERT OR IGNORE INTO flow_versions "
                "(crn, pack, flow_crn, version, source, incident_id, comments, first_seen_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (crn, pack, flow_crn, version, source, incident_id or None, comments, seen_at))

    def flow_versions(self, pack: str) -> list:
        return self._all("SELECT * FROM flow_versions WHERE pack = ? ORDER BY version", (pack,))

    def add_samples(self, deployment_crn: str, samples: list) -> None:
        """samples: [(kpi_key, t, value)]; a bucket already stored is kept."""

        with self.lock:
            self.conn.executemany(
                "INSERT OR IGNORE INTO kpi_samples (deployment_crn, kpi_key, t, value) "
                "VALUES (?, ?, ?, ?)",
                [(deployment_crn, key, t, value) for key, t, value in samples])

    def samples(self, deployment_crn: str, kpi_key: str, since: float = 0.0) -> list:
        with self.lock:
            rows = self.conn.execute(
                "SELECT t, value FROM kpi_samples WHERE deployment_crn = ? AND kpi_key = ? "
                "AND t >= ? ORDER BY t", (deployment_crn, kpi_key, since)).fetchall()

        return [(r["t"], r["value"]) for r in rows]

    # -- incidents -------------------------------------------------------

    def create_incident(self, pack: str, deployment_crn: str, deployed_flow_crn: str,
                        scenario: str, process_group: str, signals: list, summary: str,
                        expected_flow_version_crn: str, now: float) -> dict:
        stamp = datetime.fromtimestamp(now, timezone.utc).strftime("%Y%m%d-%H%M%S")
        incident_id = f"INC-{stamp}-{scenario or 'PG'}-{uuid.uuid4().hex[:4]}"
        self._insert("incidents", {
            "id": incident_id, "pack": pack, "deployment_crn": deployment_crn,
            "deployed_flow_crn": deployed_flow_crn, "scenario": scenario,
            "process_group": process_group, "status": "NEW", "summary": summary,
            "signals": signals, "expected_flow_version_crn": expected_flow_version_crn,
            "opened_at": now, "updated_at": now, "last_signal_at": now,
        })

        return self.incident(incident_id)

    def incident(self, incident_id: str) -> dict | None:
        return self._one("SELECT * FROM incidents WHERE id = ?", (incident_id,))

    def update_incident(self, incident_id: str, now: float, **fields) -> dict | None:
        fields["updated_at"] = now

        if fields.get("status") in CLOSED_STATUSES:
            fields.setdefault("closed_at", now)

        self._update("incidents", "id", incident_id, fields)

        return self.incident(incident_id)

    def open_incident(self, deployment_crn: str, process_group: str) -> dict | None:
        marks = ", ".join("?" for _ in OPEN_STATUSES)

        return self._one(
            f"SELECT * FROM incidents WHERE deployment_crn = ? AND process_group = ? "
            f"AND status IN ({marks}) ORDER BY opened_at DESC LIMIT 1",
            (deployment_crn, process_group) + OPEN_STATUSES)

    def last_closed_incident(self, deployment_crn: str, process_group: str) -> dict | None:
        return self._one(
            "SELECT * FROM incidents WHERE deployment_crn = ? AND process_group = ? "
            "AND closed_at IS NOT NULL ORDER BY closed_at DESC LIMIT 1",
            (deployment_crn, process_group))

    def incidents(self, statuses=None, pack: str = "", limit: int = 200) -> list:
        sql, params = "SELECT * FROM incidents WHERE 1 = 1", []

        if statuses:
            sql += f" AND status IN ({', '.join('?' for _ in statuses)})"
            params += list(statuses)

        if pack:
            sql += " AND pack = ?"
            params.append(pack)

        return self._all(sql + " ORDER BY opened_at DESC LIMIT ?", params + [limit])

    # -- proposals, reviews, actions, outcomes ------------------------------

    def add_proposal(self, incident_id: str, now: float, **fields) -> int:
        return self._insert("proposals", dict(fields, incident_id=incident_id, created_at=now))

    def update_proposal(self, proposal_id: int, **fields) -> None:
        self._update("proposals", "id", proposal_id, fields)

    def proposals(self, incident_id: str) -> list:
        return self._all("SELECT * FROM proposals WHERE incident_id = ? ORDER BY id", (incident_id,))

    def add_review(self, incident_id: str, decision: str, now: float, **fields) -> int:
        return self._insert("reviews", dict(fields, incident_id=incident_id, decision=decision,
                                            created_at=now))

    def reviews(self, incident_id: str) -> list:
        return self._all("SELECT * FROM reviews WHERE incident_id = ? ORDER BY id", (incident_id,))

    def add_action(self, incident_id: str, pack: str, deployment_crn: str, action_type: str,
                   dry_run: bool, status: str, **fields) -> int:
        return self._insert("actions", dict(
            fields, incident_id=incident_id, pack=pack, deployment_crn=deployment_crn,
            action_type=action_type, dry_run=int(bool(dry_run)), status=status))

    def update_action(self, action_id: int, **fields) -> None:
        self._update("actions", "id", action_id, fields)

    def action(self, action_id: int) -> dict | None:
        return self._one("SELECT * FROM actions WHERE id = ?", (action_id,))

    def actions(self, incident_id: str = "", deployment_crn: str = "") -> list:
        if incident_id:
            return self._all("SELECT * FROM actions WHERE incident_id = ? ORDER BY id",
                             (incident_id,))

        return self._all("SELECT * FROM actions WHERE deployment_crn = ? ORDER BY id",
                         (deployment_crn,))

    def performed_action_times(self, deployment_crn: str) -> list:
        """When each real (not dry-run) change to this deployment started, for the rate guard."""

        with self.lock:
            rows = self.conn.execute(
                "SELECT started_at FROM actions WHERE deployment_crn = ? AND dry_run = 0 "
                "AND status IN ('APPLYING', 'APPLIED', 'FAILED') AND started_at IS NOT NULL",
                (deployment_crn,)).fetchall()

        return [r["started_at"] for r in rows]

    def actions_awaiting_outcome(self, due_before: float) -> list:
        """Applied actions whose verify window has closed and that have no verdict yet."""

        return self._all(
            "SELECT a.* FROM actions a LEFT JOIN outcomes o ON o.action_id = a.id "
            "WHERE a.status = 'APPLIED' AND a.dry_run = 0 AND o.id IS NULL "
            "AND a.finished_at IS NOT NULL AND a.finished_at <= ? ORDER BY a.finished_at",
            (due_before,))

    def add_outcome(self, incident_id: str, action_id: int, verdict: str, period: str,
                    details: dict, now: float) -> int:
        return self._insert("outcomes", {
            "incident_id": incident_id, "action_id": action_id, "verdict": verdict,
            "period": period, "details": details, "evaluated_at": now,
        })

    def outcomes(self, incident_id: str = "") -> list:
        if incident_id:
            return self._all("SELECT * FROM outcomes WHERE incident_id = ? ORDER BY id",
                             (incident_id,))

        return self._all("SELECT * FROM outcomes ORDER BY evaluated_at DESC")


_STORE = {"store": None}
_STORE_LOCK = threading.Lock()


def get_store() -> Store:
    with _STORE_LOCK:
        if _STORE["store"] is None:
            _STORE["store"] = Store()

        return _STORE["store"]


def set_store(store: Store) -> None:
    """Tests only: point every caller at a store on a scratch path."""

    with _STORE_LOCK:
        _STORE["store"] = store
