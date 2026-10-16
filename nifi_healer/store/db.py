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

import bisect
import json
import os
import sqlite3
import threading
import uuid
from datetime import datetime, timezone

import config
from nifi_healer.kpi.outcomes import due_at

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
                "result", "details", "sources", "problems", "steps"}

# A playbook edit still waiting on the drafter or a reviewer.
OPEN_EDITS = ("DRAFTING", "PROPOSED")


class Store:
    def __init__(self, path: str | None = None):
        self.path = path or config.DB_PATH
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False,
                                    isolation_level=None)
        self.conn.row_factory = sqlite3.Row

        with open(SCHEMA_FILE) as handle:
            schema = handle.read()

        with self.lock:
            self._set_aside_single_window_outcomes()
            self.conn.executescript(schema)
            self._restore_single_window_outcomes()
            self._add_incident_columns()

    # -- migrations -----------------------------------------------------

    def _add_incident_columns(self) -> None:
        """Columns added to incidents after it was first created: NULL in older rows."""

        have = {r["name"] for r in self.conn.execute("PRAGMA table_info(incidents)")}

        for name, kind in (("kind", "TEXT"), ("rollback_of_action_id", "INTEGER")):
            if name not in have:
                self.conn.execute(f"ALTER TABLE incidents ADD COLUMN {name} {kind}")

    def _set_aside_single_window_outcomes(self) -> None:
        """
        Before the 1 h and 12 h verdicts, outcomes held one per action. That
        table is set aside, once, after a copy of the whole file next to it.
        """

        columns = {r["name"] for r in self.conn.execute("PRAGMA table_info(outcomes)")}

        if not columns or "window_seconds" in columns:
            return

        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        backup = sqlite3.connect(f"{self.path}.{stamp}.bak")

        try:
            self.conn.backup(backup)

        finally:
            backup.close()

        self.conn.execute("ALTER TABLE outcomes RENAME TO outcomes_v1")

    def _restore_single_window_outcomes(self) -> None:
        """Its rows become the first window's verdicts (also after a restart midway)."""

        if not self.conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' "
                                 "AND name = 'outcomes_v1'").fetchone():
            return

        self.conn.execute("BEGIN")

        try:
            self.conn.execute(
                "INSERT OR IGNORE INTO outcomes (id, incident_id, action_id, window_seconds, verdict, "
                "period, details_json, evaluated_at) SELECT id, incident_id, action_id, ?, verdict, "
                "period, details_json, evaluated_at FROM outcomes_v1", (config.VERIFY_WINDOW_SECONDS,))
            self.conn.execute("DROP TABLE outcomes_v1")
            self.conn.execute("COMMIT")

        except Exception:
            self.conn.execute("ROLLBACK")
            raise

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

    def fill_samples(self, deployment_crn: str, samples: list, spacing: float) -> int:
        """
        Add older history: each (kpi_key, t, value) whose key has no sample
        within spacing/2 of t. A coarse chart fills the gaps between
        polls without doubling the buckets already stored. Returns the
        number added.
        """

        by_key = {}

        for key, t, value in samples:
            by_key.setdefault(key, []).append((t, value))

        added = []

        with self.lock:
            for key, points in by_key.items():
                low = min(t for t, _ in points) - spacing
                high = max(t for t, _ in points) + spacing
                have = [r["t"] for r in self.conn.execute(
                    "SELECT t FROM kpi_samples WHERE deployment_crn = ? AND kpi_key = ? "
                    "AND t BETWEEN ? AND ? ORDER BY t", (deployment_crn, key, low, high))]

                for t, value in points:
                    at = bisect.bisect_left(have, t)
                    near = [have[i] for i in (at - 1, at) if 0 <= i < len(have)]

                    if all(abs(t - n) >= spacing / 2 for n in near):
                        added.append((deployment_crn, key, t, value))

            self.conn.executemany(
                "INSERT OR IGNORE INTO kpi_samples (deployment_crn, kpi_key, t, value) "
                "VALUES (?, ?, ?, ?)", added)

        return len(added)

    def history(self, deployment_crn: str, since: float, step: float) -> dict:
        """
        Every KPI's samples since `since`, averaged onto a grid of `step`
        seconds: {kpi_key: {bucket index: mean}}. Bucket i covers
        [since + i*step, since + (i+1)*step).
        """

        with self.lock:
            rows = self.conn.execute(
                "SELECT kpi_key, CAST((t - ?) / ? AS INTEGER) AS b, AVG(value) AS v "
                "FROM kpi_samples WHERE deployment_crn = ? AND t >= ? AND value IS NOT NULL "
                "GROUP BY kpi_key, b", (since, step, deployment_crn, since)).fetchall()

        grid = {}

        for row in rows:
            grid.setdefault(row["kpi_key"], {})[row["b"]] = row["v"]

        return grid

    def first_sample_at(self, deployment_crn: str) -> float | None:
        row = self._one("SELECT MIN(t) AS t FROM kpi_samples WHERE deployment_crn = ?",
                        (deployment_crn,))

        return row["t"] if row else None

    def add_changes(self, deployment_crn: str, pack: str, changes: list) -> int:
        """changes: [{event_crn, t, kind, name}]; an event already stored is kept."""

        with self.lock:
            before = self.conn.total_changes
            self.conn.executemany(
                "INSERT OR IGNORE INTO changes (deployment_crn, event_crn, pack, t, kind, name) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [(deployment_crn, c["event_crn"], pack, c["t"], c["kind"], c.get("name"))
                 for c in changes])

            return self.conn.total_changes - before

    def changes(self, deployment_crn: str, since: float = 0.0) -> list:
        return self._all("SELECT * FROM changes WHERE deployment_crn = ? AND t >= ? ORDER BY t",
                         (deployment_crn, since))

    # -- incidents -------------------------------------------------------

    def create_incident(self, pack: str, deployment_crn: str, deployed_flow_crn: str,
                        scenario: str, process_group: str, signals: list, summary: str,
                        expected_flow_version_crn: str, now: float, kind: str | None = None,
                        rollback_of_action_id: int | None = None) -> dict:
        stamp = datetime.fromtimestamp(now, timezone.utc).strftime("%Y%m%d-%H%M%S")
        incident_id = f"INC-{stamp}-{scenario or 'PG'}-{uuid.uuid4().hex[:4]}"
        self._insert("incidents", {
            "id": incident_id, "pack": pack, "deployment_crn": deployment_crn,
            "deployed_flow_crn": deployed_flow_crn, "scenario": scenario,
            "process_group": process_group, "status": "NEW", "summary": summary,
            "signals": signals, "expected_flow_version_crn": expected_flow_version_crn,
            "opened_at": now, "updated_at": now, "last_signal_at": now,
            "kind": kind, "rollback_of_action_id": rollback_of_action_id,
        })

        return self.incident(incident_id)

    def rollbacks_of(self, action_id: int) -> list:
        """The rollback incidents proposed for one action, oldest first."""

        return self._all("SELECT * FROM incidents WHERE rollback_of_action_id = ? ORDER BY opened_at",
                         (action_id,))

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

    def windows_due(self, now: float) -> list:
        """
        [(action, window seconds)]: each applied action's verify windows
        that have closed by `now` (outcomes.due_at) with no verdict yet.
        """

        actions = self._all("SELECT * FROM actions WHERE status = 'APPLIED' AND dry_run = 0 "
                            "AND finished_at IS NOT NULL ORDER BY finished_at")

        with self.lock:
            judged = {tuple(r) for r in self.conn.execute("SELECT action_id, window_seconds FROM outcomes")}

        return [(a, window) for a in actions for window in config.VERIFY_WINDOWS
                if (a["id"], window) not in judged
                and due_at(a["finished_at"], window, a.get("started_at")) <= now]

    def add_outcome(self, incident_id: str, action_id: int, verdict: str, period: str,
                    details: dict, now: float, window: int | None = None) -> int:
        return self._insert("outcomes", {
            "incident_id": incident_id, "action_id": action_id, "verdict": verdict,
            "window_seconds": window or config.VERIFY_WINDOW_SECONDS,
            "period": period, "details": details, "evaluated_at": now,
        })

    def outcomes(self, incident_id: str = "") -> list:
        """Oldest action first, and each action's windows shortest first."""

        if incident_id:
            return self._all("SELECT * FROM outcomes WHERE incident_id = ? "
                             "ORDER BY action_id, window_seconds", (incident_id,))

        return self._all("SELECT * FROM outcomes ORDER BY evaluated_at DESC")

    def update_outcome(self, outcome_id: int, **fields) -> None:
        self._update("outcomes", "id", outcome_id, fields)

    def supersede_outcome(self, incident_id: str, action_id: int, window: int, verdict: str,
                          period: str, details: dict, now: float) -> int:
        """A re-check's verdict for one window: the one it replaces is kept in superseded_outcomes."""

        with self.lock:
            self.conn.execute("BEGIN")

            try:
                old = self.conn.execute("SELECT * FROM outcomes WHERE action_id = ? AND window_seconds = ?",
                                        (action_id, window)).fetchone()

                if old:
                    self.conn.execute(
                        "INSERT INTO superseded_outcomes (outcome_id, incident_id, action_id, window_seconds, "
                        "verdict, period, details_json, evaluated_at, superseded_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (old["id"], old["incident_id"], old["action_id"], old["window_seconds"], old["verdict"],
                         old["period"], old["details_json"], old["evaluated_at"], now))
                    self.conn.execute("UPDATE outcomes SET verdict = ?, period = ?, details_json = ?, "
                                      "evaluated_at = ? WHERE id = ?",
                                      (verdict, period, _encode(details), now, old["id"]))
                    found = old["id"]

                else:
                    found = self.conn.execute(
                        "INSERT INTO outcomes (incident_id, action_id, window_seconds, verdict, period, "
                        "details_json, evaluated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (incident_id, action_id, window, verdict, period, _encode(details), now)).lastrowid

                self.conn.execute("COMMIT")

            except Exception:
                self.conn.execute("ROLLBACK")
                raise

        return found

    def superseded_outcomes(self, incident_id: str) -> list:
        return self._all("SELECT * FROM superseded_outcomes WHERE incident_id = ? "
                         "ORDER BY action_id, window_seconds, superseded_at", (incident_id,))

    def add_label(self, incident_id: str, action_id: int, label: str, reason: str, reviewer: str,
                  now: float) -> int:
        return self._insert("action_labels", {"incident_id": incident_id, "action_id": action_id,
                                              "label": label, "reason": reason, "reviewer": reviewer,
                                              "created_at": now})

    def label(self, label_id: int) -> dict | None:
        return self._one("SELECT * FROM action_labels WHERE id = ?", (label_id,))

    def labels(self, incident_id: str = "") -> dict:
        """{action id: its latest label row}, of one incident or all."""

        rows = self._all("SELECT * FROM action_labels WHERE incident_id = ? ORDER BY id", (incident_id,)) \
            if incident_id else self._all("SELECT * FROM action_labels ORDER BY id")

        return {r["action_id"]: r for r in rows}

    # -- playbook edits ---------------------------------------------------

    def add_playbook_edit(self, trigger_kind: str, incident_id: str, source_id: int, now: float,
                          **fields) -> int | None:
        """The new edit's id; None when this source was already drafted."""

        try:
            return self._insert("playbook_edits", {"status": "DRAFTING", "trigger_kind": trigger_kind,
                                                   "incident_id": incident_id, "source_id": source_id,
                                                   "created_at": now, **fields})

        except sqlite3.IntegrityError:
            return None

    def update_playbook_edit(self, edit_id: int, **fields) -> None:
        self._update("playbook_edits", "id", edit_id, fields)

    def playbook_edit(self, edit_id) -> dict | None:
        return self._one("SELECT * FROM playbook_edits WHERE id = ?", (edit_id,))

    def playbook_edits(self, statuses=None) -> list:
        """Newest first."""

        if statuses:
            marks = ", ".join("?" for _ in statuses)

            return self._all(f"SELECT * FROM playbook_edits WHERE status IN ({marks}) ORDER BY id DESC",
                             tuple(statuses))

        return self._all("SELECT * FROM playbook_edits ORDER BY id DESC")

    def applied_playbook_edits(self) -> list:
        """The edits the overlay applies, oldest first: a later edit of a section reads the earlier one's text."""

        return self._all("SELECT * FROM playbook_edits WHERE status = 'APPLIED' ORDER BY id")

    # -- auto-approvals and settings ----------------------------------------

    def add_auto_approval(self, incident_id: str, proposal_id, fix: str, basis: str, due_at: float,
                          now: float) -> bool:
        """False when the incident already has one (a run paused again, the app restarted)."""

        try:
            self._insert("auto_approvals", {"incident_id": incident_id, "proposal_id": proposal_id, "fix": fix,
                                            "basis": basis, "status": "WAITING", "due_at": due_at,
                                            "created_at": now})
            return True

        except sqlite3.IntegrityError:
            return False

    def auto_approval(self, incident_id: str) -> dict | None:
        return self._one("SELECT * FROM auto_approvals WHERE incident_id = ?", (incident_id,))

    def auto_approvals(self, statuses=None) -> list:
        """Oldest due first."""

        if statuses:
            marks = ", ".join("?" for _ in statuses)

            return self._all(f"SELECT * FROM auto_approvals WHERE status IN ({marks}) ORDER BY due_at",
                             tuple(statuses))

        return self._all("SELECT * FROM auto_approvals ORDER BY due_at")

    def claim_auto_approval(self, incident_id: str, status: str, now: float, reason: str = "",
                            decided_by: str = "") -> bool:
        """Move a WAITING row on; False when something else already did (a click and the tick at once)."""

        with self.lock:
            cursor = self.conn.execute(
                "UPDATE auto_approvals SET status = ?, reason = ?, decided_by = ?, decided_at = ? "
                "WHERE incident_id = ? AND status = 'WAITING'", (status, reason, decided_by, now, incident_id))

            return cursor.rowcount == 1

    def setting(self, key: str) -> dict | None:
        return self._one("SELECT * FROM settings WHERE key = ?", (key,))

    def set_setting(self, key: str, value: str, updated_by: str, now: float) -> None:
        with self.lock:
            self.conn.execute("INSERT INTO settings (key, value, updated_by, updated_at) VALUES (?, ?, ?, ?) "
                              "ON CONFLICT (key) DO UPDATE SET value = excluded.value, "
                              "updated_by = excluded.updated_by, updated_at = excluded.updated_at",
                              (key, value, updated_by, now))

    # -- chaos injections ----------------------------------------------------

    def add_injection(self, pack: str, deployment_crn: str, scenario: str, family: str, status: str,
                      at: float, **fields) -> int:
        return self._insert("chaos_injections", dict(fields, pack=pack, deployment_crn=deployment_crn,
                                                     scenario=scenario, family=family, status=status, at=at))

    def update_injection(self, injection_id: int, **fields) -> None:
        self._update("chaos_injections", "id", injection_id, fields)

    def injections(self, pack: str = "", deployment_crn: str = "", scenario: str = "",
                   since: float = 0.0, statuses=None) -> list:
        """Oldest first."""

        sql, params = "SELECT * FROM chaos_injections WHERE at >= ?", [since]

        for column, value in (("pack", pack), ("deployment_crn", deployment_crn), ("scenario", scenario)):
            if value:
                sql += f" AND {column} = ?"
                params.append(value)

        if statuses:
            sql += f" AND status IN ({', '.join('?' for _ in statuses)})"
            params += list(statuses)

        return self._all(sql + " ORDER BY at, id", params)

    # -- the app's health, and retention -----------------------------------

    def add_health(self, t: float, values: dict) -> None:
        """values: {key: number}; a key without a number is skipped."""

        rows = [(t, key, float(value)) for key, value in values.items()
                if isinstance(value, (int, float)) and not isinstance(value, bool)]

        with self.lock:
            self.conn.executemany("INSERT OR REPLACE INTO health_samples (t, key, value) VALUES (?, ?, ?)", rows)

    def health(self, key: str, since: float = 0.0) -> list:
        with self.lock:
            rows = self.conn.execute("SELECT t, value FROM health_samples WHERE key = ? AND t >= ? ORDER BY t",
                                     (key, since)).fetchall()

        return [(r["t"], r["value"]) for r in rows]

    def latest_health(self) -> dict:
        """{key: (t, value)} from the newest sample of each key."""

        rows = self._all("SELECT key, t, value FROM health_samples h WHERE t = "
                         "(SELECT MAX(t) FROM health_samples WHERE key = h.key)")

        return {r["key"]: (r["t"], r["value"]) for r in rows}

    def _delete_batches(self, sql: str, params: tuple, batch: int) -> int:
        deleted = 0

        while True:
            with self.lock:
                count = self.conn.execute(sql, params + (batch,)).rowcount

            deleted += count

            if count < batch:
                return deleted

    def prune_samples(self, before: float, batch: int = 5000) -> int:
        """
        Delete KPI samples older than `before`, deployment by deployment
        (the time index leads with it) and a batch per statement, so the
        shared connection is never held long. No VACUUM: SQLite reuses
        the freed pages. Returns rows deleted.
        """

        crns = [r["crn"] for r in self._all("SELECT crn FROM deployments")]

        return sum(self._delete_batches(
            "DELETE FROM kpi_samples WHERE rowid IN (SELECT rowid FROM kpi_samples "
            "WHERE deployment_crn = ? AND t < ? LIMIT ?)", (crn, before), batch) for crn in crns)

    def prune_health(self, before: float, batch: int = 5000) -> int:
        return self._delete_batches("DELETE FROM health_samples WHERE rowid IN "
                                    "(SELECT rowid FROM health_samples WHERE t < ? LIMIT ?)", (before,), batch)

    def closed_between(self, start: float, end: float) -> list:
        """Ids of incidents closed in [start, end), the ones whose checkpoints may go."""

        marks = ", ".join("?" for _ in CLOSED_STATUSES)

        return [r["id"] for r in self._all(
            f"SELECT id FROM incidents WHERE status IN ({marks}) AND closed_at >= ? AND closed_at < ? "
            "ORDER BY closed_at", CLOSED_STATUSES + (start, end))]

    def record(self, since: float = 0.0) -> dict:
        """Every incident opened at or after `since`, with all its rows, by table: the scorecard's reads."""

        mine = "incident_id IN (SELECT id FROM incidents WHERE opened_at >= ?)"
        out = {"incidents": self._all("SELECT * FROM incidents WHERE opened_at >= ? ORDER BY opened_at",
                                      (since,))}

        for table in ("proposals", "reviews", "actions", "outcomes", "action_labels"):
            out[table] = self._all(f"SELECT * FROM {table} WHERE {mine} ORDER BY id", (since,))

        return out


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
