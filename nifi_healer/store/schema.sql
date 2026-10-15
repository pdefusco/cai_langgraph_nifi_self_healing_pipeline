-- The healer's record: what it saw, what it proposed, what a human
-- decided, what it did, and what happened next (the ground truth that
-- Phases 2 and 3 read).
--
-- Times are epoch seconds in the tenant's clock (client.now(); the
-- simulated clock in mock mode). Columns ending _json hold JSON text.
-- The app is the only writer; the Phase 2 dashboard only reads.

CREATE TABLE IF NOT EXISTS deployments (
    crn                 TEXT PRIMARY KEY,
    pack                TEXT NOT NULL,
    name                TEXT,
    state               TEXT,
    flow_version        INTEGER,
    flow_version_crn    TEXT,
    cluster_size        TEXT,
    node_count          INTEGER,
    alerts_json         TEXT,
    seen_at             REAL
);

-- Every flow version a deployment has run, and who made it.
CREATE TABLE IF NOT EXISTS flow_versions (
    crn                 TEXT PRIMARY KEY,
    pack                TEXT NOT NULL,
    flow_crn            TEXT,
    version             INTEGER,
    source              TEXT,               -- baseline | agent | external
    incident_id         TEXT,
    comments            TEXT,
    first_seen_at       REAL
);

-- The newest bucket of each KPI chart, once per bucket. Buckets are
-- anchored to each request, so these are for display, not verdicts.
CREATE TABLE IF NOT EXISTS kpi_samples (
    deployment_crn      TEXT NOT NULL,
    kpi_key             TEXT NOT NULL,
    t                   REAL NOT NULL,
    value               REAL,
    PRIMARY KEY (deployment_crn, kpi_key, t)
);

-- The Monitor tab reads a time window of every KPI of a deployment, and
-- its first sample: by time, not by key, or each read scans all history.
CREATE INDEX IF NOT EXISTS kpi_samples_by_time
    ON kpi_samples (deployment_crn, t);

-- What changed on a deployment and when, from its CDF event log
-- (list-deployment-events): flow version, parameters, sizing, KPIs.
-- Whoever made it, the agent or an operator; the Monitor tab tells them
-- apart by the agent's own actions. Who is not stored.
CREATE TABLE IF NOT EXISTS changes (
    deployment_crn      TEXT NOT NULL,
    event_crn           TEXT NOT NULL,
    pack                TEXT,
    t                   REAL NOT NULL,
    kind                TEXT NOT NULL,      -- created | flow_version | parameters | sizing | kpis | other
    name                TEXT,
    PRIMARY KEY (deployment_crn, event_crn)
);

CREATE TABLE IF NOT EXISTS incidents (
    id                  TEXT PRIMARY KEY,
    pack                TEXT NOT NULL,
    deployment_crn      TEXT NOT NULL,
    deployed_flow_crn   TEXT,
    scenario            TEXT,
    process_group       TEXT NOT NULL,
    status              TEXT NOT NULL,
    summary             TEXT,
    signals_json        TEXT,
    expected_flow_version_crn TEXT,
    verdict             TEXT,
    opened_at           REAL NOT NULL,
    updated_at          REAL NOT NULL,
    last_signal_at      REAL,
    closed_at           REAL,
    -- NULL for a fault the agent diagnoses; 'rollback' for a proposal to
    -- undo one applied action (rollback_of_action_id), which skips the LLM.
    kind                TEXT,
    rollback_of_action_id INTEGER
);

CREATE INDEX IF NOT EXISTS incidents_by_group
    ON incidents (deployment_crn, process_group, status);

CREATE TABLE IF NOT EXISTS proposals (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id         TEXT NOT NULL REFERENCES incidents (id),
    attempt             INTEGER,
    agent_version       TEXT,
    diagnosis           TEXT,
    action_json         TEXT,
    rationale           TEXT,
    citations_json      TEXT,
    validation_json     TEXT,
    risk                TEXT,
    status              TEXT,               -- VALID | INVALID | NO_ACTION | SUPERSEDED
    created_at          REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS reviews (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id         TEXT NOT NULL REFERENCES incidents (id),
    proposal_id         INTEGER REFERENCES proposals (id),
    decision            TEXT NOT NULL,      -- approve | reject | edit | rediagnose
    reviewer            TEXT,
    reason              TEXT,
    edits_json          TEXT,
    created_at          REAL NOT NULL
);

-- One mutation (or its dry run). started_at/finished_at bracket the
-- change; the outcome evaluator splits the KPI series around them.
CREATE TABLE IF NOT EXISTS actions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id         TEXT NOT NULL REFERENCES incidents (id),
    proposal_id         INTEGER REFERENCES proposals (id),
    pack                TEXT NOT NULL,
    deployment_crn      TEXT NOT NULL,
    action_type         TEXT NOT NULL,
    family              TEXT,
    dry_run             INTEGER NOT NULL,
    status              TEXT NOT NULL,      -- PLANNED | APPLYING | APPLIED | DRY_RUN | REFUSED | FAILED
    argv_json           TEXT,
    result_json         TEXT,
    from_flow_version_crn TEXT,
    to_flow_version_crn TEXT,
    error               TEXT,
    started_at          REAL,
    finished_at         REAL
);

-- One verdict per action and verify window (config.VERIFY_WINDOWS):
-- 15 min, 1 h and 12 h after the change, each from its own chart.
CREATE TABLE IF NOT EXISTS outcomes (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id         TEXT NOT NULL REFERENCES incidents (id),
    action_id           INTEGER NOT NULL REFERENCES actions (id),
    window_seconds      INTEGER NOT NULL,
    verdict             TEXT NOT NULL,      -- IMPROVED | NO_CHANGE | WORSENED | INCONCLUSIVE
    period              TEXT,
    details_json        TEXT,
    evaluated_at        REAL NOT NULL,
    UNIQUE (action_id, window_seconds)
);

-- A verdict a reviewer's re-check replaced, as it was.
CREATE TABLE IF NOT EXISTS superseded_outcomes (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    outcome_id          INTEGER NOT NULL REFERENCES outcomes (id),
    incident_id         TEXT NOT NULL REFERENCES incidents (id),
    action_id           INTEGER NOT NULL REFERENCES actions (id),
    window_seconds      INTEGER NOT NULL,
    verdict             TEXT NOT NULL,
    period              TEXT,
    details_json        TEXT,
    evaluated_at        REAL NOT NULL,
    superseded_at       REAL NOT NULL
);

-- A reviewer's call on an applied fix, apart from what was measured (a
-- right fix can measure NO_CHANGE when another fault hides it). The
-- verdicts above stay as measured; an action's latest label is its call.
CREATE TABLE IF NOT EXISTS action_labels (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id         TEXT NOT NULL REFERENCES incidents (id),
    action_id           INTEGER NOT NULL REFERENCES actions (id),
    label               TEXT NOT NULL,      -- RIGHT_FIX | WRONG_FIX | UNCLEAR
    reason              TEXT NOT NULL,
    reviewer            TEXT,
    created_at          REAL NOT NULL
);

-- A playbook edit drafted from a reviewer's rejection or WRONG_FIX label
-- (rag/drafter.py). An APPLIED edit overlays playbooks/*.md when Chroma is
-- built (rag/overlay.py); the files themselves are never written.
-- old_text is the section's body as the drafter saw it: the overlay
-- applies an edit only while the section still reads that way.
CREATE TABLE IF NOT EXISTS playbook_edits (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    status              TEXT NOT NULL,      -- DRAFTING | PROPOSED | FAILED | SKIPPED | APPLIED | REJECTED
                                            -- | SUPERSEDED | REVERTED | STALE | MERGED
    trigger_kind        TEXT NOT NULL,      -- reject | wrong_fix
    incident_id         TEXT NOT NULL REFERENCES incidents (id),
    source_id           INTEGER NOT NULL,   -- the review (reject) or action label (wrong_fix)
    sources_json        TEXT,               -- [{kind, incident_id, id}], this one and those folded in
    playbook            TEXT,               -- the file name, backlog-slow-consumer.md
    section_id          TEXT,               -- its chunk id; "" for a new section
    heading             TEXT,
    old_text            TEXT,
    new_text            TEXT,
    rationale           TEXT,
    problems_json       TEXT,
    drafted_by          TEXT,
    error               TEXT,
    reviewer            TEXT,
    reason              TEXT,
    created_at          REAL NOT NULL,
    decided_at          REAL
);

CREATE UNIQUE INDEX IF NOT EXISTS playbook_edits_source ON playbook_edits (trigger_kind, source_id);
