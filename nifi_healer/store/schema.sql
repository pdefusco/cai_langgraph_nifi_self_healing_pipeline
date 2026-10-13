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
    closed_at           REAL
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

CREATE TABLE IF NOT EXISTS outcomes (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id         TEXT NOT NULL REFERENCES incidents (id),
    action_id           INTEGER NOT NULL UNIQUE REFERENCES actions (id),
    verdict             TEXT NOT NULL,      -- IMPROVED | NO_CHANGE | WORSENED | INCONCLUSIVE
    period              TEXT,
    details_json        TEXT,
    evaluated_at        REAL NOT NULL
);
