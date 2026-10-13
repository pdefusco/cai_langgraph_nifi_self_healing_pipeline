"""Detectors, the outcome verdicts and the incident lifecycle, on synthetic series."""

import glob
import sqlite3

import pytest

import config
from nifi_healer import fleet
from nifi_healer.kpi import detectors, metrics, outcomes
from nifi_healer.runtime.poller import Poller
from nifi_healer.store.db import Store

STEP = 72.0     # a 30-minute chart's bucket width
NOW = 1_800_000_000.0


def kpi(scenario="S01", kind="connection", component="s01-backlog", metric="queued_count",
        above=500, below=None, tolerance_seconds=60, good="down", primary=True):
    return {"scenario": scenario, "kind": kind, "component": component, "metric": metric,
            "above": above, "below": below, "tolerance_seconds": tolerance_seconds, "good": good,
            "primary": primary, "key": f"{scenario}:{kind}:{component}:{metric}", "chart": None}


def series(values, end=NOW, step=STEP):
    start = end - step * (len(values) - 1)

    return [(start + i * step, float(v)) for i, v in enumerate(values)]


def chart(k, values, end=NOW, step=STEP):
    scope = {"connection": "CONNECTION", "processor": "PROCESSOR", "system": "DEPLOYMENT"}[k["kind"]]
    label = metrics.metric(k["metric"]).label

    return {"componentType": scope, "componentName": k["component"], "name": label,
            "metrics": {"datas": [{"timestamp": t * 1000, "value": v}
                                  for t, v in series(values, end, step)]}}


# ---------------------------------------------------------------------
# Detectors
# ---------------------------------------------------------------------


def test_breach_needs_debounce(monkeypatch):
    monkeypatch.setattr(config, "BREACH_DEBOUNCE", 2)
    k = kpi()

    assert detectors.breach(k, series([100] * 10 + [600]), NOW) is None

    found = detectors.breach(k, series([100] * 10 + [600, 700]), NOW)
    assert found["type"] == "kpi_breach" and found["severity"] == detectors.CRITICAL
    assert found["process_group"] == "S01 Slow Consumer"


def test_breach_honours_tolerance(monkeypatch):
    monkeypatch.setattr(config, "BREACH_DEBOUNCE", 1)
    k = kpi(tolerance_seconds=600)

    assert detectors.breach(k, series([100] * 10 + [600] * 3), NOW) is None
    assert detectors.breach(k, series([100] * 10 + [600] * 9), NOW) is not None


def test_below_threshold():
    k = kpi(kind="processor", component="S15 Batcher", metric="flowfiles_out",
            above=None, below=0.001, good="up")

    assert detectors.breach(k, series([0] * 25), NOW)["summary"].count("below")


def test_trend_to_breach_is_a_warning(monkeypatch):
    monkeypatch.setattr(config, "TREND_HORIZON_SECONDS", 900)
    k = kpi()
    rising = series([100 + 30 * i for i in range(12)])      # ~25/min, 430 now

    found = detectors.trend_to_breach(k, rising, NOW)
    assert found and found["severity"] == detectors.WARNING

    assert detectors.trend_to_breach(k, series([100] * 12), NOW) is None
    assert detectors.by_group([found]) == {}


def test_imbalance_flags_silent_loss():
    up = kpi("S13", "processor", "S13 Source", "flowfiles_out", above=None, good="up")
    down = kpi("S13", "processor", "S13 Consumer", "flowfiles_in", above=None, good="up")
    queue = kpi("S13", "connection", "s13-expiring", "queued_count", above=500)
    charts = [chart(up, [3] * 25), chart(down, [1] * 25), chart(queue, [80] * 25)]

    found = detectors.detect("p4", {"kpis": [up, down, queue]}, charts, [], NOW)
    types = [s["type"] for s in found["signals"]]
    assert types == ["flow_imbalance"]
    assert found["kpis"][down["key"]]["status"] == "imbalance"


def test_imbalance_ignored_across_a_processor_that_merges(tenant):
    up = kpi("S15", "processor", "S15 Batcher", "flowfiles_in", above=None, good="up", primary=False)
    down = kpi("S15", "processor", "S15 Batcher", "flowfiles_out", above=None, good="up")
    charts = [chart(up, [60] * 25), chart(down, [5] * 25)]
    entry = {"kpis": [up, down]}

    assert [s["type"] for s in detectors.detect("p4", entry, charts, [], NOW)["signals"]] == ["flow_imbalance"]
    assert detectors.detect("p4", entry, charts, [], NOW, {"S15 Batcher": "MergeContent"})["signals"] == []

    # The poller reads the kinds from the deployed flow version, once per version.
    from nifi_healer import fleet
    from nifi_healer.runtime.poller import Poller

    _, client, store = tenant("p4-dataloss-latency")
    entry = fleet.pack("p4-dataloss-latency")
    poller = Poller(client, store)
    flow = client.flow_facts(entry["deployment_crn"], entry["deployed_flow_crn"])
    kinds = poller.processor_kinds(flow)

    assert kinds["S15 Batcher"] == "MergeContent" and poller.processor_kinds(flow) is kinds
    assert poller.processor_kinds({}) == {}


def test_imbalance_ignored_while_a_queue_holds_the_difference():
    up = kpi("S01", "processor", "S01 Source", "flowfiles_out", above=None, good="up")
    down = kpi("S01", "processor", "S01 Consumer", "flowfiles_in", above=None, good="up")
    queue = kpi("S01", "connection", "s01-backlog", "queued_count", above=100_000)
    charts = [chart(up, [3] * 25), chart(down, [1] * 25),
              chart(queue, [100 + 150 * i for i in range(25)])]

    found = detectors.detect("p1", {"kpis": [up, down, queue]}, charts, [], NOW)
    assert not [s for s in found["signals"] if s["type"] == "flow_imbalance"]


def test_healthy_group_is_quiet():
    queue = kpi("S19", "connection", "s19-healthy", "queued_count", above=500)
    charts = [chart(queue, [15] * 25)]

    found = detectors.detect("p5", {"kpis": [queue]}, charts, [], NOW)
    assert found["signals"] == [] and found["kpis"][queue["key"]]["status"] == "ok"


def test_alert_matching():
    backlog = kpi()
    other = kpi("S02", component="s02-backlog")
    event = {"name": "Flow Files Queued on s01-backlog", "message": "Flow Files Queued above 500",
             "severity": "WARNING", "firstOccurrence": NOW * 1000}

    assert detectors.match_alert(event, [other, backlog]) is backlog
    assert detectors.match_alert({"name": "Node restarted", "message": ""}, [backlog]) is None

    signals, unmatched = detectors.alert_signals([event, {"name": "x"}], [backlog], NOW)
    assert [s["type"] for s in signals] == ["platform_alert"] and len(unmatched) == 1
    assert signals[0]["evidence"]["alert_severity"] == "WARNING"


# ---------------------------------------------------------------------
# Outcomes
# ---------------------------------------------------------------------


@pytest.fixture
def windows(monkeypatch):
    monkeypatch.setattr(config, "VERIFY_SETTLE_SECONDS", 300)
    monkeypatch.setattr(config, "VERIFY_WINDOW_SECONDS", 900)


def around_change(pre, post, step=144.0):
    """A one-hour chart with a change at its 13th bucket: (series, started_at, finished_at)."""

    gap = 3     # buckets inside the settle gap
    values = list(pre) + [pre[-1]] * gap + list(post)
    data = series(values, step=step)
    started = data[len(pre)][0] - 1

    return data, started, started + 60


def test_draining_backlog_improves(windows):
    data, started, finished = around_change([1000 + 100 * i for i in range(8)],
                                            [1700 - 150 * i for i in range(8)])
    result = outcomes.evaluate([kpi()], {kpi()["key"]: data}, started, finished)

    assert result["verdict"] == outcomes.IMPROVED


def test_steady_growth_is_no_change(windows):
    data, started, finished = around_change([1000 + 100 * i for i in range(8)],
                                            [2100 + 100 * i for i in range(8)])
    result = outcomes.evaluate([kpi()], {kpi()["key"]: data}, started, finished)

    assert result["verdict"] == outcomes.NO_CHANGE


def test_faster_growth_worsens(windows):
    data, started, finished = around_change([1000 + 20 * i for i in range(8)],
                                            [1300 + 200 * i for i in range(8)])
    result = outcomes.evaluate([kpi()], {kpi()["key"]: data}, started, finished)

    assert result["verdict"] == outcomes.WORSENED


def test_rate_kpi_and_side_effects(windows):
    rate = kpi("S01", "processor", "S01 Consumer", "flowfiles_in", above=None, good="up",
               primary=False)
    queue = kpi()
    q_data, started, finished = around_change([2000] * 8, [400] * 8)
    r_data, _, _ = around_change([1.0] * 8, [0.5] * 8)

    result = outcomes.evaluate([queue, rate], {queue["key"]: q_data, rate["key"]: r_data},
                               started, finished)
    assert result["verdict"] == outcomes.IMPROVED
    assert result["side_effects"] == [rate["key"]]


def test_the_kpi_that_opened_the_incident_decides_too(windows):
    # S17 live: the bad endpoint fails fast, so its queue never fills; only the stub's rate shows it.
    queue = kpi("S17", component="s17-requests", above=50)
    stub = kpi("S17", "processor", "S17 Partner Stub", "flowfiles_out", above=None, good="up",
               primary=False)
    q_data, started, finished = around_change([0] * 8, [0] * 8)
    s_data, _, _ = around_change([0] * 8, [0.2] * 8)
    series_by_key = {queue["key"]: q_data, stub["key"]: s_data}

    assert outcomes.evaluate([queue, stub], series_by_key, started, finished)["verdict"] == outcomes.NO_CHANGE

    result = outcomes.evaluate([queue, stub], series_by_key, started, finished, {stub["key"]})
    assert result["verdict"] == outcomes.IMPROVED
    assert [j["signalled"] for j in result["kpis"]] == [False, True]


def test_too_few_buckets_is_inconclusive(windows):
    data, started, finished = around_change([1000] * 8, [900])
    result = outcomes.evaluate([kpi()], {kpi()["key"]: data}, started, finished)

    assert result["verdict"] == outcomes.INCONCLUSIVE


def test_plan_and_due(windows):
    assert outcomes.plan(900, NOW - 1200, NOW)["period"] == "LAST_ONE_HOUR"
    assert outcomes.plan(900, NOW - 600, NOW)["period"] == "LAST_THIRTY_MINUTES"

    hour = outcomes.plan(3600, NOW - 5400, NOW)
    assert hour["period"] == "LAST_TWELVE_HOURS" and hour["settle"] == 1800 and hour["before"] == 5400
    assert outcomes.plan(43200, NOW - 13 * 3600, NOW)["period"] == "LAST_ONE_DAY"

    # Past the 1 h chart, no chart has two buckets in 15 minutes.
    assert outcomes.plan(900, NOW - 5 * 3600, NOW) is None
    assert outcomes.plan(43200, NOW - 30 * 3600, NOW) is None

    assert outcomes.due_at(NOW, 900, NOW - 60) == NOW + 1200
    assert outcomes.due_at(NOW, 3600) == NOW + 5400 and outcomes.due_at(NOW, 43200) == NOW + 46800


def test_coarse_windows_drop_the_straddling_buckets(windows):
    hour = outcomes.plan(3600, NOW - 5400, NOW)
    data = series([10] * 25, step=1728.0)
    started = data[20][0] - 100              # bucket 19 runs into the change
    before, after = outcomes.windows(data, started, started + 60, hour)

    assert [t for t, _ in before] == [data[17][0], data[18][0]]
    assert after and after[0][0] >= started + 60 + 1800 and after[-1][0] <= started + 60 + 1800 + 3600

    q = kpi()
    result = outcomes.evaluate([q], {q["key"]: data}, started, started + 60, span=hour)
    assert result["window_seconds"] == 3600 and result["settle_seconds"] == 1800


# ---------------------------------------------------------------------
# Incidents: one per process group, cooldown after close
# ---------------------------------------------------------------------


class _Clock:
    kind = "fake"

    def __init__(self):
        self.t = NOW

    def now(self):
        return self.t


def test_incident_lifecycle(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "INCIDENT_COOLDOWN_SECONDS", 900)
    monkeypatch.setattr(config, "AUTO_DIAGNOSE", True)
    store, clock, opened = Store(str(tmp_path / "healer.db")), _Clock(), []
    poller = Poller(clock, store, on_incident=opened.append)
    entry = {"deployment_crn": "crn:d", "deployed_flow_crn": "crn:f", "kpis": [kpi()]}
    signal = detectors.breach(kpi(), series([600] * 10), NOW)

    first = poller.open_incidents("p1", entry, [signal, dict(signal)], {"flow_version_crn": "v1"}, NOW)
    assert len(first) == 1 and len(opened) == 1
    assert first[0]["expected_flow_version_crn"] == "v1" and first[0]["status"] == "NEW"

    assert poller.open_incidents("p1", entry, [signal], {}, NOW + 30) == []
    assert store.incident(first[0]["id"])["last_signal_at"] == NOW + 30

    store.update_incident(first[0]["id"], NOW + 60, status="REJECTED")
    assert poller.open_incidents("p1", entry, [signal], {}, NOW + 600) == []
    assert len(poller.open_incidents("p1", entry, [signal], {}, NOW + 1000)) == 1


# ---------------------------------------------------------------------
# Verify windows: 15 min, 1 h and 12 h after a change
# ---------------------------------------------------------------------


class _Charts(_Clock):
    def __init__(self):
        super().__init__()
        self.periods = []

    def metric_charts(self, deployment_crn, deployed_flow_crn, time_period=""):
        self.periods.append(time_period)

        return {"sources": {"flow": "ok"}, "charts": []}


@pytest.fixture
def verifying(tmp_path, monkeypatch, windows):
    """(poller, store, clock, add, verdicts): add(group) makes a VERIFYING incident with a fix that finished at NOW."""

    monkeypatch.setattr(config, "VERIFY_WINDOWS", (900, 3600, 43200))
    entry = {"deployment_crn": "crn:d", "deployed_flow_crn": "crn:f", "kpis": [kpi()]}
    monkeypatch.setattr(fleet, "pack", lambda name: entry)
    verdicts = []
    monkeypatch.setattr(outcomes, "evaluate", lambda *a, **k: {"verdict": verdicts.pop(0), "kpis": [],
                                                               "side_effects": []})
    store, clock = Store(str(tmp_path / "healer.db")), _Charts()

    def add(group="S01 Slow Consumer", family="FLOW_EDIT"):
        incident = store.create_incident("p1", "crn:d", "crn:f", "S01", group, [], "", "", NOW - 600)
        store.update_incident(incident["id"], NOW, status="VERIFYING")
        action = store.add_action(incident["id"], "p1", "crn:d", "SET_CONCURRENT_TASKS", False, "APPLIED",
                                  family=family, started_at=NOW - 60, finished_at=NOW)

        return incident["id"], action

    return Poller(clock, store), store, clock, add, verdicts


def _at(poller, clock, t):
    clock.t = t

    return [(o["window_seconds"], o["verdict"]) for o in [*poller.evaluate_outcomes()]]


def test_an_unclear_first_window_waits_for_the_hour(verifying):
    poller, store, clock, add, verdicts = verifying
    incident_id, _ = add()
    verdicts += ["NO_CHANGE", "NO_CHANGE", "WORSENED"]

    assert _at(poller, clock, NOW + 1199) == []
    assert _at(poller, clock, NOW + 1200) == [(900, "NO_CHANGE")]
    found = store.incident(incident_id)
    assert found["status"] == "VERIFYING" and found["verdict"] == "NO_CHANGE"
    assert store.open_incident("crn:d", "S01 Slow Consumer")["id"] == incident_id    # no new incident meanwhile

    assert _at(poller, clock, NOW + 5400) == [(3600, "NO_CHANGE")]
    assert store.incident(incident_id)["status"] == "UNRESOLVED"

    # The 12 h window never changes the status; only a resolved incident regresses.
    assert _at(poller, clock, NOW + 46800) == [(43200, "WORSENED")]
    assert store.incident(incident_id)["status"] == "UNRESOLVED"
    assert not store.outcomes(incident_id)[-1]["details"].get("regression")
    assert clock.periods == ["LAST_ONE_HOUR", "LAST_TWELVE_HOURS", "LAST_ONE_DAY"]
    assert [o["window_seconds"] for o in store.outcomes(incident_id)] == [900, 3600, 43200]


def test_a_clear_first_window_closes_and_a_later_one_flags_a_regression(verifying):
    poller, store, clock, add, verdicts = verifying
    incident_id, _ = add()
    verdicts += ["IMPROVED", "WORSENED"]

    _at(poller, clock, NOW + 1200)
    assert store.incident(incident_id)["status"] == "RESOLVED"

    _at(poller, clock, NOW + 5400)
    found = store.incident(incident_id)
    assert found["status"] == "RESOLVED" and found["verdict"] == "IMPROVED"
    assert store.outcomes(incident_id)[-1]["details"]["regression"] is True


def test_a_later_change_cuts_a_window_short(verifying):
    poller, store, clock, add, verdicts = verifying
    first, _ = add()
    other, _ = add("S02 Slow Drain")
    store.update_action(2, started_at=NOW + 290, finished_at=NOW + 300)     # S02's fix, 5 min later
    store.add_changes("crn:d", "p1", [{"event_crn": "op/1", "t": NOW + 600, "kind": "parameters",
                                       "name": "Parameters Updated"},
                                      {"event_crn": "op/2", "t": NOW + 700, "kind": "kpis",
                                       "name": "Alert Rules Updated"}])
    verdicts += ["IMPROVED"] * 6

    # The agent's edit to another group does not cut S01's window; the operator's change does.
    _at(poller, clock, NOW + 1500)
    cut = store.outcomes(first)[0]
    assert cut["verdict"] == "INCONCLUSIVE" and cut["details"]["cut"]["source"] == "operator"
    assert "an operator's parameters change 10 min after" in cut["details"]["reason"]
    assert store.incident(first)["status"] == "VERIFYING"
    assert store.outcomes(other)[0]["details"]["cut"]["source"] == "operator"

    assert verdicts == ["IMPROVED"] * 6


def test_the_agents_resize_or_edit_to_the_same_group_cuts_a_window(verifying):
    poller, store, clock, add, verdicts = verifying
    first, _ = add()
    add("S03 Middle", family="SIZING")
    add()                                   # a second fix to S01's group
    store.update_action(2, started_at=NOW + 2000, finished_at=NOW + 2100)
    store.update_action(3, started_at=NOW + 1500, finished_at=NOW + 1600)
    verdicts += ["IMPROVED"] * 6

    assert _at(poller, clock, NOW + 1200) == [(900, "IMPROVED")]          # the second fix lands after it
    _at(poller, clock, NOW + 5400)
    hour = store.outcomes(first)[-1]
    assert hour["window_seconds"] == 3600 and hour["verdict"] == "INCONCLUSIVE"
    assert hour["details"]["cut"] == {"t": NOW + 1600, "source": "agent", "kind": "flow_version",
                                      "action_id": 3, "text": "the agent's SET_CONCURRENT_TASKS"}

    # The resize (action 2) cuts the second fix's window.
    assert store.outcomes(store.action(3)["incident_id"])[0]["details"]["cut"]["action_id"] == 2


def test_a_window_no_chart_reaches_any_more_is_missed(verifying):
    poller, store, clock, add, verdicts = verifying
    incident_id, _ = add()

    assert _at(poller, clock, NOW + 3 * 86400) == [(900, "INCONCLUSIVE"), (3600, "INCONCLUSIVE"),
                                                  (43200, "INCONCLUSIVE")]
    assert all(o["details"]["missed"] for o in store.outcomes(incident_id))
    assert store.incident(incident_id)["status"] == "UNRESOLVED" and clock.periods == []


def test_single_window_outcomes_migrate_once(tmp_path):
    path = str(tmp_path / "healer.db")
    old = sqlite3.connect(path)
    old.executescript("""
        CREATE TABLE outcomes (id INTEGER PRIMARY KEY AUTOINCREMENT, incident_id TEXT NOT NULL,
            action_id INTEGER NOT NULL UNIQUE, verdict TEXT NOT NULL, period TEXT, details_json TEXT,
            evaluated_at REAL NOT NULL);
        INSERT INTO outcomes (incident_id, action_id, verdict, period, details_json, evaluated_at)
            VALUES ('INC-1', 7, 'IMPROVED', 'LAST_ONE_HOUR', '{"window_seconds": 900}', 10);
    """)
    old.close()

    store = Store(path)
    assert [(o["action_id"], o["window_seconds"], o["verdict"]) for o in store.outcomes("INC-1")] == \
        [(7, 900, "IMPROVED")]
    backups = glob.glob(path + ".*.bak")
    assert len(backups) == 1
    assert sqlite3.connect(backups[0]).execute("SELECT count(*) FROM outcomes").fetchone() == (1,)

    store.add_outcome("INC-1", 7, "NO_CHANGE", "LAST_TWELVE_HOURS", {}, 20, window=3600)
    with pytest.raises(sqlite3.IntegrityError):
        store.add_outcome("INC-1", 7, "NO_CHANGE", "LAST_ONE_HOUR", {}, 20, window=900)

    Store(path)
    assert len(glob.glob(path + ".*.bak")) == 1
    assert [o["window_seconds"] for o in Store(path).outcomes("INC-1")] == [900, 3600]


def test_a_recheck_from_a_later_start_supersedes_the_verdict_and_restates_the_incident(verifying, monkeypatch):
    # Live: a right fix measured NO_CHANGE while another fault held the backlog up.
    poller, store, clock, add, verdicts = verifying
    incident_id, action = add()
    for window, verdict in ((900, "NO_CHANGE"), (3600, "NO_CHANGE"), (43200, "WORSENED")):
        store.add_outcome(incident_id, action, verdict, "", {"verdict": verdict}, NOW + 5400, window)
    closed = store.update_incident(incident_id, NOW + 5400, status="UNRESOLVED", verdict="NO_CHANGE")

    store.add_changes("crn:d", "p1", [{"event_crn": "op/1", "t": NOW + 600, "kind": "parameters",
                                       "name": "Parameters Updated"}])
    starts = []
    monkeypatch.setattr(outcomes, "evaluate", lambda kpis, series, started, start, *a, **k: (
        starts.append(start) or {"verdict": "IMPROVED", "kpis": [], "side_effects": []}))
    clock.t = NOW + 7000
    assert "closes" in poller.recheck(action, 3600, NOW + 2000, "alice")[1]     # at NOW + 7400
    outcome, why = poller.recheck(action, 3600, NOW + 700, "alice")

    assert why == "" and outcome["verdict"] == "IMPROVED" and starts == [NOW + 700]
    assert outcome["measured_from"] == NOW + 700 and outcome["finished_at"] == NOW
    assert outcome["recheck"] == {"from": NOW + 700, "by": "alice", "at": NOW + 7000, "was": "NO_CHANGE",
                                  "skipped": ["an operator's parameters change"]}
    assert clock.periods == ["LAST_TWELVE_HOURS"]

    found = {o["window_seconds"]: o for o in store.outcomes(incident_id)}
    assert found[3600]["verdict"] == "IMPROVED" and found[3600]["period"] == "LAST_TWELVE_HOURS"
    assert len(found) == 3
    old = store.superseded_outcomes(incident_id)
    assert [(o["window_seconds"], o["verdict"]) for o in old] == [(3600, "NO_CHANGE")]
    assert old[0]["outcome_id"] == found[3600]["id"]

    # The deciding windows now say IMPROVED; when it closed stays, and the 12 h WORSENED is a regression.
    restated = store.incident(incident_id)
    assert restated["status"] == "RESOLVED" and restated["verdict"] == "IMPROVED"
    assert restated["closed_at"] == closed["closed_at"]
    assert found[43200]["details"]["regression"] is True


def test_a_recheck_refuses_what_it_cannot_measure(verifying):
    poller, store, clock, add, verdicts = verifying
    incident_id, action = add()
    verdicts += ["IMPROVED"] * 3
    _at(poller, clock, NOW + 1200)

    clock.t = NOW + 1300
    assert "closes 7 min from now" in poller.recheck(action, 900, NOW + 500)[1]
    assert "no earlier than the change finished" in poller.recheck(action, 900, NOW - 10)[1]
    assert "not a verify window" in poller.recheck(action, 600)[1]

    store.add_changes("crn:d", "p1", [{"event_crn": "op/1", "t": NOW + 600, "kind": "parameters",
                                       "name": "Parameters Updated"}])
    clock.t = NOW + 2000
    assert "operator's parameters change 10 min after that start would cut" in poller.recheck(action, 900)[1]

    clock.t = NOW + 5000
    assert "No chart fine enough for the 15 min window" in poller.recheck(action, 900, NOW + 700)[1]

    dry = store.add_action(incident_id, "p1", "crn:d", "SET_CONCURRENT_TASKS", True, "DRY_RUN",
                           started_at=NOW, finished_at=NOW)
    assert "not applied" in poller.recheck(dry, 900)[1]

    # Nothing was recorded by any of them.
    assert store.superseded_outcomes(incident_id) == [] and verdicts == ["IMPROVED"] * 2
