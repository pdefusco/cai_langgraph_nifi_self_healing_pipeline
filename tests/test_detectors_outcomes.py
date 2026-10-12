"""Detectors, the outcome verdicts and the incident lifecycle, on synthetic series."""

import pytest

import config
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


def test_too_few_buckets_is_inconclusive(windows):
    data, started, finished = around_change([1000] * 8, [900])
    result = outcomes.evaluate([kpi()], {kpi()["key"]: data}, started, finished)

    assert result["verdict"] == outcomes.INCONCLUSIVE


def test_period_for(windows):
    assert outcomes.period_for(NOW - 1200, NOW) == "LAST_ONE_HOUR"
    assert outcomes.period_for(NOW - 600, NOW) == "LAST_THIRTY_MINUTES"
    assert outcomes.period_for(NOW - 5 * 3600, NOW) == "LAST_TWELVE_HOURS"


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
