"""
What lets the app watch 20 deployments: the CLI worker pool, the App
health samples, and retention. The pool runs against a fake cdpcli
module, so nothing here reaches a tenant.
"""

import json
import os
import subprocess
import sys
import textwrap
import time

import pytest

import config
from nifi_healer.agent import graph
from nifi_healer.cdp import client as clients
from nifi_healer.runtime import telemetry
from nifi_healer.runtime.poller import Poller
from nifi_healer.ui import monitor, views

from test_agent_graph import S01_FIX, ScriptedLLM, agent, proposal  # noqa: F401 (agent is a fixture)

FAKE_CLIDRIVER = textwrap.dedent('''
    import json, os, sys, time

    CALLS = 0


    class CLIDriver:
        def main(self, args):
            global CALLS
            CALLS += 1
            op = args[1] if len(args) > 1 else ""

            if op == "describe-sleep":
                time.sleep(float(args[2]))

            if op == "describe-die":
                os._exit(3)

            if op == "describe-fail":
                sys.stderr.write("An error occurred: nope (Status Code: 404)\\n")
                return 254

            if op == "describe-usage":
                raise SystemExit(2)

            if op == "describe-noisy":
                os.write(1, b"not a reply\\n")

            print(json.dumps({"pid": os.getpid(), "calls": CALLS, "args": args,
                              "keyed": os.environ.get("CDP_ACCESS_KEY_ID", "")}))
            return 0


    def main():
        return CLIDriver().main(sys.argv[1:])
''')


@pytest.fixture
def fake_cdp(tmp_path, monkeypatch):
    """An environment whose cdpcli is the fake above."""

    package = tmp_path / "fake" / "cdpcli"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "clidriver.py").write_text(FAKE_CLIDRIVER)
    environment = {"PATH": os.environ.get("PATH", ""), "PYTHONPATH": str(tmp_path / "fake"),
                   "CDP_ACCESS_KEY_ID": "test-key-id", "CDP_PRIVATE_KEY": "test-private"}
    monkeypatch.setattr(clients.LiveClient, "_environment", lambda self: dict(environment))
    telemetry.drain()

    return environment


def _reply(completed) -> dict:
    return json.loads(completed.stdout)


def test_a_worker_answers_many_reads(fake_cdp):
    pool = clients.WorkerPool(2)

    try:
        first = _reply(pool.run(["df", "describe-deployment", "--x", "1"], fake_cdp, 30))
        second = _reply(pool.run(["df", "list-deployments"], fake_cdp, 30))
        assert first["pid"] == second["pid"] and second["calls"] == 2
        assert first["args"] == ["df", "describe-deployment", "--x", "1"]
        assert first["keyed"] == "test-key-id"

        failed = pool.run(["df", "describe-fail"], fake_cdp, 30)
        assert failed.returncode == 254 and "nope" in failed.stderr
        assert pool.run(["df", "describe-usage"], fake_cdp, 30).returncode == 2

        # cdpcli writing to the real stdout cannot corrupt a reply.
        assert _reply(pool.run(["df", "describe-noisy"], fake_cdp, 30))["pid"] == first["pid"]

    finally:
        pool.close()


def test_a_worker_is_recycled_replaced_and_restarted(fake_cdp, monkeypatch):
    monkeypatch.setattr(clients, "WORKER_CALLS", 2)
    pool = clients.WorkerPool(1)

    try:
        pids = [_reply(pool.run(["df", "describe-x"], fake_cdp, 30))["pid"] for _ in range(3)]
        assert pids[0] == pids[1] != pids[2]

        with pytest.raises(subprocess.TimeoutExpired):
            pool.run(["df", "describe-sleep", "5"], fake_cdp, 0.5)

        assert pool._alive == 0
        after = _reply(pool.run(["df", "describe-x"], fake_cdp, 30))["pid"]
        assert after not in pids

        # New credentials: the worker started with the old ones is not reused.
        rotated = dict(fake_cdp, CDP_ACCESS_KEY_ID="rotated-key-id")
        reply = _reply(pool.run(["df", "describe-x"], rotated, 30))
        assert reply["pid"] != after and reply["keyed"] == "rotated-key-id"
        assert pool._alive == 1

        with pytest.raises(clients._WorkerDied):
            pool.run(["df", "describe-die"], fake_cdp, 30)

        assert pool._alive == 0

    finally:
        pool.close()


def test_live_client_reads_on_the_pool_and_writes_alone(fake_cdp, monkeypatch):
    monkeypatch.setattr(config, "CDP_WORKERS", 2)
    client = clients.LiveClient()

    try:
        reads = [client.run(["df", "describe-deployment", "--deployment-crn", "c"]) for _ in range(2)]
        assert reads[0]["pid"] == reads[1]["pid"]
        assert reads[0]["args"][-2:] == ["--output", "json"]

        writes = [client.run(["df", "update-deployment", "--deployment-crn", "c"]) for _ in range(2)]
        assert len({w["pid"] for w in writes} | {reads[0]["pid"]}) == 3
        assert client.pool._alive == 1

        failed = client.run(["df", "describe-fail"])
        assert failed["error"] == "The CDP CLI returned an error." and "nope" in failed["stderr"]

        # A worker that dies twice: the call runs on its own and reports what it died of.
        died = client.run(["df", "describe-die"])
        assert "error" in died

        monkeypatch.setattr(config, "CDP_TIMEOUT_SECONDS", 1)
        slow = client.run(["df", "describe-sleep", "5"])
        assert "did not complete within 1 seconds" in slow["error"]

        # Durations and failures only: nothing of a call's arguments or credentials.
        drained = telemetry.drain()
        assert drained["cdp"]["calls"] == 7 and drained["cdp"]["errors"] == 3
        assert "deployment-crn" not in repr(drained) and "test-key-id" not in repr(drained)

    finally:
        client.pool.close()


def test_no_workers_runs_every_call_alone(fake_cdp, monkeypatch):
    monkeypatch.setattr(config, "CDP_WORKERS", 0)
    client = clients.LiveClient()
    assert client.pool is None

    pids = {client.run(["df", "describe-deployment"])["pid"] for _ in range(2)}
    assert len(pids) == 2


def test_the_worker_rejects_a_malformed_request():
    worker = subprocess.Popen([sys.executable, clients.WORKER_SCRIPT], stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
                              env={"PATH": os.environ.get("PATH", ""), "PYTHONPATH": ""})

    try:
        line = worker.stdout.readline()

        if not json.loads(line).get("ready"):
            pytest.skip("no cdpcli in this environment")

        worker.stdin.write("[1, 2]\n")
        worker.stdin.flush()
        assert json.loads(worker.stdout.readline())["rc"] == 2

    finally:
        worker.kill()
        worker.wait()


def test_percentiles_and_summaries():
    assert telemetry.percentile([], 95) is None
    assert telemetry.percentile([3, 1, 2], 50) == 2
    assert telemetry.percentile(list(range(1, 101)), 95) == 95
    assert telemetry.summary([(0.1, True), (0.3, False)]) == {"calls": 2, "errors": 1, "p50_ms": 100,
                                                               "p95_ms": 300}

    first = telemetry.process_stats()
    assert first["rss.mb"] > 0
    assert "cpu.pct" in telemetry.process_stats()


def test_poll_workers_grow_with_the_fleet(monkeypatch):
    monkeypatch.setattr(config, "POLL_WORKERS", 0)
    assert [config.poll_workers(n) for n in (0, 7, 8, 9, 20)] == [4, 4, 4, 5, 10]

    monkeypatch.setattr(config, "POLL_WORKERS", 3)
    assert config.poll_workers(20) == 3


def test_app_health_is_sampled_with_lag(tenant, monkeypatch):
    monkeypatch.setattr(config, "POLL_WORKERS", 0)
    _, client, store = tenant("p1-throughput", "p5-parameters")
    poller = Poller(client, store)
    poller.poll_now()
    poller._packs()

    values = poller.sample_health()
    assert values["poll.lag.kpis"] == 0 and values["poll.lag.health"] == 0
    assert values["cdp.calls"] > 0 and values["cdp.errors"] == 0
    assert values["poll.workers"] == 4 and values["agent.queue"] == 0
    assert values["db.mb"] > 0 and values["rss.mb"] > 0

    later = time.time() + config.POLL_KPI_SECONDS + 100
    assert poller.lag(later)["poll.lag.kpis.max"] == pytest.approx(100, abs=2)

    latest = store.latest_health()
    assert latest["cdp.calls"][1] == values["cdp.calls"]
    assert [v for _, v in store.health("poll.workers")] == [4]
    assert poller.snapshot()["heartbeat"]["health"]["poll.workers"] == 4

    # Queue waits are measured from submission.
    poller._submit(("kpis", "p1-throughput"), lambda: None)
    poller._pool.shutdown(wait=True)
    assert "poll.wait.kpis" in poller.sample_health()


def test_reads_running_late_warn_after_ten_minutes(tenant, monkeypatch):
    _, client, store = tenant("p1-throughput")
    poller = Poller(client, store)

    # A quiet minute is recorded as zero calls, not left out of the chart.
    values = poller.sample_health()
    assert values["llm.calls"] == 0 and values["llm.errors"] == 0 and "cdp.calls" in values
    assert poller.snapshot()["heartbeat"]["kpis_late_since"] is None

    late = config.POLL_KPI_SECONDS + 5
    monkeypatch.setattr(poller, "lag", lambda now=None: {"poll.lag.kpis": late})
    poller.sample_health()
    since = poller.snapshot()["heartbeat"]["kpis_late_since"]
    poller.sample_health()
    beat = poller.snapshot()["heartbeat"]
    assert beat["kpis_late_since"] == since

    assert views.falling_behind(beat) == "" and views.falling_behind(beat, now=since + 599) == ""
    warning = views.falling_behind(beat, now=since + 660)
    assert "falling behind" in warning and f"{late}s past" in warning and "for 11 min" in warning

    monkeypatch.setattr(poller, "lag", lambda now=None: {"poll.lag.kpis": 1})
    poller.sample_health()
    assert poller.snapshot()["heartbeat"]["kpis_late_since"] is None

    poller._error("kpis p1-throughput: boom")
    poller._error("kpis p1-throughput: boom")
    snapshot = poller.snapshot()
    assert snapshot["heartbeat"]["errors_hour"] == 2
    snapshot = dict(snapshot, heartbeat=dict(snapshot["heartbeat"], kpis_late_since=time.time() - 700))
    header = views.fleet_header(snapshot)
    assert header.startswith("⚠ **The poller is falling behind") and "(2 error(s) in the last hour)" in header


def test_app_health_charts_and_gates(tenant):
    _, client, store = tenant("p1-throughput")
    now = time.time()

    assert "No App health samples yet" in monitor.render_health(monitor.health_model(store, now))

    for minute in range(60):
        store.add_health(now - 3600 + 60 * minute, {"poll.lag.kpis": 70 if minute < 5 else 2, "cdp.calls": 50,
                                                    "cdp.errors": 1 if minute < 30 else 0, "cpu.pct": 20})

    h = monitor.health_model(store, now)
    assert h["samples"] == 60 and h["n"] == 86400 // monitor.HEALTH_STEP
    gates = {label: value for label, value, _, _ in h["gates"]}
    assert gates["KPI reads late, p95"] == 70 and gates["CDP calls failed"] == pytest.approx(1.0)
    assert gates["Container CPU, p95"] == 20

    rows = {row["key"]: row for row in h["rows"]}
    calls = [v for v in rows["cdp.calls"]["values"] if v is not None]
    assert calls and set(calls) == {50}  # the mean minute, not the bucket's total
    assert rows["poll.lag.kpis"]["peak"] == 70 and rows["poll.lag.kpis"]["latest"] == 2
    assert rows["llm.calls"]["latest"] is None

    html = monitor.render_health(h)
    assert html.count("mx-v-WORSENED") == 2 and html.count("mx-v-IMPROVED") == 1
    assert 'class="mx-tl"' in html and "poll.lag.kpis" in html


def test_retention_prunes_samples_and_closed_checkpoints(agent, monkeypatch):
    world, client, store, incidents, install = agent("p1-throughput")
    install(ScriptedLLM(proposal(S01_FIX)))
    incident = incidents["S01"]
    graph.start(incident["id"])
    monkeypatch.setattr(config, "CHECKPOINT_KEEP_DAYS", 0)

    crn = incident["deployment_crn"]
    old = client.now() - (config.KEEP_DAYS + 1) * 86400
    store.add_samples(crn, [("old-kpi", old + i, 1.0) for i in range(12)])
    store.add_samples(crn, [("new-kpi", client.now(), 1.0)])
    store.add_health(time.time() - (config.HEALTH_KEEP_DAYS + 1) * 86400, {"cdp.calls": 1})
    store.add_health(time.time(), {"cdp.calls": 2})
    assert store.prune_samples(old + 7, batch=5) == 7

    # An open incident keeps its paused run.
    poller = Poller(client, store)
    assert poller.retain() == {"kpi_samples": 5, "health_samples": 1, "checkpoints": 0}
    assert graph.pending(incident["id"])
    assert store.samples(crn, "old-kpi") == [] and len(store.samples(crn, "new-kpi")) == 1
    assert [v for _, v in store.health("cdp.calls")] == [2]

    store.update_incident(incident["id"], client.now(), status="REJECTED")
    world.skip(60)
    assert poller.retain()["checkpoints"] == 1
    assert not graph.state(incident["id"]) and not graph.pending(incident["id"])

    # Each closed incident is forgotten once.
    world.skip(60)
    assert poller.retain() == {"kpi_samples": 0, "health_samples": 0, "checkpoints": 0}


def test_mock_events_keep_the_newest(tenant, monkeypatch):
    from nifi_healer.sim import world as worlds

    monkeypatch.setattr(worlds, "MAX_EVENTS", 3)
    deployment = {"crn": "d"}

    for n in range(5):
        worlds.World._event(deployment, f"e{n}", n)

    assert [e["name"] for e in deployment["events"]] == ["e2", "e3", "e4"]
    assert deployment["events"][-1]["crn"] == "d/event/5"
