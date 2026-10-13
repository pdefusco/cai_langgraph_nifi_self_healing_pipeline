"""
The Monitor tab: the change log (kpi/changes.py), the stored history it
is drawn from, and the timeline model and its HTML (ui/monitor.py).
"""

import re

import gradio as gr

import config
from nifi_healer import fleet
from nifi_healer.agent import graph
from nifi_healer.kpi import changes
from nifi_healer.runtime.poller import Poller
from nifi_healer.store.db import Store
from nifi_healer.ui import gradio_app, monitor

from test_agent_graph import S01_FIX, ScriptedLLM, agent, proposal  # noqa: F401 (agent is a fixture)


def _event(n, name, t, kind="STATUS_UPDATE"):
    return {"crn": f"ev/{n}", "name": name, "firstOccurrence": t * 1000, "eventType": kind}


def test_classify_keeps_only_finished_changes():
    assert changes.classify(_event(1, "Parameters Updated", 0)) == "parameters"
    assert changes.classify(_event(1, "Change Flow Version Completed", 0)) == "flow_version"
    assert changes.classify(_event(1, "Deployment Successful", 0)) == "created"
    assert changes.classify(_event(1, "Alert Rules Updated", 0)) == "kpis"
    assert changes.classify(_event(1, "Cluster Size Updated", 0)) == "sizing"
    assert changes.classify(_event(1, "Inbound Connection Updated", 0)) == "other"

    for name in ("Updating Parameters", "Change Flow Version Initiated", "NiFi Flow Started", ""):
        assert changes.classify(_event(1, name, 0)) is None

    assert changes.classify(_event(1, "Parameters Updated", 0, kind="ALERT")) is None

    found = changes.from_events([_event(1, "Parameters Updated", 100), _event(2, "Updating Parameters", 90),
                                 {"name": "Parameters Updated", "firstOccurrence": 5}])
    assert found == [{"event_crn": "ev/1", "t": 100.0, "kind": "parameters", "name": "Parameters Updated"}]


def test_attribute_tells_the_agent_from_an_operator():
    found = changes.from_events([_event(1, "Deployment Successful", 0), _event(2, "Parameters Updated", 1000),
                                 _event(3, "Parameters Updated", 5000), _event(4, "Alert Rules Updated", 6000)])
    actions = [
        {"id": 1, "status": "APPLIED", "dry_run": 0, "family": "PARAMETER", "started_at": 4900,
         "finished_at": 4950, "action_type": "UPDATE_PARAMETER"},
        {"id": 2, "status": "APPLIED", "dry_run": 0, "family": "SIZING", "started_at": 7000,
         "finished_at": 7100, "action_type": "SCALE_DEPLOYMENT"},     # no event: shown at its finish
        {"id": 3, "status": "APPLIED", "dry_run": 1, "family": "PARAMETER", "started_at": 990,
         "finished_at": 995},                                          # a dry run changed nothing
        {"id": 4, "status": "FAILED", "dry_run": 0, "family": "PARAMETER", "started_at": 990},
    ]

    out = changes.attribute(found, actions)

    assert [(c["t"], c["kind"], c["source"]) for c in out] == [
        (0, "created", "platform"), (1000, "parameters", "operator"), (5000, "parameters", "agent"),
        (6000, "kpis", "platform"), (7100, "sizing", "agent")]
    assert out[2]["action"]["id"] == 1 and out[4]["event_crn"] is None and out[1]["action"] is None


def test_history_grid_and_backfill_without_doubling(tmp_path):
    store = Store(str(tmp_path / "h.db"))
    store.add_samples("d", [("k", 1000, 10), ("k", 1075, 20), ("k", 1150, 30)])

    # A coarser chart over the same hour: only the times not already held are added.
    assert store.fill_samples("d", [("k", t, 99) for t in (700, 850, 1000, 1150)], 150) == 2
    assert store.first_sample_at("d") == 700 and store.first_sample_at("other") is None

    assert store.history("d", 700, 300) == {"k": {0: 99, 1: 20}}           # [700, 1000), [1000, 1300)
    assert store.history("d", 1000, 75) == {"k": {0: 10, 1: 20, 2: 30}}

    rows = [{"event_crn": "e1", "t": 900, "kind": "parameters", "name": "Parameters Updated"}]
    assert store.add_changes("d", "p", rows) == 1 and store.add_changes("d", "p", rows) == 0
    assert [c["kind"] for c in store.changes("d")] == ["parameters"] and store.changes("d", since=901) == []


def test_mock_events_and_poller(tenant):
    _, client, store = tenant("p5-parameters", history=3600)
    entry = fleet.pack("p5-parameters")
    poller = Poller(client, store)
    poller.poll_now()

    names = [e["name"] for e in client.deployment_events(entry["deployment_crn"])]
    assert "Deployment Successful" in names

    assert poller.poll_events("p5-parameters", entry) >= 1
    assert poller.poll_events("p5-parameters", entry) == 0
    kinds = [c["kind"] for c in store.changes(entry["deployment_crn"])]
    assert kinds[0] == "created"

    before = store.first_sample_at(entry["deployment_crn"])
    assert poller.backfill("p5-parameters", entry) > 0
    assert store.first_sample_at(entry["deployment_crn"]) < before


def _approved_s01(agent, *more):
    world, client, store, incidents, install = agent("p1-throughput", *more)
    incident = incidents["S01"]
    install(ScriptedLLM(proposal(S01_FIX)))
    graph.start(incident["id"])
    graph.resume(incident["id"], {"decision": "approve", "reviewer": "test"})

    entry = fleet.pack("p1-throughput")
    poller = Poller(client, store)
    world.skip(config.VERIFY_SETTLE_SECONDS + config.VERIFY_WINDOW_SECONDS + 120)
    poller.poll_now()
    poller.evaluate_outcomes()
    poller.poll_events("p1-throughput", entry)

    return client, store, entry, incident


def test_monitor_model_and_render(agent):
    client, store, entry, incident = _approved_s01(agent)
    crn = entry["deployment_crn"]
    now = client.now()

    # An operator's change after the agent's, with a name that must be escaped.
    store.add_changes(crn, "p1-throughput", [{"event_crn": "op/1", "t": now - 600, "kind": "other",
                                              "name": "<b>Proxy</b> ${x} `y` Updated"}])
    m = monitor.model(store, "p1-throughput", entry, now)
    segments = m["segments"]

    assert [s["letter"] for s in segments] == ["A", "B", "C"]
    assert segments[0]["short"] == "as provisioned" and segments[0]["source"] == ""
    agent_seg = segments[1]
    assert agent_seg["source"] == "agent" and agent_seg["kind"] == "flow_version"
    assert agent_seg["verdict"] == "IMPROVED" and agent_seg["short"].startswith("v2 · ")
    assert agent_seg["incident"] == incident["id"].split("-", 3)[-1]
    assert segments[2]["source"] == "operator" and segments[2]["end"] is None
    assert m["selected"] == 2 and m["step"] == 300 and m["n"] * m["step"] <= 43200 + 600

    choices = monitor.change_choices(m)
    assert [v for _, v in choices] == ["2", "1"] and choices[0][0].startswith("C · ")

    html = monitor.render(m)
    assert "No verdict: made outside the agent." in html
    assert "${" not in html and "`" not in html and "<b>Proxy" not in html
    assert html.count('data-pick="1"') == 2 and html.count('data-pick="2"') == 2   # rail marker + ledger column
    assert "Change ledger" in html and "S01" in html and "Deployment · system metrics" in html

    # Select the agent's change: its verdict and window instead.
    html = monitor.render(monitor.model(store, "p1-throughput", entry, now, selected="1"))
    assert "Change B · " in html and "mx-v-IMPROVED" in html
    assert '15 min <span class="mx-chip mx-v-IMPROVED">improved</span>' in html
    assert '1 h <span class="mx-chip mx-v-pending">pending · due ' in html and "12 h " in html
    assert [w["label"] for w in agent_seg["windows"]] == ["15 min", "1 h", "12 h"]
    assert agent_seg["label"] == "" and "reviewer:" not in html

    # A reviewer's call on the fix, beside the measured verdict.
    action = store.actions(incident_id=incident["id"])[-1]
    store.add_label(incident["id"], action["id"], "WRONG_FIX", "drained on its own", "ops", now)
    html = monitor.render(monitor.model(store, "p1-throughput", entry, now, selected="1"))
    assert html.count('<span class="mx-chip mx-v-WORSENED">reviewer: wrong fix</span>') == 2   # ledger + detail

    # Every bucket's value for the crosshair, one per bucket.
    values = re.search(r'class="mx-val" data-v="([^"]*)"', html).group(1).split("|")
    assert len(values) == m["n"]

    assert "${" not in monitor.JS and "`" not in monitor.JS
    assert ".mx " in monitor.CSS or ".mx{" in monitor.CSS


def test_ledger_levels_and_deltas():
    assert monitor.delta(100, 40, "down") == ("−60% better", "good")
    assert monitor.delta(100, 103, "down") == ("=", "")
    assert monitor.delta(0, 50, "down") == ("from 0 worse", "bad")
    assert monitor.delta(20.0, 35.5, "down", "%") == ("+15.5 pt", "bad")
    assert monitor.delta(60, 95, "up", "ratio") == ("+35 pt", "")
    assert monitor.delta(90, 95, "up", "ratio") == ("=", "")
    assert monitor.delta(None, 5, "down") == ("", "")
    assert monitor.fmt(None) == "—" and monitor.fmt(12345.6) == "12,346" and monitor.fmt(2.5) == "2.5"
    assert monitor.short(1500) == "1.5k" and monitor.short(25000) == "25k" and monitor.short(12) == "12"

    m = {"since": 0, "step": 100, "n": 10}
    values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    assert monitor._level(m, values, {"start": 0, "end": 500}) == 4          # buckets 2, 3, 4
    assert monitor._level(m, values, {"start": 500, "end": None}) == 9       # 8, 9, 10
    assert monitor._level(m, values, {"start": 520, "end": 560}) == 6        # inside one bucket


def test_fleet_states_carry_and_merge():
    assert [monitor.kpi_state({"above": 100}, v) for v in (None, 50, 80, 100, 101)] == [None, 0, 1, 1, 2]
    assert [monitor.kpi_state({"below": 10}, v) for v in (20, 12, 10, 9)] == [0, 1, 1, 2]
    assert monitor.kpi_state({"key": "k"}, 5) is None

    # Hourly buckets bridge a gap of two; the two newest (not yet polled) take the last value.
    assert monitor._carry({"step": 3600}, [1, None, 2, None, None, None, 3, None, None, None]) == \
        [1, 1, 2, None, None, None, 3, 3, 3, None]

    bars = [{"id": "S01", "start": 0, "end": 100, "state": "improved"},
            {"id": "S02", "start": 50, "end": 150, "state": "rejected"},
            {"id": "S03", "start": 200, "end": None, "state": "open"}]
    merged = monitor._merged(bars)
    assert [(g["label"], g["start"], g["end"], g["state"]) for g in merged] == [
        ("S01 +1", 0, 150, "nochange"), ("S03", 200, None, "open")]


def test_fleet_model_and_render(agent):
    client, store, entry, _ = _approved_s01(agent, "p5-parameters")
    packs = fleet.deployed_packs()
    now = client.now()
    store.add_changes(packs["p5-parameters"]["deployment_crn"], "p5-parameters",
                      [{"event_crn": "op/1", "t": now - 600, "kind": "parameters", "name": "<b>Params</b> ${x} `y`"}])

    f = monitor.fleet_model(store, packs, now, "12 h", selected="p1-throughput")
    rows = {r["pack"]: r for r in f["rows"]}
    assert f["total"] == 2 and set(rows) == {"p1-throughput", "p5-parameters"} and not f["filtered"]
    assert all(len(r["states"]) == f["n"] == len(r["texts"]) for r in f["rows"])

    p1, p5 = rows["p1-throughput"], rows["p5-parameters"]
    assert p1["last_change"]["source"] == "agent" and p1["last_change"]["letter"] == "B"
    assert p1["verdict"] == "IMPROVED" and p1["watched"] > 0
    assert p5["last_change"]["source"] == "operator" and p5["verdict"] == ""
    assert f["open"] == p1["open"] + p5["open"] and f["changes"] >= 2

    assert [r["pack"] for r in monitor.fleet_model(store, packs, now, sort="Recent change")["rows"]][0] == \
        "p5-parameters"
    assert [r["name"] for r in monitor.fleet_model(store, packs, now, sort="Name")["rows"]] == \
        sorted(r["name"] for r in f["rows"])

    only = monitor.fleet_model(store, packs, now, query="P5")
    assert [r["pack"] for r in only["rows"]] == ["p5-parameters"] and only["filtered"] and only["total"] == 2
    assert all(monitor.needs_attention(r) for r in monitor.fleet_model(store, packs, now, attention_only=True)["rows"])

    html = monitor.render_fleet(f)
    assert html.count('data-pack="p1-throughput"') == 1 and html.count("mx-sel") == 1
    assert "${" not in html and "`" not in html and "<b>Params" not in html
    assert 'class="mx-mk mx-agent" data-pick="1"' in html and "mx-mk mx-operator" in html
    assert all(len(v.split("|")) == f["n"] for v in re.findall(r'class="mx-val" data-v="([^"]*)"', html))

    assert "No deployment matches the filter." in monitor.render_fleet(
        monitor.fleet_model(store, packs, now, query="nothing-like-it"))
    assert "No deployments registered yet." in monitor.render_fleet(monitor.fleet_model(store, {}, now))


def test_monitor_handlers(agent):
    client, store, entry, _ = _approved_s01(agent)

    packs, html, picks = gradio_app.refresh_monitor(None, "12 h", None)
    assert packs["value"] == "p1-throughput" and 'class="mx-tl"' in html
    assert picks["value"] == "1"

    # A stale tick keeps the user's pick.
    assert "value" not in gradio_app.refresh_monitor("p1-throughput", "All", "1")[2]

    html, picks = gradio_app.pick_deployment("p1-throughput", "3 h")
    assert picks["value"] == "1" and "mx-tl" in html
    assert "Select a deployment." in gradio_app.show_monitor("nope", "12 h", None)

    event = gr.EventData(None, {"pick": "1"})
    assert gradio_app.pick_change(event)["value"] == "1"

    # The fleet: the shown deployment highlighted; a row or a marker opens it below.
    html = gradio_app.show_fleet("12 h", "", monitor.FLEET_SORTS[0], False, "p1-throughput")
    assert 'data-pack="p1-throughput"' in html and "mx-sel" in html

    pack, detail, picks = gradio_app.pick_fleet("12 h", gr.EventData(None, {"pack": "p1-throughput", "pick": "1"}))
    assert pack["value"] == "p1-throughput" and picks["value"] == "1" and "Change B · " in detail
    assert [v for _, v in picks["choices"]] == ["1"]
    pack, detail, picks = gradio_app.pick_fleet("3 h", gr.EventData(None, {"pack": "p1-throughput"}))
    assert picks["value"] == "1" and "mx-tl" in detail
    assert gradio_app.pick_fleet("12 h", gr.EventData(None, {"pack": "nope"})) == (gr.skip(),) * 3
