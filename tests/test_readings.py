"""Current readings: every KPI of an incident's group, for the diagnosis prompt (kpi/readings.py)."""

import pytest

import config
from nifi_healer import fleet
from nifi_healer.agent import graph
from nifi_healer.kpi import readings
from nifi_healer.store.db import Store

from test_agent_graph import S01_FIX, ScriptedLLM, agent, proposal  # noqa: F401 (agent is a fixture)

NOW = 100_000.0
S30 = {"scenario": "S30", "pack": "p12-warehouse", "deployment_crn": "d"}
KPIS = [
    {"key": "q", "scenario": "S30", "kind": "connection", "component": "s30-to-pick", "metric": "queued_count",
     "above": 1500},
    {"key": "ship", "scenario": "S30", "kind": "processor", "component": "S30 Ship Parcels",
     "metric": "flowfiles_in", "below": 15},
    {"key": "feed", "scenario": "S30", "kind": "processor", "component": "S30 Order Feed",
     "metric": "flowfiles_out"},
    {"key": "label", "scenario": "S30", "kind": "connection", "component": "s30-to-label",
     "metric": "queued_count", "above": 1500},
    {"key": "other", "scenario": "S31", "kind": "connection", "component": "s31-to-validate",
     "metric": "queued_count", "above": 1500},
]


def _store(tmp_path, monkeypatch, window: int):
    monkeypatch.setattr(config, "COUNT_WINDOW_SECONDS", window)
    monkeypatch.setattr(fleet, "pack", lambda name: {"kpis": KPIS} if name == "p12-warehouse" else {})
    store = Store(str(tmp_path / "healer.db"))
    minutes = range(0, 15)
    store.add_samples("d", [("q", NOW - 840 + 60 * m, 1000 + 80 * m) for m in minutes])
    store.add_samples("d", [("ship", NOW - 840 + 60 * m, 6.1 * window) for m in minutes])
    # The source sent 20/s an hour ago, and 6/s since back pressure throttled it.
    store.add_samples("d", [("feed", NOW - 3600, 20 * window)])
    store.add_samples("d", [("feed", NOW - 840 + 60 * m, 6 * window) for m in minutes])
    store.add_samples("d", [("other", NOW - 60, 9999)])

    return store


@pytest.mark.parametrize("window", [1, 300])     # the simulator's per-second counts; CDF's 5-minute totals
def test_readings_cover_every_kpi_of_the_group_per_second(tmp_path, monkeypatch, window):
    rows = readings.for_incident(S30, _store(tmp_path, monkeypatch, window), NOW)

    assert [r["key"] for r in rows] == ["q", "ship", "feed", "label"]      # S30's own, breached or not
    assert readings.render(rows).splitlines() == [
        "- connection s30-to-pick: 2120 queued (alert above 1500): over, rising",
        "- S30 Ship Parcels: 6.1/s in (alert below 15/s): over, flat",
        "- S30 Order Feed (a source): 6/s out, up to 20/s in the last 2 h: flat",
        "- connection s30-to-label: no data in the last 15 min",
    ]


def test_a_falling_queue_near_its_threshold(tmp_path, monkeypatch):
    store = _store(tmp_path, monkeypatch, 1)
    store.add_samples("d", [("label", NOW - 840 + 60 * m, 1500 - 20 * m) for m in range(15)])
    row = readings.for_incident(S30, store, NOW)[-1]

    assert (row["latest"], row["state"], row["trend"]) == (1220, 1, "falling")


def test_no_scenario_reads_nothing(tmp_path, monkeypatch):
    store = _store(tmp_path, monkeypatch, 1)

    assert readings.for_incident(dict(S30, scenario=""), store, NOW) == []


def test_the_prompt_carries_the_readings(agent):
    _, _, _, incidents, install = agent("p1-throughput")
    incident = incidents["S03"]
    model = install(ScriptedLLM(proposal(S01_FIX)))

    graph.start(incident["id"])
    prompt = model.calls[0][1].content
    section = prompt.split("Current readings", 1)[1].split("\n\n", 1)[0]

    # The two queues that are fine are there as well as the one that breached.
    for name in ("s03-to-enrich", "s03-to-transform", "s03-to-publish", "S03 Source (a source)"):
        assert name in section
    assert "s03-to-transform (S03 Enrich -> S03 Transform): " in section and "over" in section and "fine" in section
    assert section.index("s03-to-enrich") < section.index("s03-to-transform") < section.index("s03-to-publish")


# S30's group as flow/inventory.py has it, cut down: two of its queues measured, one not, one expiring.
INVENTORY = {
    "group": "S30", "processors": [{"name": n} for n in (
        "S30 Ship Parcels", "S30 Label", "S30 Order Feed", "S30 Pick Items")],
    "connections": [
        {"name": "s30-to-ship", "source": "S30 Label", "destination": "S30 Ship Parcels",
         "object_threshold": 10000, "size_threshold": "1 GB", "expiration": "0 sec"},
        {"name": "s30-to-label", "source": "S30 Pick Items", "destination": "S30 Label",
         "object_threshold": 2000, "size_threshold": "1 GB", "expiration": "5 min"},
        {"name": "s30-to-pick", "source": "S30 Order Feed", "destination": "S30 Pick Items",
         "object_threshold": 2500, "size_threshold": "1 GB", "expiration": "0 sec"},
        {"name": "s30-retry", "source": "S30 Pick Items", "destination": "S30 Pick Items",
         "object_threshold": 100, "size_threshold": "", "expiration": "0 sec"},
    ],
}


def test_with_the_inventory_every_queue_reads_in_flow_order(tmp_path, monkeypatch):
    rows = readings.for_incident(S30, _store(tmp_path, monkeypatch, 1), NOW)

    assert readings.render(rows, INVENTORY).splitlines() == [
        "- connection s30-to-pick (S30 Order Feed -> S30 Pick Items): 2120 queued, back pressure at 2500 (85% full)"
        " (alert above 1500): over, rising",
        "- connection s30-to-label (S30 Pick Items -> S30 Label): no data in the last 15 min;"
        " FlowFiles expire after 5 min",
        "- connection s30-retry (S30 Pick Items -> S30 Pick Items): not measured, back pressure at 100 objects",
        "- connection s30-to-ship (S30 Label -> S30 Ship Parcels): not measured,"
        " back pressure at 10000 objects / 1 GB",
        "- S30 Order Feed (a source): 6/s out, up to 20/s in the last 2 h: flat",
        "- S30 Ship Parcels: 6.1/s in (alert below 15/s): over, flat",
    ]

    # An inventory that could not be read leaves the rows as they are.
    assert readings.render(rows, {"error": "no group"}) == readings.render(rows)


def test_a_rate_names_the_fan_outs_and_merges_before_it():
    # S24 cut down: a merge sends one FlowFile per bin, which read against the rates before it looks slow.
    inv = {"processors": [{"name": n, "kind": k} for n, k in (
        ("Batches", "GenerateFlowFile"), ("Split", "SplitText"), ("Sessionize", "UpdateAttribute"),
        ("Merge", "MergeContent"), ("Publish", "UpdateAttribute"))],
        "connections": [{"name": f"{a}-{b}", "source": a, "destination": b, "object_threshold": 10000}
                        for a, b in (("Batches", "Split"), ("Split", "Sessionize"), ("Sessionize", "Merge"),
                                     ("Merge", "Publish"), ("Merge", "Merge"))]}
    rows = [{"key": c, "kind": "processor", "component": c, "metric": m, "latest": v, "above": None,
             "below": None, "state": None, "trend": "flat", "source": False, "peak": None, "mean": None}
            for c, m, v in (("Split", "flowfiles_out", 50), ("Sessionize", "flowfiles_out", 50),
                            ("Merge", "flowfiles_in", 50), ("Publish", "flowfiles_in", 0.1))]

    assert readings.render(rows, inv).splitlines()[-4:] == [
        "- Split: 50/s out, counted after a fan-out (Split): flat",
        "- Sessionize: 50/s out, counted after a fan-out (Split): flat",
        "- Merge: 50/s in, counted after a fan-out (Split): flat",
        "- Publish: 0.1/s in, counted after a fan-out (Split) and a merge (Merge): flat",
    ]
    assert readings.regrouped(inv, "Batches", "flowfiles_out") == []


def test_flow_order_follows_the_connections_from_the_sources():
    depth, connections = readings.flow_order(INVENTORY)

    assert depth == {"S30 Order Feed": 0, "S30 Pick Items": 1, "S30 Label": 2, "S30 Ship Parcels": 3}
    assert [c["name"] for c in connections] == ["s30-to-pick", "s30-to-label", "s30-retry", "s30-to-ship"]
