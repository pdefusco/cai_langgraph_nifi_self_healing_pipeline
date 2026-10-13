"""A process group's in and out: its edges, the chart-only KPIs they need, and the sums."""

import pytest

from flows import packs
from flows.scenarios.base import Kpi
from nifi_healer import fleet
from nifi_healer.kpi import outcomes, rollup
from nifi_healer.runtime.poller import Poller

from test_detectors_outcomes import around_change, kpi, windows  # noqa: F401 (windows is a fixture)


def test_edges_are_inferred_from_the_topology():
    assert packs.SCENARIOS["S01"].flow_edges() == {"in": ["S01 Source"], "out": ["S01 Consumer"]}

    # A failure loop is not a way out; a DLQ nothing is routed to yet is.
    assert packs.SCENARIOS["S10"].flow_edges() == {"in": ["S10 Source"],
                                                   "out": ["S10 Partner Call", "S10 DLQ"]}

    # Declared where the topology misleads: S17's stub and receiver form a second chain.
    assert packs.SCENARIOS["S17"].flow_edges() == {"in": ["S17 Source"], "out": ["S17 Received"]}


def test_edge_kpis_are_chart_only_and_not_duplicated():
    s13 = packs.SCENARIOS["S13"]
    assert s13.edge_kpis() == ()     # its own KPIs already chart both edges

    added = packs.SCENARIOS["S01"].edge_kpis()
    assert [k.key for k in added] == ["S01:processor:S01 Source:flowfiles_out"]
    assert all(k.rollup and not k.alert and not k.primary for k in added)

    keys = [k.key for p in packs.PACKS.values() for k in p.kpis]
    assert len(keys) == len(set(keys))

    with pytest.raises(ValueError, match="roll-up"):
        Kpi("S01", "processor", "S01 Source", "flowfiles_out", alert=True, above=1, rollup=True)


def test_sums_use_only_the_buckets_every_edge_has():
    assert rollup.summed([[(1, 2.0), (2, 3.0)], [(2, 4.0), (3, 5.0)]]) == [(2, 7.0)]
    assert rollup.summed([[(1, 2.0)], []]) == []

    source = kpi("S10", "processor", "S10 Source", "flowfiles_out", above=None, good="up")
    call = kpi("S10", "processor", "S10 Partner Call", "flowfiles_in", above=None, good="up")
    dlq = kpi("S10", "processor", "S10 DLQ", "flowfiles_in", above=None, good="up")
    series = {source["key"]: [(1, 10.0), (2, 10.0)], call["key"]: [(1, 6.0), (2, 7.0)],
              dlq["key"]: [(1, 2.0), (2, 3.0)]}

    flow = rollup.group_flow([source, call, dlq], series)
    assert flow["in"] == [(1, 10.0), (2, 10.0)] and flow["out"] == [(1, 8.0), (2, 10.0)]
    assert flow["edges"] == {"in": ["S10 Source"], "out": ["S10 Partner Call", "S10 DLQ"]}
    assert rollup.ratio(flow) == pytest.approx(0.9)

    # Without the DLQ's chart the out side would be part of the group: none rather than wrong.
    flow = rollup.group_flow([source, call], series)
    assert flow["in"] and flow["out"] == [] and rollup.ratio(flow) is None


def test_rollup_kpis_never_signal_or_decide(tenant, windows):
    _, client, store = tenant("p1-throughput", "p3-errors", "p4-dataloss-latency", history=3600)
    poller = Poller(client, store)
    poller.poll_now()
    snapshot = poller.snapshot()

    for name, view in snapshot["packs"].items():
        rollups = {k["key"] for k in fleet.pack(name)["kpis"] if k.get("rollup")}
        assert rollups and rollups <= set(view["kpis"])
        assert all(view["kpis"][key]["rollup"] for key in rollups)
        assert not {s.get("kpi") for s in view["signals"]} & rollups
        assert any(flow["in"] and flow["out"] for flow in view["groups"].values())

    source = kpi("S01", "processor", "S01 Source", "flowfiles_out", above=None, good="up", primary=False)
    source["rollup"] = True
    consumer = kpi("S01", "processor", "S01 Consumer", "flowfiles_in", above=None, good="up", primary=False)
    queue = kpi()
    q_data, started, finished = around_change([2000] * 8, [400] * 8)
    in_data, _, _ = around_change([3.0] * 8, [3.0] * 8)
    out_data, _, _ = around_change([1.0] * 8, [3.0] * 8)
    series = {queue["key"]: q_data, source["key"]: in_data, consumer["key"]: out_data}

    result = outcomes.evaluate([queue, consumer, source], series, started, finished)
    assert result["verdict"] == outcomes.IMPROVED
    assert source["key"] not in [k["kpi"] for k in result["kpis"]]
    assert result["group"]["edges"] == {"in": ["S01 Source"], "out": ["S01 Consumer"]}
    assert result["group"]["ratio_before"] == pytest.approx(1 / 3)
    assert result["group"]["ratio_after"] == pytest.approx(1.0)

    assert outcomes.evaluate([queue], {queue["key"]: q_data}, started, finished)["group"] == {}
