"""
The prompt budget (NFH-97): a group of more than SCOPE_PROCESSORS
processors is shown only around the fault (flow/inventory.scope), and its
readings with it. A size twin's scoped inventory is its small twin's,
renamed, with its decoy beside it, and a small group is shown whole.
"""

import pytest

from flows import packs
from flows.scenarios import scale
from nifi_healer.flow.inventory import SCOPE_PROCESSORS, inventory, render, scope
from nifi_healer.kpi import readings

WIDE = [s for pack in packs.PACKS.values() if pack.wave == 3 for s in pack.scenarios
        if s.twin and s.build.processors > SCOPE_PROCESSORS]


def _inventory(scenario) -> dict:
    return inventory(packs.build(packs.pack_of(scenario.id).name), scenario.group_name)


def _seeds(scenario) -> list:
    return [k.component for k in scenario.kpis]


def _names(inv: dict) -> tuple:
    return {p["name"] for p in inv["processors"]}, {c["name"] for c in inv["connections"]}


def test_a_wide_twin_is_scoped_to_its_small_twin_and_its_decoy():
    assert {s.id for s in WIDE} >= {"S46", "S47", "S48", "S49", "S50"}

    for scenario in WIDE:
        small = packs.SCENARIOS[scenario.twin]
        rename = scale.renamer(small.id, scenario.id)
        whole, scoped, few = _inventory(scenario), scope(_inventory(scenario), _seeds(scenario)), _inventory(small)
        processors, connections = _names(scoped)
        decoy = {n for n in processors if n.startswith(f"{scenario.id} {scale.DECOY} ")}

        assert processors - decoy == {rename(n) for n in _names(few)[0]}, scenario.id
        assert {c for c in connections if not c.startswith(scale._slug(scenario.id, scale.DECOY))} \
            == {rename(n) for n in _names(few)[1]}, scenario.id
        assert bool(decoy) == bool(scenario.build.decoy)
        assert scoped["dlqs"] == [rename(n) for n in few["dlqs"]]
        assert scoped["outside"]["processors"] == len(whole["processors"]) - len(processors)
        # Every flow of filler is left out, each one flow, a crossing through its ports too (NFH-96).
        assert scoped["outside"]["farther"] == 0
        assert scoped["outside"]["flows"] == len(scenario.build.shapes), scenario.id
        # A tenth of the text, or less.
        assert len(render(scoped)) < len(render(whole)) / 10 + len(render(few))


def test_the_inventory_says_what_it_leaves_out():
    scenario = packs.SCENARIOS["S49"]
    text = render(scope(_inventory(scenario), _seeds(scenario)))

    assert text.splitlines()[1] == ("Also in this group, not shown: 236 processor(s) and 213 connection(s), in 42 "
                                    "flow(s) not connected to the ones above.")
    assert "S49 Invoices" not in text and "S49 Exports Batch" in text


def test_a_small_group_is_shown_whole():
    for pack in packs.PACKS.values():
        definition = None

        for scenario in pack.scenarios:
            if scenario.twin and scenario.build.processors > SCOPE_PROCESSORS:
                continue

            definition = definition or packs.build(pack.name)
            inv = inventory(definition, scenario.group_name)

            if len(inv["processors"]) <= SCOPE_PROCESSORS:
                assert scope(inv, _seeds(scenario)) is inv, scenario.id


def _processor(name: str) -> dict:
    return {"name": name, "kind": "UpdateAttribute", "state": "ENABLED", "concurrent_tasks": 1, "run_schedule": "0 sec",
            "penalty": "30 sec", "yield": "1 sec", "retried": [], "relationships": ["success"], "routed": {},
            "auto_terminated": [], "properties": {}, "editable_properties": []}


def _link(name: str, source: str, destination: str) -> dict:
    return {"name": name, "source": source, "destination": destination, "relationships": ["success"],
            "object_threshold": 10000, "size_threshold": "1 GB", "expiration": "0 sec", "self_loop": False}


def _chain(n: int) -> dict:
    """A group of one chain of `n` processors, p0 to p{n-1}, and a DLQ nothing routes to."""

    processors = [_processor(f"p{i}") for i in range(n)] + [_processor("Reject DLQ")]
    connections = [_link(f"c{i}", f"p{i}", f"p{i + 1}") for i in range(n - 1)]

    return {"group": "G", "processors": processors, "connections": connections, "dlqs": ["Reject DLQ"],
            "parameters": []}


def test_one_long_flow_is_cut_hop_by_hop_within_the_budget():
    scoped = scope(_chain(100), ["c50"])
    kept = _names(scoped)[0]

    # c50's two ends, then 29 hops each way: 60 of the chain. The DLQ nothing routes to stays.
    assert kept == {f"p{i}" for i in range(21, 81)} | {"Reject DLQ"}
    assert scoped["outside"] == {"processors": 40, "connections": 40, "flows": 0, "farther": 2, "hops": 29}
    assert "more than 29 connection(s) from the components the incident's KPIs read" in render(scoped)


def test_a_dlq_does_not_join_two_flows():
    inv = _chain(70)
    inv["processors"] += [_processor("q0"), _processor("q1")]
    inv["connections"] += [_link("d0", "p0", "Reject DLQ"), _link("d1", "q1", "Reject DLQ"), _link("e0", "q0", "q1")]

    assert _names(scope(inv, ["q0"])) == ({"q0", "q1", "Reject DLQ"}, {"e0", "d1"})


def test_without_a_seed_in_the_group_it_is_shown_whole():
    inv = _chain(100)

    assert scope(inv, []) is inv and scope(inv, ["elsewhere"]) is inv


def test_the_seeds_are_the_incidents_own_kpis_and_none_for_the_nodes():
    rows = [{"component": "s49-ingest", "rollup": False}, {"component": "S49 Invoices Feed", "rollup": True},
            {"component": "", "rollup": False}]
    signals = [{"component": "s49-ingest"}]

    assert readings.seeds({"signals": signals}, rows) == ["s49-ingest", "s49-ingest"]
    assert readings.seeds({"signals": signals + [{"component": ""}]}, rows) == []


def _row(kind, component, metric, latest):
    return {"key": component, "kind": kind, "component": component, "metric": metric, "latest": latest,
            "above": None, "below": None, "state": 0, "trend": "flat", "per_minute": 0.0, "source": False,
            "rollup": False, "peak": None, "mean": None}


def test_the_readings_keep_only_what_the_inventory_shows():
    scoped = scope(_chain(100), ["c50"])
    rows = [_row("connection", "c50", "queued_count", 10), _row("processor", "p2", "flowfiles_out", 1.0),
            _row("processor", "p60", "flowfiles_out", 1.0), _row("system", "", "cpu", 40)]
    text = readings.render(rows, scoped)

    assert "c50 (p50 -> p51): 10 queued" in text and "p60: 1/s out" in text and "CPU" in text
    assert "p2:" not in text and "c1 " not in text
    assert text.count("\n") + 1 == len(scoped["connections"]) + 2


@pytest.mark.parametrize("name", ["S49", "S50"])
def test_a_wide_twins_prompt_is_about_its_small_twins(name):
    scenario = packs.SCENARIOS[name]
    small = packs.SCENARIOS[scenario.twin]

    assert len(render(scope(_inventory(scenario), _seeds(scenario)))) < 2.5 * len(render(_inventory(small)))
