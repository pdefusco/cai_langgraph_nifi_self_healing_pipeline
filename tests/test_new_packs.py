"""
The second-wave packs in the simulator: each builds and compiles, its
faults present as the detectors see them, the design fix clears each
one, and the wrong fixes it names do not. A compound fault clears only
once all its faults are fixed, and a masked one shows a new signal
after the first.
"""

import copy

import pytest

from flows import packs
from flows.builder.lib import check
from nifi_healer.flow import edits
from nifi_healer.kpi import detectors, metrics
from nifi_healer.sim.engine import Simulation

START = 1_800_000_000.0
SETTLE = 3600.0         # long enough for every second-wave fault to present
AFTER_FIX = 1800.0      # the longest clear_within among them
WAVE_2 = [name for name, pack in packs.PACKS.items() if pack.wave == 2]
SCOPES = {"connection": "CONNECTION", "processor": "PROCESSOR", "system": "SYSTEM"}
MASKED = {"S31", "S33"}   # the second fault shows only once the first is fixed


def _kpi(k) -> dict:
    return {"scenario": k.scenario, "key": k.key, "kind": k.kind, "component": k.component,
            "metric": k.metric, "above": k.above, "below": k.below,
            "tolerance_seconds": k.tolerance_seconds, "good": k.good, "primary": k.primary,
            "rollup": k.rollup, "chart": None}


def _charts(sim: Simulation, kpis: list) -> list:
    # The 30-minute charts the poller reads, as the mock client serves them.
    charts = []

    for k in kpis:
        key = f"{k['kind']}|{k['component']}|{k['metric']}"
        datas = sim.chart(key, "LAST_THIRTY_MINUTES", sim.t)
        assert datas, key       # a cleared fault must not be a missing chart
        charts.append({"componentType": SCOPES[k["kind"]], "componentName": k["component"],
                       "name": metrics.metric(k["metric"]).label, "metrics": {"datas": datas}})

    return charts


def _faulty(sim: Simulation, pack, scenario) -> list:
    """The scenario's critical signals, as one poll of this simulation finds them."""

    kpis = [_kpi(k) for k in pack.kpis]
    kinds = {p.name: p.kind for p in sim.model.processors.values()}
    found = detectors.detect(pack.name, {"kpis": kpis}, _charts(sim, kpis), [], sim.t, kinds)

    return [s["summary"] for s in found["signals"]
            if s["scenario"] == scenario.id and s["severity"] == detectors.CRITICAL]


@pytest.fixture(scope="module")
def settled():
    """{pack name: its simulation, an hour after deploy}, built once."""

    sims = {}

    for name in WAVE_2:
        pack = packs.PACKS[name]
        sim = Simulation(packs.build(name), dict(pack.parameters), pack.cluster_size, pack.nodes, START)
        sim.advance(START + SETTLE)
        sims[name] = sim

    return sims


def _apply(sim: Simulation, scenario, steps) -> None:
    """Each step as its family applies it: a parameter, the deployment's size, or a flow edit."""

    definition, parameters = sim.definition, dict(sim.parameters)
    size, nodes, flow = sim.cluster_size, sim.nodes, []

    for step in steps:
        if step["action"] == "UPDATE_PARAMETER":
            parameters[step["parameter"]] = step["value"]

        elif step["action"] == "SCALE_DEPLOYMENT":
            size, nodes = step["cluster_size"], step["nodes"]

        else:
            flow.append(dict(step))

    if flow:
        definition, changes = edits.apply(definition, scenario.group_name, flow)
        assert changes, flow

    sim.configure(definition, parameters, size, nodes)


def _after(settled, pack, scenario, steps) -> Simulation:
    sim = copy.deepcopy(settled[pack.name])
    _apply(sim, scenario, steps)
    sim.advance(sim.t + AFTER_FIX)

    return sim


def _scenarios(compound: bool) -> list:
    return [(name, s) for name in WAVE_2 for s in packs.PACKS[name].scenarios
            if (s.kind == "compound") == compound]


def _cases(field: str, compound: bool) -> list:
    return [pytest.param(name, s.id, i, id=f"{s.id}-{i}")
            for name, s in _scenarios(compound)
            for i in range(len(getattr(s, field)))]


def test_the_second_wave_is_not_provisioned_by_default():
    assert WAVE_2 and not set(WAVE_2) & set(packs.DEFAULT)

    for name in WAVE_2:
        for scenario in packs.PACKS[name].scenarios:
            assert scenario.kind != "classic" and scenario.wrong, scenario.id


@pytest.mark.parametrize("name", WAVE_2)
def test_each_pack_builds_and_compiles(name):
    definition = packs.build(name)

    pack = packs.PACKS[name]
    assert check(definition)["problems"] == []
    assert (pack.cluster_size, pack.nodes) == ("EXTRA_SMALL", 1)

    sim = Simulation(definition, dict(pack.parameters), pack.cluster_size, pack.nodes, START)
    groups = {p.group_name for p in sim.model.processors.values()}
    assert groups == {s.group_name for s in packs.PACKS[name].scenarios}

    for scenario in packs.PACKS[name].scenarios:
        assert 10 <= sum(p.group_name == scenario.group_name for p in sim.model.processors.values()) <= 30


@pytest.mark.parametrize("name", WAVE_2)
def test_each_fault_presents(settled, name):
    pack = packs.PACKS[name]

    for scenario in pack.scenarios:
        assert _faulty(settled[name], pack, scenario), scenario.id


@pytest.mark.parametrize("name, scenario_id", [pytest.param(n, s.id, id=s.id)
                                               for n, s in _scenarios(False)])
def test_the_design_fix_clears_the_fault(settled, name, scenario_id):
    pack, scenario = packs.PACKS[name], packs.SCENARIOS[scenario_id]
    sim = _after(settled, pack, scenario, scenario.expected)

    assert _faulty(sim, pack, scenario) == []


@pytest.mark.parametrize("name, scenario_id, index", _cases("acceptable", False))
def test_an_acceptable_fix_clears_it_alone(settled, name, scenario_id, index):
    pack, scenario = packs.PACKS[name], packs.SCENARIOS[scenario_id]
    sim = _after(settled, pack, scenario, [scenario.acceptable[index]])

    assert _faulty(sim, pack, scenario) == []


@pytest.mark.parametrize("name, scenario_id", [pytest.param(n, s.id, id=s.id)
                                               for n, s in _scenarios(True)])
def test_a_compound_fault_clears_once_all_its_faults_are_fixed(settled, name, scenario_id):
    pack, scenario = packs.PACKS[name], packs.SCENARIOS[scenario_id]

    assert len(scenario.faults) >= 2 and scenario.expected == scenario.faults[:1]
    assert scenario.faults[1] in scenario.acceptable
    assert _faulty(_after(settled, pack, scenario, scenario.faults), pack, scenario) == []


@pytest.mark.parametrize("name, scenario_id, index", _cases("faults", True))
def test_one_of_its_faults_fixed_alone_does_not(settled, name, scenario_id, index):
    pack, scenario = packs.PACKS[name], packs.SCENARIOS[scenario_id]
    left = _faulty(_after(settled, pack, scenario, [scenario.faults[index]]), pack, scenario)

    assert left, scenario.faults[index]

    if scenario.id in MASKED and index == 0:
        # What is left was not visible before: the masked fault, not the stall.
        assert not set(left) & set(_faulty(settled[name], pack, scenario)), left


@pytest.mark.parametrize("name, scenario_id, index", _cases("wrong", False) + _cases("wrong", True))
def test_a_wrong_fix_does_not(settled, name, scenario_id, index):
    pack, scenario = packs.PACKS[name], packs.SCENARIOS[scenario_id]
    sim = _after(settled, pack, scenario, [scenario.wrong[index]])

    assert _faulty(sim, pack, scenario), scenario.wrong[index]
