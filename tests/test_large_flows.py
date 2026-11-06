"""
The customer-sized packs (NFH-95) in the simulator. The filler is
deterministic and healthy, and never touches the fault, so a size twin
is its twin with more flow around it: the same readings, shown at the
same poll, and the same verdict on its design fix, its acceptable fixes
and its wrong ones. A decoy beside it adds a KPI, a trap and a wrong fix,
which must leave the fault in place too.

A control is filler alone, and stays quiet: each pack with controls is
deployed at midnight UTC and polled every SCAN over their detect_within,
the poll reading every KPI of the pack at once, as the poller does.
"""

import copy

import pytest

from flows import packs
from flows.build_all import check_pack
from flows.builder.lib import ProcessGroup, check, iter_processors
from flows.scenarios import scale
from flows.scenarios.base import action
from nifi_healer.kpi import detectors
from nifi_healer.sim.engine import Simulation
from test_new_packs import MIDNIGHT, SCAN, START, _apply, _charts, _faulty, _kpi, _polls

WAVE_3 = [name for name, pack in packs.PACKS.items() if pack.wave == 3]
TWINS = [s for name in WAVE_3 for s in packs.PACKS[name].scenarios if s.twin]
DECOYED = [s for s in TWINS if s.build.decoy]
CONTROLS = [s for name in WAVE_3 for s in packs.PACKS[name].scenarios if s.kind == "control"]
WITH_CONTROLS = [name for name in WAVE_3 if any(s.kind == "control" for s in packs.PACKS[name].scenarios)]


def _group(scenario) -> ProcessGroup:
    group = ProcessGroup(scenario.id, scenario.group_name)
    scenario.build(group)

    return group


def _verdicts(scenario) -> dict:
    """
    {"shown": seconds from deploy to the first poll that finds it faulty,
    then for each fix, its own and its acceptable and wrong ones, what is
    still critical a clear_within after it is applied there}.
    """

    pack = packs.pack_of(scenario.id)
    sim = Simulation(packs.build(pack.name), dict(pack.parameters), pack.cluster_size, pack.nodes, START)
    found = {"shown": _polls(sim, pack, scenario, scenario.detect_within, until_faulty=True)[-1][0] - START}
    fixes = {"expected": scenario.expected}
    fixes.update({f"acceptable[{i}]": (a,) for i, a in enumerate(scenario.acceptable)})
    fixes.update({f"wrong[{i}]": (a,) for i, a in enumerate(scenario.wrong)})

    for label, steps in fixes.items():
        after = copy.deepcopy(sim)
        _apply(after, scenario, steps)
        after.advance(after.t + scenario.clear_within)
        found[label] = _faulty(after, pack, scenario)

    return found


def test_the_third_wave_is_not_provisioned_by_default():
    assert WAVE_3 and not set(WAVE_3) & set(packs.DEFAULT)


@pytest.mark.parametrize("name", WAVE_3)
def test_each_pack_builds_and_checks(name):
    definition = packs.build(name)

    assert check(definition)["problems"] == []
    assert check_pack(packs.PACKS[name], definition) == []
    assert packs.build(name) == definition      # the filler is deterministic


@pytest.mark.parametrize("scenario", TWINS, ids=lambda s: s.id)
def test_a_twin_is_its_twins_fault_renamed(scenario):
    small = packs.SCENARIOS[scenario.twin]
    rename = scale.renamer(small.id, scenario.id)

    # A decoy adds one of each after the twin's own.
    extra = 1 if scenario.build.decoy else 0

    assert (scenario.family, scenario.kind, scenario.fault) == (small.family, small.kind, small.fault)
    assert scenario.expected == rename(small.expected) and scenario.acceptable == rename(small.acceptable)
    assert (scenario.traps, scenario.wrong) == (rename(small.traps) + scenario.traps[len(small.traps):],
                                                rename(small.wrong) + scenario.wrong[len(small.wrong):])
    assert (len(scenario.traps) - len(small.traps), len(scenario.wrong) - len(small.wrong)) == (extra, extra)
    assert [(k.component, k.metric, k.above) for k in scenario.kpis[:len(small.kpis)]] == \
        [(rename(k.component), k.metric, k.above) for k in small.kpis]
    assert len(scenario.kpis) == len(small.kpis) + extra


@pytest.mark.parametrize("scenario", TWINS, ids=lambda s: s.id)
def test_the_filler_fills_the_group_and_never_touches_the_fault(scenario):
    group, core = _group(scenario), ProcessGroup(scenario.id, scenario.group_name)
    scenario.build.core(core)
    names = {p["name"] for p in core.processors.values()}
    target = scenario.build.processors

    assert 0.95 * target <= len(group.processors) <= target
    assert scenario.build.shapes and (target < 50 or len(set(scenario.build.shapes)) == len(scale.SHAPES))

    for c in group.connections:
        assert (c["source"]["name"] in names) == (c["destination"]["name"] in names), c["name"]


@pytest.mark.parametrize("name", WAVE_3)
def test_only_the_faults_dlqs_raise_bulletins(name):
    # A WARN bulletin is Concerning Health in CDF: the filler's DLQs, which
    # reject one in 20 by design, log at INFO; a twin's own DLQs at WARN, as its twin's do (NFH-98).
    faults = set()

    for scenario in packs.PACKS[name].scenarios:
        if scenario.twin:
            core = ProcessGroup(scenario.id, scenario.group_name)
            scenario.build.core(core)
            faults |= {p["name"] for p in core.processors.values()}

    levels = {p["name"]: p["properties"]["Log Level"] for _, _, p in iter_processors(packs.build(name))
              if p["type"].endswith(".LogAttribute")}

    assert any(level == "info" for level in levels.values())
    assert levels == {n: "warn" if n in faults else "info" for n in levels}


@pytest.mark.parametrize("scenario", TWINS, ids=lambda s: s.id)
def test_a_twin_presents_and_clears_as_its_twin_does(scenario):
    small = packs.SCENARIOS[scenario.twin]
    rename = scale.renamer(small.id, scenario.id)
    found, expected = _verdicts(scenario), _verdicts(small)
    decoy = {label: found.pop(label) for label in set(found) - set(expected)}

    assert found["expected"] == []
    assert found == {label: rename(value) for label, value in expected.items()}
    assert len(decoy) == (1 if scenario.build.decoy else 0) and all(decoy.values()), decoy


@pytest.mark.parametrize("scenario", DECOYED, ids=lambda s: s.id)
def test_a_decoy_is_charted_and_healthy(scenario):
    pack = packs.pack_of(scenario.id)
    sim = Simulation(packs.build(pack.name), dict(pack.parameters), pack.cluster_size, pack.nodes, START)
    decoy = scenario.kpis[-1]
    seen = []

    while sim.t < START + 3600:
        sim.advance(sim.t + 60)
        seen.append(sim.chart(f"{decoy.kind}|{decoy.component}|{decoy.metric}", "LAST_THIRTY_MINUTES",
                              sim.t)[-1]["value"])

    assert not decoy.alert and not decoy.primary and decoy.component in scenario.traps[-1]
    # A batch climbs as a backlog would, and empties; a retry loop stays near empty.
    assert {"batch": max(seen) > 4000 and min(seen[-20:]) < 1000, "retry": max(seen) < 50}[scenario.build.decoy], seen


@pytest.fixture(scope="module")
def polled():
    """
    {pack name: {scenario id: the critical summaries of each poll}}, for
    each pack with controls, deployed at midnight UTC and polled every
    SCAN over its controls' detect_within, every KPI of the pack at once.
    """

    found = {}

    for name in WITH_CONTROLS:
        pack = packs.PACKS[name]
        sim = Simulation(packs.build(name), dict(pack.parameters), pack.cluster_size, pack.nodes, MIDNIGHT)
        kpis = [_kpi(k) for k in pack.kpis]
        kinds = {p.name: p.kind for p in sim.model.processors.values()}
        end = MIDNIGHT + max(s.detect_within for s in pack.scenarios if s.kind == "control")
        polls = found[name] = {s.id: [] for s in pack.scenarios}

        while sim.t < end:
            sim.advance(sim.t + SCAN)
            signals = detectors.detect(pack.name, {"kpis": kpis}, _charts(sim, kpis), [], sim.t, kinds)["signals"]

            for scenario_id, summaries in polls.items():
                summaries.append([s["summary"] for s in signals
                                  if s["scenario"] == scenario_id and s["severity"] == detectors.CRITICAL])

    return found


@pytest.mark.parametrize("scenario", CONTROLS, ids=lambda s: s.id)
def test_a_control_stays_quiet_however_large_the_pack(polled, scenario):
    polls = polled[packs.pack_of(scenario.id).name][scenario.id]

    assert len(polls) * SCAN >= scenario.detect_within
    assert [found for found in polls if found] == []
    assert scenario.expected == (action("NO_ACTION"),) and not scenario.wrong


@pytest.mark.parametrize("name", WITH_CONTROLS)
def test_each_fault_among_controls_shows_in_its_own_group(polled, name):
    for scenario in packs.PACKS[name].scenarios:
        if scenario.kind != "control":
            assert any(polled[name][scenario.id]), scenario.id


@pytest.mark.parametrize("name", WAVE_3)
def test_the_kpis_grow_with_the_flow_not_faster(name):
    pack = packs.PACKS[name]
    processors = len(Simulation(packs.build(name), {}, "EXTRA_SMALL", 1, START).model.processors)

    # Most are edge_kpis' chart-only roll-ups, one per entry and exit of each small flow.
    assert len(pack.kpis) <= processors / 2, (len(pack.kpis), processors)
