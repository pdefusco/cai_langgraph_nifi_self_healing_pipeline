"""
The customer-sized packs (NFH-95) in the simulator. The filler is
deterministic and healthy, and never touches the fault, so a size twin
is its twin with more flow around it: the same readings, shown at the
same poll, and the same verdict on its design fix, its acceptable fixes
and its wrong ones.
"""

import copy

import pytest

from flows import packs
from flows.build_all import check_pack
from flows.builder.lib import ProcessGroup, check
from flows.scenarios import scale
from nifi_healer.sim.engine import Simulation
from test_new_packs import START, _apply, _faulty, _polls

WAVE_3 = [name for name, pack in packs.PACKS.items() if pack.wave == 3]
TWINS = [s for name in WAVE_3 for s in packs.PACKS[name].scenarios if s.twin]


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

    assert (scenario.family, scenario.kind, scenario.fault) == (small.family, small.kind, small.fault)
    assert scenario.expected == rename(small.expected) and scenario.traps == rename(small.traps)
    assert [(k.component, k.metric, k.above) for k in scenario.kpis] == \
        [(rename(k.component), k.metric, k.above) for k in small.kpis]


@pytest.mark.parametrize("scenario", TWINS, ids=lambda s: s.id)
def test_the_filler_fills_the_group_and_never_touches_the_fault(scenario):
    group, core = _group(scenario), ProcessGroup(scenario.id, scenario.group_name)
    scenario.build.core(core)
    names = {p["name"] for p in core.processors.values()}
    target = scenario.build.processors

    assert 0.95 * target <= len(group.processors) <= target
    assert len(set(scenario.build.shapes)) == len(scale.SHAPES)

    for c in group.connections:
        assert (c["source"]["name"] in names) == (c["destination"]["name"] in names), c["name"]


@pytest.mark.parametrize("scenario", TWINS, ids=lambda s: s.id)
def test_a_twin_presents_and_clears_as_its_twin_does(scenario):
    small = packs.SCENARIOS[scenario.twin]
    rename = scale.renamer(small.id, scenario.id)
    found, expected = _verdicts(scenario), _verdicts(small)

    assert found["expected"] == []
    assert found == {label: rename(value) for label, value in expected.items()}
