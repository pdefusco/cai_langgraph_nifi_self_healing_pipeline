"""
The second-wave packs in the simulator: each builds and compiles, its
faults present as the detectors see them, the design fix clears each
one, and the wrong fixes it names do not. A compound fault clears only
once all its faults are fixed, and a masked one shows a new signal
after the first.

A timed fault (intermittent or slow burn) is polled every SCAN seconds
from midnight UTC instead: it shows within its detect_within, an
intermittent one clears on its own and comes back, a slow burn warns
before it breaches, and a fix is judged over a whole detect_within
after its clear_within, so the next burst or batch is in it.

A control is polled the same way over its detect_within: nothing
critical shows, though the noise it was built with does.
"""

import copy

import pytest

from flows import packs
from flows.builder.lib import check
from flows.scenarios.base import action
from nifi_healer.flow import edits
from nifi_healer.kpi import detectors, metrics
from nifi_healer.sim.engine import Simulation

START = 1_800_000_000.0
SETTLE = 3600.0         # long enough for every steady second-wave fault to present
AFTER_FIX = 1800.0      # the longest clear_within among them
WAVE_2 = [name for name, pack in packs.PACKS.items() if pack.wave == 2]
SCOPES = {"connection": "CONNECTION", "processor": "PROCESSOR", "system": "SYSTEM"}
MASKED = {"S31", "S33"}   # the second fault shows only once the first is fixed
STEADY = ("cascade", "compound")        # faulty from deploy on, and until fixed
TIMED = ("intermittent", "slow_burn")   # on a schedule, or only after hours
CONTROL = ("control",)                  # healthy, and noisy enough to tempt a fix
MIDNIGHT = START - 8 * 3600             # 00:00 UTC: a day's schedule runs from the start
SCAN = 300.0

# What makes each control noisy, from what it showed over its detect_within:
# (each KPI's values by key, the warnings polled, its processor count).
NOISE = {
    "S40": lambda seen, warned, size: (max(seen["S40:connection:s40-to-load:queued_count"]) > 0.7 * 5000
                                       and "trend_to_breach" in warned),
    "S41": lambda seen, warned, size: (max(seen["S41:processor:S41 Reject DLQ:flowfiles_in"]) > 0
                                       and min(seen["S41:processor:S41 Validate Partner Order:flowfiles_in"]) == 0),
    "S42": lambda seen, warned, size: (max(seen["S42:processor:S42 Load Readings:flowfiles_out"])
                                       >= 10 * min(seen["S42:processor:S42 Load Readings:flowfiles_out"]) > 0),
    "S43": lambda seen, warned, size: (min(seen["S43:processor:S43 Post Ledger:flowfiles_out"]) == 0
                                       < max(seen["S43:processor:S43 Post Ledger:flowfiles_out"])),
    "S44": lambda seen, warned, size: size == 30,
    "S45": lambda seen, warned, size: max(seen["S45:system::cpu"]) >= 70,
}


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


def _signals(sim: Simulation, pack, scenario) -> list:
    """The scenario's signals, as one poll of this simulation finds them."""

    kpis = [_kpi(k) for k in pack.kpis]
    kinds = {p.name: p.kind for p in sim.model.processors.values()}
    found = detectors.detect(pack.name, {"kpis": kpis}, _charts(sim, kpis), [], sim.t, kinds)

    return [s for s in found["signals"] if s["scenario"] == scenario.id]


def _faulty(sim: Simulation, pack, scenario) -> list:
    """The scenario's critical signals, as one poll of this simulation finds them."""

    return [s["summary"] for s in _signals(sim, pack, scenario) if s["severity"] == detectors.CRITICAL]


def _polls(sim: Simulation, pack, scenario, seconds: float, until_faulty: bool = False) -> list:
    """
    [(t, critical summaries, warning types)], polled every SCAN seconds
    for `seconds` as `sim` moves on; with `until_faulty`, up to the
    first poll that finds a critical signal.
    """

    polls, t, end = [], sim.t, sim.t + seconds

    while t < end:
        t += SCAN
        sim.advance(t)
        found = _signals(sim, pack, scenario)
        polls.append((t, [s["summary"] for s in found if s["severity"] == detectors.CRITICAL],
                      [s["type"] for s in found if s["severity"] == detectors.WARNING]))

        if until_faulty and polls[-1][1]:
            break

    return polls


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


def _scenarios(*kinds) -> list:
    return [(name, s) for name in WAVE_2 for s in packs.PACKS[name].scenarios if s.kind in kinds]


def _cases(field: str, *kinds) -> list:
    return [pytest.param(name, s.id, i, id=f"{s.id}-{i}")
            for name, s in _scenarios(*kinds)
            for i in range(len(getattr(s, field)))]


@pytest.fixture(scope="module")
def shown():
    """
    {timed scenario id: (its simulation at the first poll that finds it
    faulty, the polls up to it)}, deployed at midnight UTC, built once.
    """

    sims = {}

    for name, scenario in _scenarios(*TIMED):
        pack = packs.PACKS[name]
        sim = Simulation(packs.build(name), dict(pack.parameters), pack.cluster_size, pack.nodes, MIDNIGHT)
        sims[scenario.id] = sim, _polls(sim, pack, scenario, scenario.detect_within, until_faulty=True)

    return sims


@pytest.fixture(scope="module")
def quiet():
    """
    {control id: (its polls over its detect_within, each of its KPIs'
    values by key)}, deployed at midnight UTC, built once.
    """

    sims = {}

    for name, scenario in _scenarios(*CONTROL):
        pack = packs.PACKS[name]
        sim = Simulation(packs.build(name), dict(pack.parameters), pack.cluster_size, pack.nodes, MIDNIGHT)
        polls, seen = [], {}

        while sim.t < MIDNIGHT + scenario.detect_within:
            polls += _polls(sim, pack, scenario, SCAN)

            # Until the 30-minute charts stop reaching back before the deploy.
            if sim.t - MIDNIGHT < 3600:
                continue

            for k in scenario.kpis:
                datas = sim.chart(f"{k.kind}|{k.component}|{k.metric}", "LAST_THIRTY_MINUTES", sim.t)
                seen.setdefault(k.key, []).extend(v for _, v in metrics.points({"datas": datas}))

        sims[scenario.id] = polls, seen

    return sims


def _after_shown(shown, name, scenario, steps) -> list:
    """The polls over a detect_within, from clear_within after `steps` are applied to the fault as it shows."""

    pack = packs.PACKS[name]
    sim = copy.deepcopy(shown[scenario.id][0])
    _apply(sim, scenario, steps)
    sim.advance(sim.t + scenario.clear_within)

    return _polls(sim, pack, scenario, scenario.detect_within)


def test_the_second_wave_is_not_provisioned_by_default():
    assert WAVE_2 and not set(WAVE_2) & set(packs.DEFAULT)

    for name in WAVE_2:
        for scenario in packs.PACKS[name].scenarios:
            # A control has nothing to fix, so no fix is wrong for it but every one.
            assert scenario.kind != "classic" and (scenario.wrong or scenario.kind == "control"), scenario.id


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


@pytest.mark.parametrize("name, scenario_id", [pytest.param(n, s.id, id=s.id) for n, s in _scenarios(*STEADY)])
def test_each_fault_presents(settled, name, scenario_id):
    assert _faulty(settled[name], packs.PACKS[name], packs.SCENARIOS[scenario_id])


@pytest.mark.parametrize("name, scenario_id", [pytest.param(n, s.id, id=s.id)
                                               for n, s in _scenarios("cascade")])
def test_the_design_fix_clears_the_fault(settled, name, scenario_id):
    pack, scenario = packs.PACKS[name], packs.SCENARIOS[scenario_id]
    sim = _after(settled, pack, scenario, scenario.expected)

    assert _faulty(sim, pack, scenario) == []


@pytest.mark.parametrize("name, scenario_id, index", _cases("acceptable", "cascade"))
def test_an_acceptable_fix_clears_it_alone(settled, name, scenario_id, index):
    pack, scenario = packs.PACKS[name], packs.SCENARIOS[scenario_id]
    sim = _after(settled, pack, scenario, [scenario.acceptable[index]])

    assert _faulty(sim, pack, scenario) == []


@pytest.mark.parametrize("name, scenario_id", [pytest.param(n, s.id, id=s.id)
                                               for n, s in _scenarios("compound")])
def test_a_compound_fault_clears_once_all_its_faults_are_fixed(settled, name, scenario_id):
    pack, scenario = packs.PACKS[name], packs.SCENARIOS[scenario_id]

    assert len(scenario.faults) >= 2 and scenario.expected == scenario.faults[:1]
    assert scenario.faults[1] in scenario.acceptable
    assert _faulty(_after(settled, pack, scenario, scenario.faults), pack, scenario) == []


@pytest.mark.parametrize("name, scenario_id, index", _cases("faults", "compound"))
def test_one_of_its_faults_fixed_alone_does_not(settled, name, scenario_id, index):
    pack, scenario = packs.PACKS[name], packs.SCENARIOS[scenario_id]
    left = _faulty(_after(settled, pack, scenario, [scenario.faults[index]]), pack, scenario)

    assert left, scenario.faults[index]

    if scenario.id in MASKED and index == 0:
        # What is left was not visible before: the masked fault, not the stall.
        assert not set(left) & set(_faulty(settled[name], pack, scenario)), left


@pytest.mark.parametrize("name, scenario_id, index", _cases("wrong", *STEADY))
def test_a_wrong_fix_does_not(settled, name, scenario_id, index):
    pack, scenario = packs.PACKS[name], packs.SCENARIOS[scenario_id]
    sim = _after(settled, pack, scenario, [scenario.wrong[index]])

    assert _faulty(sim, pack, scenario), scenario.wrong[index]


def test_every_second_wave_scenario_is_steady_timed_or_a_control():
    assert {s.kind for _, s in _scenarios(*STEADY, *TIMED, *CONTROL)} == set(STEADY + TIMED + CONTROL)
    assert len(_scenarios(*STEADY, *TIMED, *CONTROL)) == sum(len(packs.PACKS[n].scenarios) for n in WAVE_2)


@pytest.mark.parametrize("name, scenario_id", [pytest.param(n, s.id, id=s.id) for n, s in _scenarios(*CONTROL)])
def test_a_control_stays_quiet_over_its_detect_within(quiet, name, scenario_id):
    polls = quiet[scenario_id][0]
    critical = [(t, found) for t, found, _ in polls if found]

    assert polls[-1][0] >= MIDNIGHT + packs.SCENARIOS[scenario_id].detect_within
    assert critical == [], critical[:1]


@pytest.mark.parametrize("name, scenario_id", [pytest.param(n, s.id, id=s.id) for n, s in _scenarios(*CONTROL)])
def test_a_control_is_noisy(quiet, name, scenario_id):
    scenario = packs.SCENARIOS[scenario_id]
    polls, seen = quiet[scenario_id]
    warned = {w for _, _, warnings in polls for w in warnings}
    model = Simulation(packs.build(name), {}, "EXTRA_SMALL", 1, START).model
    size = sum(p.group_name == scenario.group_name for p in model.processors.values())

    assert scenario.expected == (action("NO_ACTION"),) and not scenario.wrong
    assert NOISE[scenario_id](seen, warned, size), {k: (min(v), max(v)) for k, v in seen.items()}


@pytest.mark.parametrize("name, scenario_id", [pytest.param(n, s.id, id=s.id) for n, s in _scenarios(*TIMED)])
def test_a_timed_fault_shows_within_its_detect_within(shown, name, scenario_id):
    polls = shown[scenario_id][1]

    assert polls[-1][1], f"{scenario_id}: nothing critical in {packs.SCENARIOS[scenario_id].detect_within} s"


@pytest.mark.parametrize("name, scenario_id", [pytest.param(n, s.id, id=s.id)
                                               for n, s in _scenarios("slow_burn")])
def test_a_slow_burn_warns_before_it_breaches(shown, name, scenario_id):
    polls = shown[scenario_id][1]

    # Healthy for its first hour, then a trend warning ahead of the breach.
    assert polls[-1][0] - MIDNIGHT > 3600
    assert any("trend_to_breach" in warnings for _, _, warnings in polls[:-1])


@pytest.mark.parametrize("name, scenario_id", [pytest.param(n, s.id, id=s.id)
                                               for n, s in _scenarios("intermittent")])
def test_an_intermittent_fault_clears_on_its_own_and_comes_back(shown, name, scenario_id):
    scenario = packs.SCENARIOS[scenario_id]
    sim = copy.deepcopy(shown[scenario_id][0])
    faulty = [bool(critical) for _, critical, _ in _polls(sim, packs.PACKS[name], scenario,
                                                           2 * scenario.detect_within)]

    assert False in faulty and True in faulty[faulty.index(False):], faulty


@pytest.mark.parametrize("name, scenario_id, index",
                         [pytest.param(n, s.id, -1, id=f"{s.id}-expected") for n, s in _scenarios(*TIMED)]
                         + _cases("acceptable", *TIMED))
def test_a_timed_fix_holds_through_the_next_burst(shown, name, scenario_id, index):
    scenario = packs.SCENARIOS[scenario_id]
    steps = scenario.expected if index < 0 else [scenario.acceptable[index]]
    left = [(t, critical) for t, critical, _ in _after_shown(shown, name, scenario, steps) if critical]

    assert left == [], left[:1]


@pytest.mark.parametrize("name, scenario_id, index", _cases("wrong", *TIMED))
def test_a_wrong_timed_fix_does_not(shown, name, scenario_id, index):
    scenario = packs.SCENARIOS[scenario_id]

    assert any(critical for _, critical, _ in _after_shown(shown, name, scenario, [scenario.wrong[index]])), \
        scenario.wrong[index]
