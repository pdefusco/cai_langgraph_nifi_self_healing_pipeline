"""The action catalog's bounds, and every scenario's known fix applied to its own flow."""

import pytest

from flows import packs
from nifi_healer.actions import catalog
from nifi_healer.flow import edits
from nifi_healer.flow.inventory import inventory, render
from nifi_healer.sim.world import groups_from_definition, parameters_of

FLOW_EDITS = [(sid, "expected", s.expected) for sid, s in packs.SCENARIOS.items()
              if catalog.family_of([catalog.normalize(x) for x in s.expected]) == catalog.FLOW_EDIT]
FLOW_EDITS += [(sid, f"acceptable[{i}]", (alt,)) for sid, s in packs.SCENARIOS.items()
               for i, alt in enumerate(s.acceptable)
               if catalog.CATALOG.get(catalog.normalize(alt)["action"], None)
               and catalog.CATALOG[catalog.normalize(alt)["action"]].family == catalog.FLOW_EDIT]


@pytest.fixture(scope="module")
def definitions():
    return {name: packs.build(name) for name in packs.PACKS}


@pytest.mark.parametrize("scenario_id, label, steps", FLOW_EDITS,
                         ids=[f"{s}-{l}" for s, l, _ in FLOW_EDITS])
def test_known_fix_applies_cleanly(definitions, scenario_id, label, steps):
    scenario = packs.SCENARIOS[scenario_id]
    definition = definitions[packs.pack_of(scenario_id).name]
    normalized = [catalog.normalize(s) for s in steps]

    assert catalog.check_steps(normalized) == []

    edited, changes = edits.apply(definition, scenario.group_name, normalized)
    report = edits.validate_edit(definition, edited, parameters_of(groups_from_definition(definition)),
                                 scenario.group_name)

    assert report["ok"], report["problems"]
    assert changes and report["diff"]


def test_every_scenario_fix_is_in_bounds():
    for sid, scenario in packs.SCENARIOS.items():
        for steps in (scenario.expected,) + tuple((alt,) for alt in scenario.acceptable):
            assert catalog.check_steps([catalog.normalize(s) for s in steps]) == [], sid


def test_edit_that_changes_nothing_is_an_error(definitions):
    definition = definitions["p1-throughput"]

    with pytest.raises(edits.EditError, match="already 1"):
        edits.apply(definition, "S01 Slow Consumer",
                    [{"action": "SET_CONCURRENT_TASKS", "target": "S01 Consumer", "value": 1}])


def test_edit_stays_inside_its_group(definitions):
    definition = definitions["p1-throughput"]

    with pytest.raises(edits.EditError, match="no processor named 'S02 Drain'"):
        edits.apply(definition, "S01 Slow Consumer",
                    [{"action": "SET_RUN_SCHEDULE", "target": "S02 Drain", "value": "1 sec"}])

    edited, _ = edits.apply(definition, "S02 Slow Run Schedule",
                            [{"action": "SET_RUN_SCHEDULE", "target": "S02 Drain", "value": "1 sec"}])
    report = edits.validate_edit(definition, edited, {}, "S01 Slow Consumer")
    assert not report["ok"] and any("outside" in p for p in report["problems"])


def test_edit_keeps_identifiers(definitions):
    definition = definitions["p3-errors"]
    group = "S09 Failure Self-Loop"
    steps = [s for s in packs.SCENARIOS["S09"].expected]
    edited, _ = edits.apply(definition, group, steps)
    ids = lambda d: {p["identifier"] for p in edits.find_group(d, group)["processors"]}

    assert ids(edited) == ids(definition)


def test_inventory_flags_unhandled_and_masks_secrets(definitions):
    definition = definitions["p3-errors"]
    text = render(inventory(definition, "S10 Unreachable Endpoint",
                            [{"name": "ctx", "parameters": [{"name": "nope", "value": "x"}]}]))

    assert "S10 Partner Call (self-loop)" in text and "Dead-letter processors: S10 DLQ" in text
    assert "nope" not in text

    inv = inventory(definitions["p5-parameters"], "S17 Wrong Endpoint Parameter",
                    [{"name": "nfh-p5-parameters", "parameters": [
                        {"name": "s17.target.url", "value": "secret", "sensitive": True}]}])
    assert inv["parameters"] == [{"context": "nfh-p5-parameters", "name": "s17.target.url",
                                  "value": "********", "sensitive": True}]


@pytest.mark.parametrize("step, fragment", [
    ({"action": "SET_CONCURRENT_TASKS", "target": "P", "value": 0}, "1 to 8"),
    ({"action": "SET_CONCURRENT_TASKS", "target": "P", "value": "3"}, None),
    ({"action": "SET_RUN_SCHEDULE", "target": "P", "value": "2 hours"}, "at most 1 hour"),
    ({"action": "SET_RUN_SCHEDULE", "target": "P", "value": "fast"}, "duration"),
    ({"action": "SET_BACKPRESSURE", "target": "C"}, "object_threshold and/or"),
    ({"action": "SET_BACKPRESSURE", "target": "C", "size_threshold": "20 GB"}, "at most 10 GB"),
    ({"action": "SET_FLOWFILE_EXPIRATION", "target": "C", "value": "0 sec"}, None),
    ({"action": "SET_RETRY_POLICY", "target": "P", "backoff": "SOMETIMES"}, "PENALIZE_FLOWFILE"),
    ({"action": "SET_PROCESSOR_PROPERTY", "target": "P", "property": "k", "value": "#{x}"}, "parameter"),
    ({"action": "SET_SCHEDULED_STATE", "target": "P", "value": "DISABLED"}, "ENABLED"),
    ({"action": "ROUTE_RELATIONSHIP_TO_DLQ", "target": "P", "relationship": "failure", "dlq": "Sink"}, "DLQ"),
    ({"action": "SCALE_DEPLOYMENT", "cluster_size": "HUGE", "nodes": 9}, "cluster_size"),
    ({"action": "RESTART_DEPLOYMENT"}, "unknown action"),
    ({"action": "SET_CONCURRENT_TASKS", "value": 2}, "target"),
])
def test_bounds(step, fragment):
    problems = catalog.check(catalog.normalize(step))

    if fragment is None:
        assert problems == []

    else:
        assert any(fragment in p for p in problems), problems


def test_proposal_shape_rules():
    no_action = {"action": "NO_ACTION"}
    tasks = {"action": "SET_CONCURRENT_TASKS", "target": "P", "value": 2}
    scale = {"action": "SCALE_DEPLOYMENT", "cluster_size": "SMALL", "nodes": 1}

    assert catalog.check_steps([]) == ["a proposal needs at least one step"]
    assert any("only step" in p for p in catalog.check_steps([no_action, tasks]))
    assert any("mix API paths" in p for p in catalog.check_steps([tasks, scale]))
    assert any("twice" in p for p in catalog.check_steps([tasks, dict(tasks, value=3)]))
    assert any("at most 4" in p for p in catalog.check_steps([
        dict(tasks, target=f"P{i}") for i in range(5)]))
    assert catalog.risk_of([tasks]) == "low" and catalog.risk_of([scale]) == "high"


def test_routes_of_one_processor_to_one_dlq_are_one_step():
    route = {"action": "ROUTE_RELATIONSHIP_TO_DLQ", "target": "P", "relationship": "Retry", "dlq": "P DLQ"}
    other = dict(route, target="Q", relationship="failure")
    tasks = {"action": "SET_CONCURRENT_TASKS", "target": "P", "value": 2}

    merged = catalog.merge_routes([route, other, tasks, dict(route, relationship="Failure"),
                                   dict(route, relationship=["No Retry", "Retry"])])

    assert merged == [dict(route, relationship=["Failure", "No Retry", "Retry"]), other, tasks]
    assert catalog.check_steps(merged[:2]) == [] and route["relationship"] == "Retry"     # the input is kept
    assert catalog.merge_routes([route, dict(route, dlq="Other DLQ")])[1]["dlq"] == "Other DLQ"
