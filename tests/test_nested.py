"""Nested process groups linked by input and output ports, in the builder, the simulator and the agent (NFH-96)."""

import pytest

from flows.builder.lib import ProcessGroup, check, flow_definition, iter_ports, processor_links
from flows.scenarios.common import dlq, generator, sink, stage
from nifi_healer.actions import capacity
from nifi_healer.flow.edits import EditError, apply, validate_edit
from nifi_healer.flow.inventory import inventory, render, scope
from nifi_healer.sim.engine import DT, Simulation

START = 1_790_000_000.0


def nested(*, inner_period: str = "0 sec", queue: int = 10000, depth: int = 2) -> dict:
    """
    Source -> [Region: in -> (Site: in -> Work -> out) -> out] -> Sink:
    a GenerateFlowFile at 2 per second down through ports and back up.
    """

    root = ProcessGroup("root", "root")
    top = root.group("g", "N Top")
    generator(top, "N Source", period="500 ms")
    sink(top, "sink", "N Sink", period="0 sec")
    group, parents = top, []

    for level in range(1, depth + 1):
        child = group.group(f"l{level}", f"N Level {level}")
        child.input_port("in", f"N In {level}")
        child.output_port("out", f"N Out {level}")
        parents.append((group, child))
        group = child

    stage(group, "work", "N Work", period=inner_period)
    group.connect("in", "work", (), "n-into-work")
    group.connect("work", "out", ["success"], "n-work-out", object_threshold=queue)

    for level, (parent, child) in enumerate(parents, 1):
        upstream = "gen" if level == 1 else "in"
        downstream = "sink" if level == 1 else "out"
        parent.connect(upstream, (child, "in"), ["success"] if level == 1 else (), f"n-down-{level}")
        parent.connect((child, "out"), downstream, (), f"n-up-{level}")

    return flow_definition(root)


def test_ports_serialise_and_connect_across_one_level():
    definition = nested()
    top = definition["flowContents"]["processGroups"][0]
    region = top["processGroups"][0]
    down = next(c for c in top["connections"] if c["name"] == "n-down-1")

    assert check(definition)["problems"] == []
    assert [p["name"] for p in region["inputPorts"]] == ["N In 1"] and region["inputPorts"][0]["type"] == "INPUT_PORT"
    assert down["destination"]["type"] == "INPUT_PORT" and down["destination"]["groupId"] == region["identifier"]
    assert down["source"]["type"] == "PROCESSOR" and down["groupIdentifier"] == top["identifier"]
    assert sorted(p["name"] for _, _, p in iter_ports(definition)) == ["N In 1", "N In 2", "N Out 1", "N Out 2"]


def test_links_are_followed_through_ports():
    top = nested()["flowContents"]["processGroups"][0]
    names = {p["identifier"]: p["name"] for g in (top, top["processGroups"][0]["processGroups"][0])
             for p in g["processors"]}
    links = sorted((names[s], names[d], tuple(via)) for s, d, via in processor_links(top))

    assert links == [("N Source", "N Work", ("n-down-1", "n-down-2", "n-into-work")),
                     ("N Work", "N Sink", ("n-work-out", "n-up-2", "n-up-1"))]

    # Seen from inside, the links that leave the group are not links.
    assert processor_links(top["processGroups"][0]) == []


def test_bad_port_connections_are_refused():
    root = ProcessGroup("root", "root")
    top = root.group("g", "B Top")
    generator(top, "B Source")
    child = top.group("c", "B Child")
    grandchild = child.group("gc", "B Grandchild")
    child.input_port("in", "B In")
    grandchild.input_port("in", "B Deep In")

    with pytest.raises(ValueError, match="not a direct child"):
        top.connect("gen", (grandchild, "in"), ["success"])

    with pytest.raises(ValueError, match="no output port"):
        top.connect((child, "in"), "gen", ())

    with pytest.raises(ValueError, match="no relationships"):
        child.connect("in", (grandchild, "in"), ["success"])

    top.connect("gen", (child, "in"), ["success"], "b-down")
    problems = check(flow_definition(root))["problems"]

    assert "root/B Top/B Child: input port 'B In' is connected to nothing in its group" in problems
    assert "root/B Top/B Child: B Grandchild's port 'B Deep In' is connected to nothing in B Child" in problems


def test_a_hand_edited_end_of_the_wrong_kind_is_found():
    definition = nested()
    top = definition["flowContents"]["processGroups"][0]
    next(c for c in top["connections"] if c["name"] == "n-down-1")["destination"]["type"] = "PROCESSOR"

    assert "root/N Top/n-down-1: destination type is not INPUT_PORT" in check(definition)["problems"]


def run(definition: dict, steps: int = 40) -> Simulation:
    sim = Simulation(definition, {}, "EXTRA_SMALL", 1, START)
    assert sim.model.problems == []

    for _ in range(steps):
        sim.step()

    return sim


@pytest.mark.parametrize("depth", [1, 2, 3])
def test_flowfiles_go_down_through_ports_and_back_up(depth):
    sim = run(nested(depth=depth))

    # 2 per second in, 2 per second out, once the ports have filled the pipe.
    assert sim.latest("processor|N Work|flowfiles_in") == pytest.approx(2.0)
    assert sim.latest("processor|N Sink|flowfiles_in") == pytest.approx(2.0)
    assert not any(k.startswith("processor|N In") or k.startswith("processor|N Out") for k in sim.series)

    # Each hop holds what arrived in the last step, as a direct link does: nothing builds up.
    for name in [f"n-down-{level}" for level in range(1, depth + 1)] + ["n-into-work", "n-work-out", "n-up-1"]:
        assert sim.latest(f"connection|{name}|queued_count") == pytest.approx(2.0 * DT)


def test_back_pressure_beyond_a_port_holds_the_flow_before_it():
    # Work runs once a minute: it queues past n-into-work's threshold, which stops the In ports, and the
    # backlog moves up the levels to the queue Source fills.
    root = ProcessGroup("root", "root")
    top = root.group("g", "P Top")
    generator(top, "P Source", period="100 ms")
    child = top.group("c", "P Child")
    child.input_port("in", "P In")
    sink(child, "sink", "P Slow", period="1 min")
    child.connect("in", "sink", (), "p-inside", object_threshold=50)
    top.connect("gen", (child, "in"), ["success"], "p-outside", object_threshold=100000)
    sim = run(flow_definition(root), steps=80)

    assert sim.latest("connection|p-inside|queued_count") <= 51
    assert sim.latest("connection|p-outside|queued_count") > 500


def test_a_processor_fed_only_through_a_port_is_valid():
    sim = run(nested(), steps=1)
    work = sim.model.by_name("N Work")

    assert work.inputs and not work.problems and not work.is_source


# ---------------------------------------------------------------------
# The agent: inventory, edits, capacity
# ---------------------------------------------------------------------


def agent_flow() -> dict:
    """
    nested() with a CompressContent that drops its failures two levels
    down, a DLQ beside it and one at the top, a flow of Level 1's own, and
    a sibling group beside N Top.
    """

    root = ProcessGroup("root", "root")
    top = root.group("g", "N Top")
    generator(top, "N Source", period="500 ms")
    sink(top, "sink", "N Sink", period="0 sec")
    dlq(top, "N Top DLQ", connected=False)
    region = top.group("l1", "N Level 1")
    site = region.group("l2", "N Level 2")

    for child in (region, site):
        child.input_port("in", f"N In {child.label[-1]}")
        child.output_port("out", f"N Out {child.label[-1]}")

    generator(region, "N Other Source", key="other")
    sink(region, "othersink", "N Other Sink")
    region.connect("other", "othersink", ["success"], "n-other")
    site.processor("work", "N Work", "CompressContent", {
        "Mode": "compress", "Compression Format": "gzip", "Compression Level": "1",
    }, period="0 sec", auto_terminate=("failure",))
    dlq(site, "N Site DLQ", connected=False)
    site.connect("in", "work", (), "n-into-work")
    site.connect("work", "out", ["success"], "n-work-out")
    region.connect("in", (site, "in"), (), "n-down-2")
    region.connect((site, "out"), "out", (), "n-up-2")
    top.connect("gen", (region, "in"), ["success"], "n-down-1")
    top.connect((region, "out"), "sink", (), "n-up-1")

    sibling = root.group("q", "Q Top")
    generator(sibling, "Q Source")
    sink(sibling, "sink", "Q Sink")
    sibling.connect("gen", "sink", ["success"], "q-flow")

    return flow_definition(root)


def test_the_inventory_holds_the_groups_inside_by_path_and_shows_ports():
    definition = agent_flow()
    inv = inventory(definition, "N Top")
    text = render(inv)
    paths = {p["name"]: p["path"] for p in inv["processors"]}

    assert check(definition)["problems"] == []
    assert paths["N Work"] == "N Level 1/N Level 2" and paths["N Other Source"] == "N Level 1"
    assert "Q Source" not in paths
    assert "Process group: N Top / N Level 1 / N Level 2" in text
    assert '- n-into-work: input port "N In 2" -[]-> N Work' in text
    assert '- n-down-1: N Source -[success]-> input port "N In 1"' in text
    assert "N Top DLQ" in inv["dlqs"] and "N Site DLQ" in inv["dlqs"]


def test_a_flat_group_renders_without_paths():
    text = render(inventory(nested(), "N Level 2"))

    assert text.count("Process group:") == 1 and "Groups inside it" not in text


def test_scope_crosses_ports_and_names_the_groups_it_leaves_out():
    inv = inventory(agent_flow(), "N Top")
    scoped = scope(inv, ["n-into-work"], budget=4)
    kept = {p["name"] for p in scoped["processors"]}

    assert {"N Source", "N Work", "N Sink"} <= kept and "N Other Source" not in kept
    assert {c["name"] for c in scoped["connections"]} == {
        "n-down-1", "n-down-2", "n-into-work", "n-work-out", "n-up-2", "n-up-1"}
    assert scoped["outside"]["groups"] == [("N Level 1", 2)]
    assert "Groups inside it with none of their own processors shown: N Level 1 (2 processor(s))." in render(scoped)


def test_an_edit_finds_a_nested_target_by_name_or_by_path():
    definition = agent_flow()

    for target in ("N Work", "N Level 1/N Level 2/N Work"):
        after, changes = apply(definition, "N Top", [{"action": "SET_CONCURRENT_TASKS", "target": target,
                                                      "value": 3}])
        report = validate_edit(definition, after, {}, "N Top")

        # The change is told by the target as the step names it.
        assert changes == [f"{target}: concurrent tasks 1 → 3"]
        assert report["ok"], report["problems"]

    after, _ = apply(definition, "N Top", [{"action": "SET_BACKPRESSURE", "target": "n-into-work",
                                            "object_threshold": 50}])
    assert validate_edit(definition, after, {}, "N Top")["ok"]


def test_a_name_in_two_groups_needs_its_path():
    root = ProcessGroup("root", "root")
    top = root.group("g", "A Top")
    child = top.group("c", "A Child")

    for group in (top, child):
        generator(group, "A Source")
        sink(group, "sink", "A Sink")
        group.connect("gen", "sink", ["success"], f"a-flow-{group.label}")

    definition = flow_definition(root)

    with pytest.raises(EditError, match=r"more than one processor in 'A Top': A Child/A Sink, A Sink"):
        apply(definition, "A Top", [{"action": "SET_CONCURRENT_TASKS", "target": "A Sink", "value": 2}])

    after, _ = apply(definition, "A Top", [{"action": "SET_CONCURRENT_TASKS", "target": "A Child/A Sink",
                                            "value": 2}])
    inner = after["flowContents"]["processGroups"][0]["processGroups"][0]

    assert next(p for p in inner["processors"] if p["name"] == "A Sink")["concurrentlySchedulableTaskCount"] == 2


def test_a_relationship_is_routed_to_the_dlq_beside_it_not_one_in_another_group():
    definition = agent_flow()

    with pytest.raises(EditError, match="the DLQ must be in the source's .DLQs there: N Site DLQ"):
        apply(definition, "N Top", [{"action": "ROUTE_RELATIONSHIP_TO_DLQ", "target": "N Work",
                                     "relationship": "failure", "dlq": "N Top DLQ"}])

    after, changes = apply(definition, "N Top", [{"action": "ROUTE_RELATIONSHIP_TO_DLQ", "target": "N Work",
                                                  "relationship": "failure", "dlq": "N Site DLQ"}])
    site = after["flowContents"]["processGroups"][0]["processGroups"][0]["processGroups"][0]
    added = next(c for c in site["connections"] if c["name"] == "n-to-dlq")

    assert added["groupIdentifier"] == site["identifier"] == added["destination"]["groupId"]
    assert "n-to-dlq: new connection from N Work to N Site DLQ" in changes
    assert validate_edit(definition, after, {}, "N Top")["ok"]


def test_an_edit_to_a_sibling_group_is_outside_the_incident():
    definition = agent_flow()
    after, _ = apply(definition, "Q Top", [{"action": "SET_CONCURRENT_TASKS", "target": "Q Sink", "value": 2}])

    assert "the edit changed something outside 'N Top'" in validate_edit(definition, after, {}, "N Top")["problems"]

    with pytest.raises(EditError, match="no processor named 'Q Sink'"):
        apply(definition, "N Top", [{"action": "SET_CONCURRENT_TASKS", "target": "Q Sink", "value": 2}])


def test_capacity_finds_the_intake_of_a_processor_fed_through_ports():
    inv = inventory(agent_flow(), "N Top")
    sources = capacity._sources(inv, {}, "N Work")

    assert [(s["name"], copies) for s, copies in sources] == [("N Source", 1)]
    assert capacity._configured(sources[0][0], 1) == pytest.approx(2.0)


# ---------------------------------------------------------------------
# Edge KPIs
# ---------------------------------------------------------------------


def _flat_edges(scenario) -> dict:
    """flow_edges as it was before NFH-96: the group's own processors and connections."""

    from flows.builder.processors import SPLITTING, kind_of

    group = ProcessGroup(scenario.id, scenario.group_name)
    scenario.build(group)
    names = {p["identifier"]: p["name"] for p in group.processors.values()}
    links = [(c["source"]["id"], c["destination"]["id"]) for c in group.connections
             if c["source"]["id"] != c["destination"]["id"]]
    fed, feeding = {d for _, d in links}, {s for s, _ in links}

    return {"in": [n for i, n in names.items() if i in feeding and i not in fed],
            "out": [n for i, n in names.items() if i not in feeding],
            "split": [p["name"] for p in group.processors.values() if kind_of(p) in SPLITTING],
            "processors": list(names.values()),
            "connections": [c["name"] for c in group.connections if c["name"]]}


def test_the_edges_of_a_flat_scenario_are_what_they_were():
    from flows.packs import SCENARIOS

    flat = [s for s in SCENARIOS.values() if not _subgroups(s)]

    assert len(flat) >= 97

    for scenario in flat:
        expected = _flat_edges(scenario)

        if scenario.edges:
            expected.update({"in": list(scenario.edges[0]), "out": list(scenario.edges[1])})

        assert scenario.flow_edges() == expected, scenario.id


def _subgroups(scenario) -> list:
    group = ProcessGroup(scenario.id, scenario.group_name)
    scenario.build(group)

    return group.groups


def test_the_edges_of_a_nested_scenario_go_through_its_ports():
    from flows.scenarios.base import Scenario

    def build(pg):
        generator(pg, "E Source", period="500 ms")
        sink(pg, "sink", "E Sink", period="0 sec")
        child = pg.group("c", "E Child")
        child.input_port("in", "E In")
        child.output_port("out", "E Out")
        stage(child, "work", "E Work")
        child.connect("in", "work", (), "e-into-work")
        child.connect("work", "out", ["success"], "e-work-out")
        pg.connect("gen", (child, "in"), ["success"], "e-down")
        pg.connect((child, "out"), "sink", (), "e-up")

    scenario = Scenario("S990", "Edges", "none", "nothing", build, (), "NONE")
    edges = scenario.flow_edges()

    assert edges["in"] == ["E Source"] and edges["out"] == ["E Sink"]
    assert edges["processors"] == ["E Source", "E Sink", "E Work"]
    assert edges["connections"] == ["e-down", "e-up", "e-into-work", "e-work-out"]


# NiFi's boxes, about: a component this size from its position covers its neighbours on the canvas.
BOX = {"processors": (350, 130), "processGroups": (380, 176), "inputPorts": (240, 48), "outputPorts": (240, 48)}


def _overlaps(group: dict, path: str = "") -> list:
    path = f"{path}/{group['name']}"
    boxes = [(c["name"], c["position"]["x"], c["position"]["y"], *BOX[kind]) for kind in BOX for c in group[kind]]
    found = [(path, a[0], b[0]) for i, a in enumerate(boxes) for b in boxes[i + 1:]
             if a[1] < b[1] + b[3] and b[1] < a[1] + a[3] and a[2] < b[2] + b[4] and b[2] < a[2] + a[4]]

    return found + [hit for child in group["processGroups"] for hit in _overlaps(child, path)]


def test_no_component_covers_another_on_the_canvas():
    # Live (NFH-108): on p25, each group inside sat on its crossing's source, and its ports on a stage and on filler.
    from flows import packs

    for name in packs.PACKS:
        assert _overlaps(packs.build(name)["flowContents"]) == [], name
