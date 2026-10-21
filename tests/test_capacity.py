"""A throughput fix too small for the intake goes back to the agent (actions/capacity.py, NFH-61)."""

from flows import packs
from nifi_healer.actions import capacity, executor
from nifi_healer.agent import graph
from nifi_healer.flow.inventory import inventory

from test_agent_graph import S01_FIX, ScriptedLLM, agent, proposal  # noqa: F401 (agent is a fixture)


def tasks(target, value):
    return {"action": "SET_CONCURRENT_TASKS", "target": target, "value": value}


def schedule(target, value):
    return {"action": "SET_RUN_SCHEDULE", "target": target, "value": value}


def problems(client, store, incident, steps):
    return capacity.check(executor.plan(client, incident, steps, store), incident, store, client.now())


def test_a_fix_short_of_the_intake_is_sent_back(agent):
    # S30: 20/s in; picking 1 task every 125 ms (8/s), labelling every 200 ms (5/s).
    _, _, store, incidents, install = agent("p12-warehouse")
    incident = incidents["S30"]
    model = install(ScriptedLLM(proposal([tasks("S30 Pick Items", 2)]),
                                proposal([tasks("S30 Pick Items", 4), tasks("S30 Label Parcel", 8)])))

    state = graph.start(incident["id"])

    retry = model.calls[1][1].content
    assert "S30 Pick Items: 2 task(s) every 125 ms is about 16/s, below the intake of about 20/s" in retry
    assert [p["status"] for p in store.proposals(incident["id"])] == ["SUPERSEDED", "VALID"]
    assert state["attempts"] == 2 and state["plan"]["capacity"] == [] and state["plan"]["warnings"] == []


def test_the_design_fixes_pass(agent):
    _, client, store, incidents, _ = agent("p1-throughput", "p12-warehouse")

    assert problems(client, store, incidents["S01"], S01_FIX) == []                  # 4 tasks at 1 sec, 3/s in
    assert problems(client, store, incidents["S01"], [schedule("S01 Consumer", "250 ms")]) == []
    assert problems(client, store, incidents["S02"], [schedule("S02 Drain", "1 sec")]) == []
    assert problems(client, store, incidents["S03"], [tasks("S03 Transform", 4)]) == []
    assert problems(client, store, incidents["S30"], [schedule("S30 Pick Items", "0 sec")]) == []

    [short] = problems(client, store, incidents["S03"], [tasks("S03 Transform", 2)])
    assert short.startswith("S03 Transform: 2 task(s) every 1 sec is about 2/s, below the intake of about 2/s")


def test_an_expiring_queue_hides_its_backlog(agent):
    # S25's buffer expires what the sessionizer can't reach: it holds steady, never full.
    _, client, store, incidents, _ = agent("p9-clickstream")

    assert problems(client, store, incidents["S25"], [tasks("S25 Sessionize", 4)]) == []
    [short] = problems(client, store, incidents["S25"], [tasks("S25 Sessionize", 2)])
    assert "below the intake of about 20/s" in short


def test_no_design_fix_is_sent_back(agent):
    names = ("p1-throughput", "p2-backpressure", "p8-orders", "p9-clickstream", "p10-iot", "p11-payments",
             "p12-warehouse", "p13-billing")
    _, client, store, incidents, _ = agent(*names)
    fixes = [(s, list(packs.SCENARIOS[s].expected)) for s in incidents] + \
        [(s, [step]) for s in incidents for step in packs.SCENARIOS[s].acceptable]

    # S04 opens later, on a DataFlow alert; the oracle replay covers it.
    assert {s.id for n in names for s in packs.PACKS[n].scenarios} - set(incidents) == {"S04"}
    assert [(s, steps) for s, steps in fixes if problems(client, store, incidents[s], steps)] == []


def test_the_wrong_stage_is_sent_back(agent):
    # S03's Transform is the bottleneck; the queue in front of Publish is empty.
    _, client, store, incidents, _ = agent("p1-throughput")

    assert problems(client, store, incidents["S03"], [tasks("S03 Publish", 8)]) == [
        "None of the processors changed has a backlog in front of it; find the stage whose input queue is full "
        "and whose output queue is not."]


def test_quiet_without_a_measure(agent):
    _, client, store, incidents, _ = agent("p9-clickstream", "p12-warehouse")

    # S24's clicks come out of a SplitText, so the source's rate is not what reaches the sessionizer.
    plan = executor.plan(client, incidents["S24"], [tasks("S24 Sessionize", 2)], store)
    assert capacity._sources(inventory(plan["_definition"], incidents["S24"]["process_group"]),
                             "S24 Sessionize") is None

    # No samples stored at all: nothing to compare with.
    blank = dict(incidents["S30"], deployment_crn="crn:none")
    assert problems(client, store, blank, [tasks("S30 Pick Items", 2)]) == []

    # Slowing a stage down is for something else; a runaway source has nothing in front of it.
    assert problems(client, store, incidents["S30"], [schedule("S30 Pick Items", "1 sec")]) == []
    assert problems(client, store, incidents["S30"], [schedule("S30 Order Feed", "10 sec")]) == []


def test_after_the_last_attempt_it_is_a_warning(agent):
    _, _, store, incidents, install = agent("p12-warehouse")
    incident = incidents["S30"]
    install(ScriptedLLM(*[proposal([tasks("S30 Pick Items", 2)])] * 3))

    state = graph.start(incident["id"])
    review = graph.pending(incident["id"])

    assert state["attempts"] == 3 and review["plan"]["ok"]
    assert [p["status"] for p in store.proposals(incident["id"])] == ["SUPERSEDED", "SUPERSEDED", "VALID"]
    assert any("below the intake" in w for w in review["plan"]["warnings"])


def test_a_reviewers_edit_is_never_sent_back(agent):
    _, _, store, incidents, install = agent("p12-warehouse")
    incident = incidents["S30"]
    install(ScriptedLLM(proposal([schedule("S30 Pick Items", "0 sec")])))

    graph.start(incident["id"])
    state = graph.resume(incident["id"], {"decision": "edit", "steps": [tasks("S30 Pick Items", 2)]})
    review = graph.pending(incident["id"])

    assert state["plan"]["ok"] and review["plan"]["steps"][0]["value"] == 2
    assert any("below the intake" in w for w in review["plan"]["warnings"])
