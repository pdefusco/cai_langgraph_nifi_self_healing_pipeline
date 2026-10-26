"""Several first proposals, the valid fix most of them agree on (agent/nodes.diagnose_and_propose, NFH-86)."""

import config
from nifi_healer.agent import graph, nodes

from test_agent_graph import S01_FIX, ScriptedLLM, agent, proposal  # noqa: F401 (agent is a fixture)


def tasks(target, value):
    return {"action": "SET_CONCURRENT_TASKS", "target": target, "value": value}


def schedule(target, value):
    return {"action": "SET_RUN_SCHEDULE", "target": target, "value": value}


def down(messages):
    raise RuntimeError("endpoint down")


def test_one_draw_by_default(agent):
    _, _, store, incidents, install = agent("p1-throughput")
    model = install(ScriptedLLM(proposal(S01_FIX)))

    state = graph.start(incidents["S01"]["id"])

    assert len(model.calls) == 1 and "vote" not in state["plan"]
    assert "/samples:" not in store.proposals(incidents["S01"]["id"])[-1]["agent_version"]


def test_the_fix_most_agree_on(agent, monkeypatch):
    monkeypatch.setattr(config, "SAMPLES", 3)
    _, _, store, incidents, install = agent("p1-throughput")
    model = install(ScriptedLLM(proposal(S01_FIX), proposal([schedule("S01 Consumer", "250 ms")]),
                                proposal(S01_FIX)))

    state = graph.start(incidents["S01"]["id"])
    [row] = store.proposals(incidents["S01"]["id"])

    assert len(model.calls) == 3 and state["attempts"] == 1
    assert row["status"] == "VALID" and row["action"] == list(S01_FIX)
    assert row["agent_version"].endswith("/samples:3")
    assert state["plan"]["vote"] == {"samples": 3, "replies": 3, "valid": 3, "same_lever": 2, "agreeing": 2}


def test_the_lever_first_then_the_value(agent, monkeypatch):
    monkeypatch.setattr(config, "SAMPLES", 3)
    _, _, store, incidents, install = agent("p1-throughput")
    install(ScriptedLLM(proposal([tasks("S01 Consumer", 6)]), proposal([schedule("S01 Consumer", "250 ms")]),
                        proposal([tasks("S01 Consumer", 8)])))

    state = graph.start(incidents["S01"]["id"])

    assert [s["action"] for s in state["plan"]["steps"]] == ["SET_CONCURRENT_TASKS"]
    assert state["plan"]["vote"]["same_lever"] == 2 and state["plan"]["vote"]["agreeing"] == 1


def test_the_rejected_draws_do_not_vote(agent, monkeypatch):
    # S30: 2 tasks is short of the intake twice over; the one draw that clears it goes on.
    monkeypatch.setattr(config, "SAMPLES", 3)
    _, _, store, incidents, install = agent("p12-warehouse")
    model = install(ScriptedLLM(proposal([tasks("S30 Pick Items", 2)]), proposal([schedule("S30 Pick Items", "0 sec")]),
                                proposal([tasks("S30 Pick Items", 2)])))

    state = graph.start(incidents["S30"]["id"])

    assert len(model.calls) == 3 and state["attempts"] == 1
    assert state["plan"]["ok"] and state["plan"]["steps"] == [schedule("S30 Pick Items", "0 sec")]
    assert state["plan"]["vote"] == {"samples": 3, "replies": 3, "valid": 1, "same_lever": 1, "agreeing": 1}


def test_none_valid_is_sent_back_as_one_would_be(agent, monkeypatch):
    monkeypatch.setattr(config, "SAMPLES", 3)
    _, _, store, incidents, install = agent("p12-warehouse")
    incident = incidents["S30"]
    model = install(ScriptedLLM(*[proposal([tasks("S30 Pick Items", 2)])] * 3,
                                proposal([tasks("S30 Pick Items", 4), tasks("S30 Label Parcel", 8)])))

    state = graph.start(incident["id"])
    first, second = store.proposals(incident["id"])

    # The retry is one draw, with the first one's problems.
    assert len(model.calls) == 4 and state["attempts"] == 2
    assert "below the intake of about 20/s" in model.calls[3][1].content
    assert first["status"] == "SUPERSEDED" and first["validation"]["vote"]["valid"] == 0
    assert second["status"] == "VALID" and "vote" not in second["validation"]


def test_a_failed_call_is_left_out(agent, monkeypatch):
    monkeypatch.setattr(config, "SAMPLES", 3)
    _, _, store, incidents, install = agent("p1-throughput")
    install(ScriptedLLM(down, proposal(S01_FIX), down))

    state = graph.start(incidents["S01"]["id"])

    assert state["plan"]["ok"] and state["plan"]["vote"]["replies"] == 1


def test_every_call_failed(agent, monkeypatch):
    monkeypatch.setattr(config, "SAMPLES", 3)
    _, _, store, incidents, install = agent("p1-throughput")
    install(ScriptedLLM(down, down, down))

    state = graph.start(incidents["S01"]["id"])

    assert state["error"] == "LLM call failed: endpoint down"
    assert store.incident(incidents["S01"]["id"])["status"] == "NEW"


def test_one_fix_however_it_is_written():
    routes = [{"action": "ROUTE_RELATIONSHIP_TO_DLQ", "target": "A", "relationship": "failure", "dlq": "D"},
              {"action": "route_relationship_to_dlq", "target": "A", "relationship": ["retry"], "dlq": "D"}]
    merged = [{"action": "ROUTE_RELATIONSHIP_TO_DLQ", "target": "A", "relationship": ["retry", "failure"],
               "dlq": "D"}]

    assert nodes._fix(routes) == nodes._fix(merged)
    assert nodes._fix([tasks("A", 4), schedule("B", "1 sec")]) == nodes._fix([schedule("B", "1 sec"), tasks("A", "4")])
    assert nodes._fix([tasks("A", 4)]) != nodes._fix([tasks("A", 8)])
    assert nodes._fix([tasks("A", 4)], values=False) == nodes._fix([tasks("A", 8)], values=False)
