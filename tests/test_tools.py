"""Read-only tools the agent may call before it proposes (agent/tools.py, NFH-91)."""

import json

import pytest
from langchain_core.messages import AIMessage

import config
from nifi_healer import fleet
from nifi_healer.agent import graph, prompts, tools
from nifi_healer.runtime.poller import Poller

from test_agent_graph import S01_FIX, ScriptedLLM, agent, proposal  # noqa: F401 (agent is a fixture)


def tasks(target, value):
    return {"action": "SET_CONCURRENT_TASKS", "target": target, "value": value}


def schedule(target, value):
    return {"action": "SET_RUN_SCHEDULE", "target": target, "value": value}


def check_fix(*steps):
    return json.dumps({"tool": "check_fix", "steps": list(steps)})


def history(component, metric):
    return json.dumps({"tool": "kpi_history", "component": component, "metric": metric})


@pytest.fixture
def armed(monkeypatch):
    monkeypatch.setattr(config, "TOOLS", True)
    monkeypatch.setattr(config, "TOOL_CALLS", 3)


def test_the_agent_checks_a_fix_before_it_proposes(agent, armed):
    # S30: 20/s in; 2 pickers every 125 ms is 16/s. Told so, it proposes the 0 sec schedule, valid first time.
    _, _, store, incidents, install = agent("p12-warehouse")
    incident = incidents["S30"]
    model = install(ScriptedLLM(check_fix(tasks("S30 Pick Items", 2)), check_fix(schedule("S30 Pick Items", "0 sec")),
                                proposal([schedule("S30 Pick Items", "0 sec")])))

    state = graph.start(incident["id"])

    assert len(model.calls) == 3 and '"tool": "check_fix"' in model.calls[0][0].content
    first, second = model.calls[1][-1].content, model.calls[2][-1].content
    assert first.startswith("check_fix answered:\nThese steps would be sent back:\n- S30 Pick Items: 2 task(s) "
                            "every 125 ms is about 16/s, below the intake of about 20/s")
    assert first.endswith("You may call 2 more tool(s), or reply with the proposal.")
    assert "No problems: validation and the capacity check would pass these steps." in second
    assert isinstance(model.calls[1][-2], AIMessage) and model.calls[1][-2].content == check_fix(
        tasks("S30 Pick Items", 2))

    [row] = store.proposals(incident["id"])
    assert row["status"] == "VALID" and state["attempts"] == 1
    assert row["agent_version"].endswith(f"/tools:{prompts.TOOLS_VERSION}x3")
    called = row["validation"]["tool_calls"]
    assert [(c["tool"], c["args"]["steps"][0]["value"]) for c in called] == [("check_fix", 2), ("check_fix", "0 sec")]
    assert called[0]["result"].startswith("These steps would be sent back:")


def test_kpi_history_reads_the_store(agent):
    _, client, store, incidents, _ = agent("p2-backpressure")
    incident = incidents["S07"]
    Poller(client, store).backfill("p2-backpressure", fleet.pack("p2-backpressure"))
    calls = tools.Calls(lambda steps: [], incident, store, client.now())

    # Per second, in 10 minute steps; the backfilled hour and the hour before it, with nothing.
    shown = calls.run(json.loads(history("s07  SOURCE", "flowfiles_out"))).splitlines()
    assert shown[:2] == ["kpi_history answered:", "S07 Source flowfiles_out, the last 2 h in 10 min steps, oldest first:"]
    assert shown[2] == "- 120 to 110 min ago: no data"
    assert shown[13].startswith("- 10 to 0 min ago: 0.") and shown[13].endswith("/s at most")

    missing = calls.run(json.loads(history("S07 Sink", "queued_count")))
    assert "S07 Sink queued_count is not measured. Measured: S07 Fan Out flowfiles_out; S07 Sink flowfiles_in;" in missing
    assert calls.run({"tool": "restart"}).startswith('restart answered:\nThere is no tool "restart"; the tools are '
                                                      "check_fix, kpi_history.\n\nThat was the last tool call")
    assert [c["tool"] for c in calls.trace] == ["kpi_history", "kpi_history", "restart"]


def test_a_call_past_the_budget_is_refused(agent, armed):
    _, _, store, incidents, install = agent("p1-throughput")
    ask = history("S01 Consumer", "flowfiles_in")
    model = install(ScriptedLLM(ask, ask, ask, ask, ask, proposal(S01_FIX)))

    state = graph.start(incidents["S01"]["id"])

    # Three answered, one told none are left, then a tool request is a bad reply, repaired once.
    assert len(model.calls) == 6
    assert model.calls[3][-1].content.endswith(prompts.TOOL_LAST)
    assert model.calls[4][-1].content == prompts.TOOL_NONE_LEFT
    assert model.calls[5][-1].content.startswith("Your reply could not be used:")
    assert len(state["tool_calls"]) == 3 and store.proposals(incidents["S01"]["id"])[-1]["status"] == "VALID"


def test_a_bad_check_is_an_answer():
    calls = tools.Calls(lambda steps: 1 / 0, {}, None, 0.0, budget=2)

    assert "check_fix needs \"steps\"" in calls.run({"tool": "check_fix", "steps": "4 tasks"})
    assert "check_fix failed: division by zero" in calls.run({"tool": "check_fix", "steps": list(S01_FIX)})


def test_off_by_default(agent):
    _, _, store, incidents, install = agent("p1-throughput")
    model = install(ScriptedLLM(proposal(S01_FIX)))

    state = graph.start(incidents["S01"]["id"])

    assert "check_fix" not in model.calls[0][0].content and prompts.system_prompt() == model.calls[0][0].content
    assert "/tools:" not in store.proposals(incidents["S01"]["id"])[-1]["agent_version"]
    assert "tool_calls" not in state["plan"] and state["tool_calls"] == []


def test_each_draw_has_its_own_calls(agent, armed, monkeypatch):
    monkeypatch.setattr(config, "SAMPLES", 3)
    _, _, store, incidents, install = agent("p1-throughput")

    def reply(messages):
        return check_fix(*S01_FIX) if len(messages) == 2 else proposal(S01_FIX)(messages)

    install(ScriptedLLM(*[reply] * 6))

    state = graph.start(incidents["S01"]["id"])

    drawn = state["plan"]["vote"]["drawn"]
    assert [len(d["tools"]) for d in drawn] == [1, 1, 1] and state["plan"]["vote"]["valid"] == 3
    assert all(d["tools"][0]["result"].startswith("No problems") for d in drawn)
    assert len(state["plan"]["tool_calls"]) == 1


class Counting:
    def __init__(self):
        self.asked = []

    def flow_definition(self, crn):
        self.asked.append(crn)
        return {"error": "busy"} if len(self.asked) == 1 else {"crn": crn, "groups": []}

    def now(self):
        return 5.0


def test_flow_reads_are_remembered():
    client = Counting()
    reads = tools.Reads(client)

    assert reads.flow_definition("a") == {"error": "busy"}           # an error is asked again
    found = reads.flow_definition("a")
    found["groups"].append("changed")

    assert reads.flow_definition("a") == {"crn": "a", "groups": []}  # a copy each time
    assert client.asked == ["a", "a"] and reads.now() == 5.0
