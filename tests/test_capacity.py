"""A throughput fix too small for the intake goes back to the agent (actions/capacity.py, NFH-61)."""

import pytest

import config
from flows import packs
from nifi_healer import fleet
from nifi_healer.actions import capacity, executor
from nifi_healer.agent import graph
from nifi_healer.flow.inventory import inventory
from nifi_healer.kpi import readings
from nifi_healer.runtime.poller import Poller

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
    # 2/s against about 2/s: enough to keep up, not to drain the backlog, and not called short of it.
    assert short.startswith("S03 Transform: 2 task(s) every 1 sec is about 2/s, within 10% of the intake of about 2/s")


def test_an_expiring_queue_hides_its_backlog(agent):
    # S25's buffer expires what the sessionizer can't reach: it holds steady, never full.
    _, client, store, incidents, _ = agent("p9-clickstream")

    assert problems(client, store, incidents["S25"], [tasks("S25 Sessionize", 4)]) == []
    [short] = problems(client, store, incidents["S25"], [tasks("S25 Sessionize", 2)])
    assert "20/s, within 10% of the intake of about 20/s" in short


@pytest.mark.parametrize("watch_all", [False, True])
def test_no_design_fix_is_sent_back(agent, monkeypatch, watch_all):
    # With every queue and rate measured (NFH-65) the check knows more, and still passes them.
    monkeypatch.setattr(config, "WATCH_ALL", watch_all)
    names = ("p1-throughput", "p2-backpressure", "p8-orders", "p9-clickstream", "p10-iot", "p11-payments",
             "p12-warehouse", "p13-billing")
    _, client, store, incidents, _ = agent(*names)
    # S07's acceptable 100 ms and 4 tasks keep pace with the fan-out at best, and leave its backlog in place
    # (NFH-83); they are sent back.
    fixes = [(s, list(packs.SCENARIOS[s].expected)) for s in incidents] + \
        [(s, [step]) for s in incidents for step in packs.SCENARIOS[s].acceptable if s != "S07"]

    # S04 opens later, on a DataFlow alert; the oracle replay covers it.
    assert {s.id for n in names for s in packs.PACKS[n].scenarios} - set(incidents) == {"S04"}
    assert [(s, steps) for s, steps in fixes if problems(client, store, incidents[s], steps)] == []


def test_the_wrong_stage_is_sent_back(agent):
    # S03's Transform is the bottleneck; the queue in front of Publish is empty.
    _, client, store, incidents, _ = agent("p1-throughput")

    assert problems(client, store, incidents["S03"], [tasks("S03 Publish", 8)]) == [
        "None of the processors changed has a backlog in front of it; find the stage whose input queue is full "
        "and whose output queue is not."]


def test_a_measured_split_is_where_the_intake_starts(agent):
    # S24: 2 batches/s from the source, split into 100/s of clicks; the sessionizer does 50/s, back
    # pressure holds every queue full, and 2 tasks (100/s) would only keep pace.
    _, client, store, incidents, _ = agent("p9-clickstream")
    incident = incidents["S24"]
    Poller(client, store).backfill("p9-clickstream", fleet.pack("p9-clickstream"))     # as the app does
    shown = readings.render(readings.for_incident(incident, store, client.now()))

    assert "- S24 Split Clicks: 50/s out, up to 100/s in the last 2 h: flat" in shown.splitlines()
    [short] = problems(client, store, incident, [tasks("S24 Sessionize", 2)])
    assert "2 task(s) every 20 ms is about 100/s, within 10% of the intake of about 100/s" in short
    assert problems(client, store, incident, [tasks("S24 Sessionize", 3)]) == []


def test_a_held_fan_out_is_read_through(agent):
    # S07: 0.1/s from the source, each copied 100 times plus the original, so 10/s reach the sink. Back
    # pressure holds the fan-out, which shows less than that, and its own input queue fills behind it.
    _, client, store, incidents, _ = agent("p2-backpressure")
    incident = incidents["S07"]
    Poller(client, store).backfill("p2-backpressure", fleet.pack("p2-backpressure"))
    plan = executor.plan(client, incident, [schedule("S07 Sink", "90 ms")], store)
    inv = inventory(plan["_definition"], incident["process_group"])
    rows = {(r["kind"], r["component"], r["metric"]): r
            for r in readings.for_incident(incident, store, client.now())}

    assert [(s["name"], copies) for s, copies in capacity._sources(inv, rows, "S07 Sink")] == [("S07 Source", 101)]
    assert round(capacity._intake(inv, rows, "S07 Sink", 1)) == 10
    assert capacity._behind(inv, rows, "S07 Sink") == 101 * rows[("connection", "s07-to-fanout", "queued_count")]["latest"]


def test_a_fix_must_clear_what_waits_upstream(agent):
    # S07: the sink's queue stays at its limit until the FlowFiles behind the fan-out are through.
    _, client, store, incidents, _ = agent("p2-backpressure")
    incident = incidents["S07"]
    Poller(client, store).backfill("p2-backpressure", fleet.pack("p2-backpressure"))

    # Short of the intake or not, the one message names the rate that also clears the backlog.
    for value in ("90 ms", "50 ms"):
        [found] = problems(client, store, incident, [schedule("S07 Sink", value)])
        assert found.startswith(f"S07 Sink: 1 task(s) every {value} is about ") \
            and " the intake of about 10/s. About " in found
        assert "FlowFiles also wait upstream of its full input queue" in found
        assert "To clear them within 15 min it needs about" in found
    assert problems(client, store, incident, [schedule("S07 Sink", "0 sec")]) == []


def test_a_burst_is_not_an_intake(agent):
    # S04: 600 FlowFiles every 10 minutes, 1/s on average, with no back pressure on the way. The burst shows
    # as up to 10/s, and between bursts the queue dips under its alert with FlowFiles still waiting.
    world, client, store, incidents, _ = agent("p1-throughput")
    poller = Poller(client, store, on_incident=lambda i: incidents.setdefault(i["scenario"], i))
    poller.backfill("p1-throughput", fleet.pack("p1-throughput"))
    found, shown = [], ""

    for _ in range(20):
        world.skip(60)
        poller.poll_now()

        if "S04" in incidents:
            for steps in ([tasks("S04 Drain", 4)], [schedule("S04 Drain", "250 ms")]):
                found += problems(client, store, incidents["S04"], steps)

            plan = executor.plan(client, incidents["S04"], [tasks("S04 Drain", 4)], store)
            inv = inventory(plan["_base_definition"], incidents["S04"]["process_group"])
            shown += readings.render(readings.for_incident(incidents["S04"], store, client.now()), inv)

    assert "S04" in incidents and found == []
    assert "/s on average" in shown


def test_quiet_without_a_measure(agent):
    _, client, store, incidents, _ = agent("p9-clickstream", "p12-warehouse")

    # Without the split's own rate, the source's is not what reaches the sessionizer.
    plan = executor.plan(client, incidents["S24"], [tasks("S24 Sessionize", 2)], store)
    assert capacity._sources(inventory(plan["_definition"], incidents["S24"]["process_group"]), {},
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
