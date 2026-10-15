"""
Graduated autonomy (agent/autonomy.py): a low-risk flow edit that has
worked on its process group often enough, with a person's approval,
approves itself after a veto window; any person can approve, reject or
hold it first, and anything a guardrail says at the end hands it back.

A fix's past on S01's group is planted (`past`): the mock opens one S01
incident per tenant, and earning trust needs several judged ones.
"""

import json

import gradio as gr
import pytest

import config
from nifi_healer import llm
from nifi_healer.agent import autonomy, graph
from nifi_healer.eval import scorecard as sc
from nifi_healer.rag import priors
from nifi_healer.runtime.poller import Poller
from nifi_healer.store import db
from nifi_healer.ui import gradio_app, views

from test_memory import FixLLM, fleet  # noqa: F401 (fixture)

FIX = "SET_CONCURRENT_TASKS 'S01 Consumer'"


def _try(outcome=sc.WORKED, value=4, auto=False, n=0, group="S01 Slow Consumer"):
    return {"incident": f"INC-past-{n}", "pack": "p1-throughput", "process_group": group,
            "symptom": "Flow Files Queued over its threshold", "group_fix": FIX,
            "fleet_fix": "SET_CONCURRENT_TASKS", "outcome": outcome, "auto": auto,
            "steps": [{"action": "SET_CONCURRENT_TASKS", "target": "S01 Consumer", "value": value}]}


def _stand(*found):
    return autonomy.standing(None, "p1-throughput", "S01 Slow Consumer", FIX, found=list(found))


@pytest.fixture
def trusted(fleet, monkeypatch):  # noqa: F811
    """(world, client, store, s01, futures) with `wins` human-approved wins of S01's design fix planted."""

    monkeypatch.setattr(config, "AUTONOMY", True)
    monkeypatch.setattr(config, "AUTONOMY_AFTER", 3)
    monkeypatch.setattr(config, "AUTONOMY_VETO_SECONDS", 600)
    futures = []
    background = graph.resume_in_background
    monkeypatch.setattr(graph, "resume_in_background",
                        lambda incident_id, decision: futures.append(background(incident_id, decision)) or futures[-1])

    def build(wins=3, values=(4,), steps=None):
        world, client, store, incidents = fleet("p1-throughput")
        s01 = incidents["S01"]
        past = [_try(value=values[n % len(values)], n=n, group=s01["process_group"]) for n in range(wins)]
        real = priors.tries
        monkeypatch.setattr(priors, "tries", lambda record, skip="", pending=False:
                            [t for t in past if t["incident"] != skip] + real(record, skip, pending))
        llm.set_chat_model(FixLLM(steps))

        return world, client, store, s01, futures

    yield build

    for future in futures:  # an approval still applying would write into the next test's tenant
        future.result(timeout=60)


# ---------------------------------------------------------------------
# The trust rule
# ---------------------------------------------------------------------


def test_trust_is_earned_by_wins_a_person_approved_and_lost_on_any_miss(monkeypatch):
    monkeypatch.setattr(config, "AUTONOMY_AFTER", 3)
    wins = [_try(n=n) for n in range(3)]

    assert _stand(*wins[:2])["trusted"] is False and _stand(*wins)["trusted"] is True
    assert _stand(*wins)["values"] == {FIX: {"value": [4]}}

    for miss in ("rejected", sc.NO_HELP, sc.WORSE):           # WRONG_FIX is counted as NO_HELP
        stand = _stand(*wins, _try(miss))
        assert (stand["trusted"], stand["wins"], stand["broken_by"]) == (False, 0, miss)

    # A run of wins counts from the last miss; a pending try is not judged yet.
    assert _stand(*wins, _try(sc.NO_HELP), *wins[:2])["trusted"] is False
    assert _stand(*wins, _try(sc.PENDING))["trusted"] is True

    # A win it approved itself keeps the trust but never earns it, and adds no approved value.
    kept = _stand(*wins, _try(auto=True, value=99))
    assert (kept["trusted"], kept["wins"], kept["human_wins"]) == (True, 4, 3)
    assert kept["values"] == {FIX: {"value": [4]}}
    assert _stand(*wins[:2], _try(auto=True), _try(auto=True))["trusted"] is False


def test_values_must_be_ones_a_person_approved_or_between_them():
    approved = {FIX: {"value": [4, 8]}}
    step = {"action": "SET_CONCURRENT_TASKS", "target": "S01 Consumer"}

    assert autonomy.outside([dict(step, value=4)], approved) == []
    assert autonomy.outside([dict(step, value=6)], approved) == []
    assert autonomy.outside([dict(step, value=32)], approved) == [
        f"{FIX} value=32 is not a value a person approved here (approved: 4, 8)"]

    schedule = {"SET_RUN_SCHEDULE 'S02 Drain'": {"value": ["1 sec", "5 sec"]}}
    drain = {"action": "SET_RUN_SCHEDULE", "target": "S02 Drain"}
    assert autonomy.outside([dict(drain, value="2 sec")], schedule) == []
    assert autonomy.outside([dict(drain, value="1 min")], schedule)
    assert autonomy.outside([dict(drain, value="1 sec", concurrent=2)], schedule) == [
        "SET_RUN_SCHEDULE 'S02 Drain' concurrent=2 is not a value a person approved here (approved: none)"]


def test_only_low_risk_flow_edits_qualify():
    assert autonomy.ELIGIBLE == {"SET_CONCURRENT_TASKS", "SET_RUN_SCHEDULE", "SET_BACKPRESSURE",
                                 "SET_PENALTY_YIELD", "SET_RETRY_POLICY"}
    assert autonomy._eligible([{"action": "SCALE_DEPLOYMENT"}, {"action": "UPDATE_PARAMETER"},
                               {"action": "SET_PROCESSOR_PROPERTY"}, {"action": "SET_RETRY_POLICY"}]) == \
        ["SCALE_DEPLOYMENT", "SET_PROCESSOR_PROPERTY", "UPDATE_PARAMETER"]


# ---------------------------------------------------------------------
# End to end
# ---------------------------------------------------------------------


def test_a_trusted_fix_approves_itself_when_its_window_ends(trusted):
    world, client, store, s01, futures = trusted()
    graph._run(s01["id"])

    window = store.auto_approval(s01["id"])
    assert window["status"] == autonomy.WAITING and window["fix"] == FIX
    assert window["due_at"] == pytest.approx(client.now() + 600, abs=5)
    assert "worked the last 3 time(s)" in window["basis"]
    assert db.Store(config.DB_PATH).auto_approval(s01["id"])["status"] == autonomy.WAITING  # kept over a restart

    view = views.review(store, client, s01["id"])
    assert "**Trusted fix: approves itself at" in view["proposal"] and view["can_hold"] and view["can_approve"]
    assert "Autonomy:** waits" not in view["proposal"]

    # Not due yet: nothing happens.
    autonomy.tick(client, store)
    assert store.auto_approval(s01["id"])["status"] == autonomy.WAITING and not futures

    world.skip(601)
    autonomy.tick(client, store)
    futures[0].result(timeout=60)

    assert store.auto_approval(s01["id"])["status"] == autonomy.APPROVED
    assert [(r["decision"], r["reviewer"]) for r in store.reviews(s01["id"])] == [("approve", autonomy.REVIEWER)]
    assert store.actions(incident_id=s01["id"])[-1]["status"] == "APPLIED"
    assert "This trusted fix approved itself" in views.review(store, client, s01["id"])["proposal"]
    assert "**Veto window:** approved" in views.incident_detail(store, s01["id"])

    # One self-approved run of a fix at a time, until it is measured.
    payload = {"plan": {"ok": True, "steps": [dict(_try()["steps"][0])], "warnings": []}}
    assert any("still being measured" in b for b in autonomy.blockers(store, client, s01, payload))

    world.skip(config.VERIFY_SETTLE_SECONDS + config.VERIFY_WINDOW_SECONDS + 120)
    Poller(client, store).evaluate_outcomes()
    case = next(c for c in sc.cases(store.record()) if c["incident"] == s01["id"])

    assert (case["review"], case["auto"], case["result"]) == (sc.AUTO, True, sc.WORKED)
    assert sc.summarize([case])["approved"] is None                    # not a person's approval
    assert autonomy.standing(store, s01["pack"], s01["process_group"], FIX)["wins"] == 4
    assert not any("still being measured" in b for b in autonomy.blockers(store, client, s01, payload))


def test_without_enough_wins_it_waits_for_a_person_and_says_why(trusted):
    world, client, store, s01, futures = trusted(wins=2)
    graph._run(s01["id"])

    assert store.auto_approval(s01["id"]) is None
    view = views.review(store, client, s01["id"])
    assert ("**Autonomy:** waits for a person: this fix has worked 2 time(s) in a row on this process group, "
            "2 of them approved by a person; at 3 it approves itself.") in view["proposal"]
    assert not view["can_hold"]


def test_a_value_no_person_approved_waits_for_a_person(trusted):
    world, client, store, s01, futures = trusted(
        values=(2,), steps=[{"action": "SET_CONCURRENT_TASKS", "target": "S01 Consumer", "value": 4}])
    graph._run(s01["id"])

    assert store.auto_approval(s01["id"]) is None
    assert "value=4 is not a value a person approved here (approved: 2)" in \
        views.review(store, client, s01["id"])["proposal"]


def test_what_always_waits_for_a_person(trusted, monkeypatch):
    world, client, store, s01, futures = trusted()
    graph._run(s01["id"])
    payload = graph.pending(s01["id"])

    assert autonomy.blockers(store, client, s01, payload) == []

    def first(incident=s01, **changes):
        return autonomy.blockers(store, client, incident, dict(payload, **changes))

    assert first(plan=dict(payload["plan"], warnings=["w"])) == ["the review has a warning: w"]
    assert first(plan=dict(payload["plan"], ok=False)) == ["the proposal failed validation"]
    assert first(plan=dict(payload["plan"], steps=[{"action": "SCALE_DEPLOYMENT", "size": "LARGE"}])) == \
        ["only low-risk flow edits approve themselves, not SCALE_DEPLOYMENT"]
    assert "a rollback always waits for a person" in first(dict(s01, kind="rollback"))

    monkeypatch.setattr(config, "ARMED", False)
    assert first() == ["writes are disarmed, so an approval is only a dry run"]
    monkeypatch.setattr(config, "ARMED", True)

    autonomy.pause(store, "alice", client.now())
    assert first() == ["autonomy is paused (by alice)"]
    autonomy.unpause(store, "alice", client.now())

    monkeypatch.setattr(config, "AUTONOMY", False)
    assert first() == ["autonomy is off (SELFHEAL_AUTONOMY)"]
    monkeypatch.setattr(config, "AUTONOMY", True)

    # A person's edit of the steps is theirs to approve.
    steps = views.review(store, client, s01["id"])["steps"]
    assert "validated" in views.decide(s01["id"], "edit", "alice", "", steps)
    assert "these steps are a person's edit" in autonomy.blockers(store, client, s01, graph.pending(s01["id"]))


# ---------------------------------------------------------------------
# The veto window
# ---------------------------------------------------------------------


def test_a_rejection_in_the_window_cancels_it_and_takes_the_trust_away(trusted):
    world, client, store, s01, futures = trusted()
    graph._run(s01["id"])

    assert "needs a reason" in views.decide(s01["id"], "reject", "alice", "  ")
    assert store.auto_approval(s01["id"])["status"] == autonomy.WAITING

    assert views.decide(s01["id"], "reject", "alice", "not during the partner's batch").endswith("rejected.")
    window = store.auto_approval(s01["id"])
    assert (window["status"], window["decided_by"], window["reason"]) == \
        (autonomy.CANCELLED, "alice", "reject by a person")

    world.skip(601)
    autonomy.tick(client, store)
    assert not futures and [r["reviewer"] for r in store.reviews(s01["id"])] == ["alice"]
    assert autonomy.standing(store, s01["pack"], s01["process_group"], FIX)["broken_by"] == "rejected"


def test_hold_keeps_the_review_for_a_person(trusted):
    world, client, store, s01, futures = trusted()
    graph._run(s01["id"])

    assert views.decide(s01["id"], "hold", "alice").startswith("Held")
    assert "no veto window open" in views.decide(s01["id"], "hold", "alice")

    world.skip(601)
    autonomy.tick(client, store)
    view = views.review(store, client, s01["id"])

    assert store.auto_approval(s01["id"])["status"] == autonomy.HELD and not futures
    assert graph.pending(s01["id"]) and "**Held for review** by alice" in view["proposal"]
    assert not view["can_hold"] and view["can_approve"]

    # A person approves it in their own time, as any review.
    assert "Approved; applying" in views.decide(s01["id"], "approve", "alice")
    futures[0].result(timeout=60)
    assert [r["reviewer"] for r in store.reviews(s01["id"])] == ["alice"]


def test_a_guardrail_at_the_end_hands_it_back(trusted, monkeypatch):
    world, client, store, s01, futures = trusted()
    graph._run(s01["id"])
    monkeypatch.setattr(autonomy.executor, "precheck", lambda *a: "S01 is in its 15 min cooldown.")

    world.skip(601)
    autonomy.tick(client, store)
    window = store.auto_approval(s01["id"])

    assert (window["status"], window["reason"]) == (autonomy.HANDED_BACK, "S01 is in its 15 min cooldown")
    assert not futures and graph.pending(s01["id"]) and store.reviews(s01["id"]) == []
    assert "**Autonomy handed this to you** at" in views.review(store, client, s01["id"])["proposal"]


def test_pausing_hands_back_what_is_due_and_opens_nothing(trusted):
    world, client, store, s01, futures = trusted()
    graph._run(s01["id"])

    assert views.set_autonomy(True, "alice").startswith("Autonomy paused")
    assert "**paused** by alice" in views.system(gradio_app._snapshot(), client)

    world.skip(601)
    autonomy.tick(client, store)
    assert store.auto_approval(s01["id"])["reason"] == "autonomy is paused (by alice)" and not futures

    assert autonomy.consider(s01["id"]) is False                      # one window per incident, ever
    views.set_autonomy(False, "alice")
    assert "**on**" in views.system(gradio_app._snapshot(), client)


def test_a_click_and_the_tick_at_once_only_one_wins(trusted, monkeypatch):
    world, client, store, s01, futures = trusted()
    graph._run(s01["id"])
    cancel = autonomy.cancel

    def tick_first(*args):
        assert store.claim_auto_approval(s01["id"], autonomy.APPROVED, client.now(), decided_by=autonomy.REVIEWER)

        return cancel(*args)

    # The tick claims the window between the reviewer's read and their claim; the reject is not sent.
    monkeypatch.setattr(autonomy, "cancel", tick_first)
    assert "approved itself before your reject" in views.decide(s01["id"], "reject", "alice", "no")
    assert store.reviews(s01["id"]) == [] and graph.pending(s01["id"])
    assert not store.claim_auto_approval(s01["id"], autonomy.HELD, client.now())


# ---------------------------------------------------------------------
# The System tab and who may switch it
# ---------------------------------------------------------------------


def test_the_system_tab_lists_trusted_fixes_and_viewers_cannot_switch_or_hold(trusted):
    world, client, store, s01, futures = trusted()
    shown = views.system(gradio_app._snapshot(), client)

    assert f"| Trusted fixes | p1-throughput › {s01['process_group']}: `{FIX}` (3 win(s), 3 approved" in shown
    assert isinstance(gradio_app.build(), gr.Blocks)

    class Request:
        def __init__(self, **headers):
            self.headers = headers

    viewer = Request(**{"remote-user": "guest", "remote-user-perm": "RO"})
    assert "can only view" in gradio_app._autonomy_switch(True)(request=viewer)[0]
    assert autonomy.paused(store) is None

    graph._run(s01["id"])
    out = gradio_app._decide("hold")(s01["id"], "", "", "", request=viewer)
    assert "can only view" in out[0] and store.auto_approval(s01["id"])["status"] == autonomy.WAITING
    assert len(gradio_app.show_review(s01["id"])) == gradio_app.PANEL_OUTPUTS

    owner = Request(**{"remote-user": "pdefusco", "remote-user-perm": "RW"})
    assert gradio_app._autonomy_switch(True)(request=owner)[0].startswith("Autonomy paused")
    assert autonomy.paused(store)["updated_by"] == "pdefusco"


def test_autonomy_off_says_how_to_turn_it_on(trusted, monkeypatch):
    world, client, store, s01, futures = trusted()
    monkeypatch.setattr(config, "AUTONOMY", False)
    graph._run(s01["id"])

    assert store.auto_approval(s01["id"]) is None
    assert "Autonomy:**" not in views.review(store, client, s01["id"])["proposal"]
    assert "off (set SELFHEAL_AUTONOMY=true" in views.system(gradio_app._snapshot(), client)
    assert "Autonomy is off" in views.set_autonomy(True, "alice") and autonomy.paused(store) is None
    assert json.dumps(autonomy.trusted_fixes(store))                  # still listed while off
