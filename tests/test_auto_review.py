"""
The answer-key reviewer (runtime/auto_review.py, NFH-116): each review is
decided as the Scorecard grades it, through the Review Queue's own paths,
and a playbook draft that passes its checks is approved.
"""

import re

import pytest

import config
from flows import packs
from nifi_healer.agent import graph
from nifi_healer.runtime import auto_review as ar
from nifi_healer.ui import gradio_app, legend, views

from test_agent_graph import S01_FIX, ScriptedLLM, agent, proposal  # noqa: F401 (agent is a fixture)
from test_playbook_edits import CPU, SECTION, drafting, edit_reply, settle  # noqa: F401 (drafting is a fixture)

S = packs.SCENARIOS
WRONG = {"action": "SET_PENALTY_YIELD", "target": "S02 Drain", "yield_duration": "3 sec"}


@pytest.fixture(autouse=True)
def fresh():
    ar.reset()
    yield
    ar.reset()


def test_each_proposal_gets_the_scorecards_verdict():
    tasks = lambda target, value: {"action": "SET_CONCURRENT_TASKS", "target": target, "value": value}  # noqa: E731

    assert ar.verdict(list(S01_FIX), S["S01"]) == (ar.APPROVE, "the expected fix")
    assert ar.verdict(list(S["S01"].acceptable), S["S01"]) == (ar.APPROVE, "the acceptable fix")
    assert ar.verdict([tasks("S01 Consumer", 8)], S["S01"])[0] == ar.APPROVE       # beyond the design is tuning
    assert ar.verdict([tasks("S01 Consumer", 2)], S["S01"]) == (ar.REJECT, ar.SHORT)
    assert ar.verdict([tasks("S19 Consumer", 2)], S["S19"]) == (ar.REJECT, ar.NOTHING)
    assert ar.verdict([S["S10"].expected[1]], S["S10"]) == (ar.REJECT, ar.PART)
    assert ar.verdict([dict(WRONG, target="S01 Consumer")], S["S01"]) == (ar.REJECT, ar.LEVER)
    assert ar.verdict([{"action": "SCALE_DEPLOYMENT", "cluster_size": "MEDIUM", "nodes": 2}], S["S01"]) == \
        (ar.REJECT, ar.KIND)
    assert ar.verdict([tasks("S01 Source", 4)], S["S01"]) == (ar.REJECT, ar.COMPONENT)
    assert ar.verdict([], S["S01"]) == (ar.REJECT, ar.KIND)                          # nothing is NO_ACTION


def test_no_reason_gives_the_design_fix_away():
    # The reason is what memory and the playbook drafter read: no component, action or value of any design.
    reasons = (ar.NOTHING, ar.PART, ar.SHORT, ar.LEVER, ar.KIND, ar.COMPONENT)
    designs = [f for s in S.values() for f in (*s.expected, *s.acceptable)]

    for reason in reasons:
        assert not re.search(r"\bS\d{2,3}\b|\d", reason), reason
        assert not any(f.get("target") and f["target"] in reason for f in designs)
        assert not any(f["action"] in reason for f in designs)


def test_a_tick_decides_every_review_and_approves_the_draft(drafting, monkeypatch, capsys):
    _, client, store, incidents, install = drafting("p1-throughput")
    monkeypatch.setattr(config, "ARMED", False)
    monkeypatch.setattr(config, "AUTO_REVIEW", True)
    s01, s02 = incidents["S01"]["id"], incidents["S02"]["id"]
    install(ScriptedLLM(proposal(S01_FIX), proposal([WRONG], diagnosis="S02 Drain yields too long", cite=[SECTION]),
                        edit_reply(CPU)))
    graph.start(s01)
    graph.start(s02)

    gradio_app._tick(client, store)               # as the poller does
    settle()
    assert store.incident(s01)["status"] == "DRY_RUN"
    assert store.incident(s02)["status"] == "REJECTED"
    [edit] = store.playbook_edits()
    assert edit["status"] == "PROPOSED" and edit["incident_id"] == s02
    [review] = store.reviews(s02)
    assert (review["decision"], review["reviewer"], review["reason"]) == ("reject", ar.REVIEWER, ar.LEVER)

    ar.tick(client, store)
    assert store.playbook_edit(edit["id"])["status"] == "APPLIED"
    assert store.playbook_edit(edit["id"])["reviewer"] == ar.REVIEWER

    ar.tick(client, store)
    ar.tick(client, store)
    out = capsys.readouterr().out
    assert f"{s01} (S01): approve, the expected fix:" in out
    assert f"{s02} (S02): reject, {ar.LEVER}" in out
    assert f"PE-{edit['id']}: Approved PE-{edit['id']}" in out
    assert out.count("[auto-review] done:") == 1


def test_a_review_it_cannot_grade_is_left_for_a_person(agent, monkeypatch, capsys):
    _, client, store, incidents, install = agent("p1-throughput")
    monkeypatch.setattr(config, "ARMED", False)
    s01 = incidents["S01"]["id"]
    install(ScriptedLLM(proposal(S01_FIX)))
    graph.start(s01)
    store.update_incident(s01, 0, scenario="S999")

    ar.tick(client, store)
    ar.tick(client, store)

    assert store.incident(s01)["status"] == "PENDING_REVIEW" and graph.pending(s01)
    assert capsys.readouterr().out.count("no design label for S999; left for a person") == 1


def test_armed_it_approves_only_when_told_to(agent, monkeypatch, capsys):
    _, client, store, incidents, install = agent("p1-throughput")         # armed
    monkeypatch.setattr(config, "AUTO_REVIEW_ARMED", False)
    s01 = incidents["S01"]["id"]
    install(ScriptedLLM(proposal(S01_FIX)))
    graph.start(s01)

    ar.tick(client, store)
    assert store.incident(s01)["status"] == "PENDING_REVIEW"
    assert "but writes are armed: left for a person" in capsys.readouterr().out


def test_the_system_tab_says_whether_it_is_on(monkeypatch):
    monkeypatch.setattr(config, "AUTO_REVIEW", False)
    assert views._auto_review_state().startswith("off (SELFHEAL_AUTO_REVIEW=true")

    monkeypatch.setattr(config, "AUTO_REVIEW", True)
    assert "approves only while writes are disarmed" in views._auto_review_state()
    assert "**Answer-key reviewer**" in legend.SYSTEM
