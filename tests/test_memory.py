"""
Remediation memory (rag/memory.py): judged and rejected cases become
evidence in later diagnoses, on the app's store and across replay rounds.
"""

import json
import re

import pytest

import config
from flows import packs
from nifi_healer import llm
from nifi_healer.agent import graph, prompts
from nifi_healer.eval import replay, scorecard as sc
from nifi_healer.rag import ingest, memory, retriever
from nifi_healer.runtime.poller import Poller
from nifi_healer.ui import views


class FixLLM:
    """Proposes the scenario's design fix, citing every case id it was shown; records each user prompt."""

    model_name = "fixer"

    def __init__(self, steps=None):
        self.steps = steps
        self.prompts = []

    def invoke(self, messages):
        text = prompts.text_of(messages[1])
        self.prompts.append(text)
        scenario = packs.SCENARIOS[re.search(r"\bINC-[\d-]+-(S\d\d)-", text).group(1)]

        return type("Reply", (), {"content": json.dumps({
            "diagnosis": f"{scenario.group_name} is at fault", "root_cause_component": "",
            "steps": self.steps if self.steps is not None else list(scenario.expected),
            "rationale": "it fixes the fault", "citations": re.findall(r"^\[(case:[^\]]+)\]", text, re.M),
            "confidence": 0.8, "expected_effect": "the KPIs recover"})})()


@pytest.fixture
def fleet(tenant, monkeypatch):
    llm.set_embedder(llm.HashEmbedder())
    ingest.reset_client()
    retriever.invalidate()
    graph.reset()
    monkeypatch.setattr(config, "ARMED", True)
    monkeypatch.setattr(config, "MEMORY", True)

    def build(*names):
        world, client, store = tenant(*names, history=3600)
        opened = []
        Poller(client, store, on_incident=opened.append).poll_now()

        return world, client, store, {i["scenario"]: i for i in opened}

    yield build

    graph.reset()
    llm.set_chat_model(None)
    llm.set_embedder(None)
    ingest.reset_client()
    retriever.invalidate()


def _judged(world, client, store, incident):
    graph.start(incident["id"])
    graph.resume(incident["id"], {"decision": "approve", "reviewer": "test"})
    world.skip(config.VERIFY_SETTLE_SECONDS + config.VERIFY_WINDOW_SECONDS + 120)
    Poller(client, store).evaluate_outcomes()


def test_judged_and_rejected_cases_reach_the_next_diagnosis(fleet):
    world, client, store, incidents = fleet("p1-throughput")
    model = FixLLM()
    llm.set_chat_model(model)

    _judged(world, client, store, incidents["S01"])
    graph.start(incidents["S02"]["id"])
    graph.resume(incidents["S02"]["id"], {"decision": "reject", "reviewer": "test",
                                          "reason": "the drain schedule is set by the partner"})

    assert "(none)" in model.prompts[0]                       # nothing judged before S01
    found = {l["incident"]: l for l in memory.lessons(store.record())}

    assert set(found) == {incidents["S01"]["id"], incidents["S02"]["id"]}       # S03 is not judged yet
    s01, s02 = found[incidents["S01"]["id"]], found[incidents["S02"]["id"]]
    assert s01["outcome"] == sc.WORKED and "Reviewer approved it as proposed" in s01["text"]
    assert "15 min IMPROVED" in s01["text"] and "SET_CONCURRENT_TASKS 'S01 Consumer'" in s01["text"]
    assert s02["outcome"] == "rejected" and "the drain schedule is set by the partner" in s02["text"]

    graph.start(incidents["S03"]["id"])
    review = graph.pending(incidents["S03"]["id"])
    shown = {h["id"] for h in review["memory"]}

    assert shown == {f"case:{incidents['S01']['id']}", f"case:{incidents['S02']['id']}"}
    assert f"[case:{incidents['S01']['id']}]" in model.prompts[-1]
    assert review["plan"]["warnings"] == []                   # a cited case id is a known source
    assert "Past cases shown to the agent" in views.review(store, client, incidents["S03"]["id"])["proposal"]


def test_memory_off_shows_no_cases(fleet, monkeypatch):
    world, client, store, incidents = fleet("p1-throughput")
    model = FixLLM()
    llm.set_chat_model(model)
    _judged(world, client, store, incidents["S01"])
    monkeypatch.setattr(config, "MEMORY", False)

    graph.start(incidents["S03"]["id"])

    assert memory.recall(incidents["S03"]) == [] and graph.pending(incidents["S03"]["id"])["memory"] == []
    assert "case:" not in model.prompts[-1]
    assert store.proposals(incidents["S03"]["id"])[-1]["agent_version"].endswith("/model:fixer/no-memory")


def test_sync_embeds_only_new_cases(fleet):
    world, client, store, incidents = fleet("p1-throughput")
    llm.set_chat_model(FixLLM())
    _judged(world, client, store, incidents["S01"])

    assert memory.sync(store) == 1 and memory.sync(store) == 0

    graph.start(incidents["S02"]["id"])
    graph.resume(incidents["S02"]["id"], {"decision": "reject", "reviewer": "test", "reason": "not now"})

    assert memory.sync(store) == 1
    assert ingest.collection(memory.COLLECTION).count() == 2


def test_a_value_is_shown_only_once_validated():
    step = {"action": "UPDATE_PARAMETER", "target": "ctx", "parameter": "s17.token", "value": "hunter2"}

    assert "hunter2" in memory._steps({"action": [step], "validation": {"ok": True}})
    assert memory._steps({"action": [step], "validation": {"ok": False}}) == "UPDATE_PARAMETER 'ctx' parameter=s17.token"


@pytest.fixture
def replayed(tenant, tmp_path, monkeypatch):
    for name in replay.ISOLATED:
        monkeypatch.setattr(config, name, getattr(config, name))

    monkeypatch.setattr(config, "REPLAY_DIR", str(tmp_path / "replay"))
    llm.set_embedder(llm.HashEmbedder())

    yield config.REPLAY_DIR

    graph.reset()
    llm.set_chat_model(None)
    llm.set_embedder(None)
    ingest.reset_client()
    retriever.invalidate()


def test_replay_rounds_learn_from_earlier_ones(replayed):
    replay.run(["S01"], oracle=True)

    assert memory.lessons(replay.load()["record"]) == []      # the oracle's fixes are the design labels

    model = FixLLM()
    llm.set_chat_model(model)
    replay.run(["S01"], trials=2)

    assert "(none)" in model.prompts[0] and "[case:INC-" in model.prompts[-1]

    replay.run(["S01"], memory=False)
    snapshot = replay.load()
    found = replay.cases(snapshot)

    assert "case:" not in model.prompts[-1] and snapshot["runs"][-1]["memory"] is False
    assert snapshot["runs"][-1]["agent_version"].endswith("/no-memory")
    assert any(c["run"].endswith(" · no memory") for c in found)
    assert {name for name, _ in sc.table(found, "Replay round")} == {"round 1", "round 2"}
