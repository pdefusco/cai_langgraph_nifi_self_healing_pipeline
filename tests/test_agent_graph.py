"""
The whole graph on a mock tenant with a scripted LLM: poller incident ->
diagnosis -> validation -> interrupt -> review -> executor -> outcome.
"""

import json
import re

import pytest

import config
from flows import packs
from nifi_healer import llm
from nifi_healer.agent import graph, prompts
from nifi_healer.rag import ingest, retriever
from nifi_healer.runtime.poller import Poller


class ScriptedLLM:
    """Replies in order; a callable reply is given the messages. Records every call."""

    model_name = "scripted-test-model"

    def __init__(self, *replies):
        self.replies = list(replies)
        self.calls = []

    def invoke(self, messages):
        self.calls.append(list(messages))
        reply = self.replies.pop(0)

        if callable(reply):
            reply = reply(messages)

        return type("Reply", (), {"content": reply if isinstance(reply, str) else json.dumps(reply)})()


def proposal(steps, diagnosis="S01 Consumer is the bottleneck: one task cannot keep up.", cite=None):
    def reply(messages):
        cited = cite if cite is not None else re.findall(r"^\[([^\]]+)\]", messages[1].content, re.M)[:1]

        return json.dumps({"diagnosis": diagnosis, "root_cause_component": "S01 Consumer",
                           "steps": list(steps), "rationale": "more consumers drain the queue",
                           "citations": cited, "confidence": 0.8,
                           "expected_effect": "s01-backlog falls within 15 minutes"})

    return reply


S01_FIX = packs.SCENARIOS["S01"].expected


@pytest.fixture
def agent(tenant, monkeypatch):
    """(world, client, store, incidents by scenario, install(llm))."""

    llm.set_embedder(llm.HashEmbedder())
    ingest.reset_client()
    retriever.invalidate()
    graph.reset()
    monkeypatch.setattr(config, "ARMED", True)

    def build(*names):
        world, client, store = tenant(*names, history=3600)
        opened = []
        Poller(client, store, on_incident=opened.append).poll_now()

        def install(model):
            llm.set_chat_model(model)

            return model

        return world, client, store, {i["scenario"]: i for i in opened}, install

    yield build

    graph.reset()
    llm.set_chat_model(None)
    llm.set_embedder(None)
    ingest.reset_client()
    retriever.invalidate()


def test_approve_applies_and_outcome_improves(agent):
    world, client, store, incidents, install = agent("p1-throughput")
    incident = incidents["S01"]
    model = install(ScriptedLLM(proposal(S01_FIX)))

    state = graph.start(incident["id"])
    review = graph.pending(incident["id"])

    assert review and review["plan"]["ok"] and review["proposal_id"] == state["proposal_id"]
    assert review["plan"]["family"] == "FLOW_EDIT" and review["plan"]["warnings"] == []
    assert store.incident(incident["id"])["status"] == "PENDING_REVIEW"
    assert "S01 Consumer" in model.calls[0][1].content          # the inventory reached the prompt
    row = store.proposals(incident["id"])[-1]
    assert row["status"] == "VALID" and row["agent_version"].startswith(f"prompt:{prompts.PROMPT_VERSION}/")

    state = graph.resume(incident["id"], {"decision": "approve", "reviewer": "test"})

    assert graph.pending(incident["id"]) is None
    assert state["result"]["status"] == "APPLIED" and not state["result"]["dry_run"]
    assert store.incident(incident["id"])["status"] == "VERIFYING"
    assert [r["decision"] for r in store.reviews(incident["id"])] == ["approve"]
    facts = client.flow_facts(incident["deployment_crn"], incident["deployed_flow_crn"])
    assert facts["flow_version_crn"] == state["result"]["to_flow_version_crn"]

    world.skip(config.VERIFY_SETTLE_SECONDS + config.VERIFY_WINDOW_SECONDS + 120)
    judged = Poller(client, store).evaluate_outcomes()

    assert [o["verdict"] for o in judged] == ["IMPROVED"]
    assert store.incident(incident["id"])["status"] == "RESOLVED"


def test_unarmed_approval_is_a_dry_run(agent, monkeypatch):
    _, client, store, incidents, install = agent("p1-throughput")
    monkeypatch.setattr(config, "ARMED", False)
    incident = incidents["S01"]
    install(ScriptedLLM(proposal(S01_FIX)))
    before = client.flow_facts(incident["deployment_crn"], incident["deployed_flow_crn"])["flow_version_crn"]

    graph.start(incident["id"])
    assert graph.pending(incident["id"])["armed"] is False
    state = graph.resume(incident["id"], {"decision": "approve"})

    assert state["result"]["status"] == "DRY_RUN" and len(state["result"]["argv"]) == 2
    assert store.incident(incident["id"])["status"] == "DRY_RUN"
    assert client.flow_facts(incident["deployment_crn"],
                             incident["deployed_flow_crn"])["flow_version_crn"] == before


def test_bad_replies_are_repaired_then_fed_back(agent):
    _, _, store, incidents, install = agent("p1-throughput")
    incident = incidents["S01"]
    model = install(ScriptedLLM(
        "I would raise the concurrency.",                                       # not JSON: repair turn
        proposal([{"action": "SET_CONCURRENT_TASKS", "target": "S02 Drain", "value": 4}]),
        proposal(S01_FIX),
    ))

    state = graph.start(incident["id"])

    assert len(model.calls) == 3 and state["attempts"] == 2
    assert "could not be used" in model.calls[1][-1].content                 # the repair prompt
    retry = model.calls[2][1].content
    assert "rejected by validation" in retry and "no processor named 'S02 Drain'" in retry
    assert [p["status"] for p in store.proposals(incident["id"])] == ["SUPERSEDED", "VALID"]
    assert graph.pending(incident["id"])["plan"]["ok"]


def test_a_reasoning_reply_is_read_after_its_thinking():
    # nemotron live: its thinking sketches the shape with placeholders, then </think>, then the answer.
    answer = {"diagnosis": "50 copies per FlowFile", "steps": [{"action": "UPDATE_PARAMETER",
              "target": "nfh-p5-parameters", "parameter": "s18.copies", "value": "1"}]}
    reply = ('We need to output JSON.\n{\n  "diagnosis": "...",\n  "steps": [...],\n  "confidence": 0.95\n}\n'
             "Make sure no trailing commas.\n</think>\n\n" + json.dumps(answer, indent=2))

    assert prompts.extract_json(reply) == answer
    assert prompts.extract_json(reply.replace("</think>", "")) == answer      # no tag: the last object wins
    assert prompts.extract_json("```json\n" + json.dumps(answer) + "\n``` and {not json}") == answer

    with pytest.raises(ValueError, match="does not parse"):
        prompts.extract_json('{"steps": [...]}')

    with pytest.raises(ValueError, match="no JSON object"):
        prompts.extract_json("I think the consumer is slow.")


def test_invalid_after_all_attempts_goes_to_review(agent):
    _, _, store, incidents, install = agent("p1-throughput")
    incident = incidents["S01"]
    wrong = proposal([{"action": "SET_CONCURRENT_TASKS", "target": "S01 Consumer", "value": 40}])
    install(ScriptedLLM(wrong, wrong, wrong))

    graph.start(incident["id"])
    review = graph.pending(incident["id"])

    assert not review["plan"]["ok"] and "1 to 8" in review["plan"]["problems"][0]

    graph.resume(incident["id"], {"decision": "approve"})
    assert "failed validation" in graph.pending(incident["id"])["error"]

    graph.resume(incident["id"], {"decision": "edit", "steps": [dict(S01_FIX[0])]})
    review = graph.pending(incident["id"])
    assert review["plan"]["ok"] and review["error"] == ""
    assert store.proposals(incident["id"])[-1]["agent_version"].endswith("+review")


def test_review_rules_edit_and_reject(agent):
    _, _, store, incidents, install = agent("p1-throughput")
    incident = incidents["S01"]
    install(ScriptedLLM(proposal(S01_FIX)))
    graph.start(incident["id"])

    graph.resume(incident["id"], {"decision": "reject"})
    assert "needs a reason" in graph.pending(incident["id"])["error"]

    graph.resume(incident["id"], {"decision": "maybe"})
    assert "must be one of" in graph.pending(incident["id"])["error"]

    edited = [dict(S01_FIX[0], value=6)]
    graph.resume(incident["id"], {"decision": "edit", "steps": edited, "reviewer": "ops"})
    review = graph.pending(incident["id"])
    assert any(d["after"] == 6 for d in review["plan"]["diff"]) and review["proposal"]["steps"] == edited

    state = graph.resume(incident["id"], {"decision": "reject", "reason": "wait for the vendor fix"})

    assert graph.pending(incident["id"]) is None and "result" not in state
    assert store.incident(incident["id"])["status"] == "REJECTED"
    assert [(r["decision"], r["reason"]) for r in store.reviews(incident["id"])] == [
        ("edit", ""), ("reject", "wait for the vendor fix")]
    assert [p["status"] for p in store.proposals(incident["id"])] == ["SUPERSEDED", "VALID"]

    with pytest.raises(ValueError, match="not waiting"):
        graph.resume(incident["id"], {"decision": "approve"})


def test_parameter_fix_through_the_graph(agent):
    _, client, store, incidents, install = agent("p5-parameters")
    incident = incidents["S17"]
    model = install(ScriptedLLM(proposal(packs.SCENARIOS["S17"].expected, diagnosis="wrong endpoint")))

    graph.start(incident["id"])
    assert graph.pending(incident["id"])["plan"]["family"] == "PARAMETER"

    # Live: S17's backlog signals retrieved only backlog and latency playbooks, never the
    # runbook row with the correct URL, and the agent changed the port but kept the dead host.
    cited = [h["id"] for h in graph.pending(incident["id"])["playbooks"]]
    assert "parameter-misconfiguration#runbook-correct-values-for-the-nfh-p5-parameters-context" in cited
    assert "`http://localhost:9876/ingest`" in model.calls[0][1].content

    state = graph.resume(incident["id"], {"decision": "approve"})
    assert state["result"]["status"] == "APPLIED"
    assert store.incident(incident["id"])["status"] == "VERIFYING"


def test_no_action_closes_the_incident(agent):
    _, _, store, incidents, install = agent("p1-throughput")
    incident = incidents["S02"]
    install(ScriptedLLM(proposal([{"action": "NO_ACTION"}], diagnosis="a transient")))

    state = graph.start(incident["id"])

    assert graph.pending(incident["id"]) is None and state["plan"]["family"] == "NONE"
    assert store.incident(incident["id"])["status"] == "NO_ACTION"
    assert store.proposals(incident["id"])[-1]["status"] == "NO_ACTION"


def test_llm_unavailable_leaves_the_incident_new(agent, monkeypatch):
    _, _, store, incidents, install = agent("p1-throughput")
    incident = incidents["S01"]

    with monkeypatch.context() as down:
        down.setattr(llm, "chat_available", lambda: (False, "no endpoint configured"))
        state = graph.start(incident["id"])

    assert "no endpoint" in state["error"] and graph.pending(incident["id"]) is None
    assert store.incident(incident["id"])["status"] == "NEW" and store.proposals(incident["id"]) == []

    # The endpoint is back: the same thread is diagnosed again.
    install(ScriptedLLM(proposal(S01_FIX)))
    state = graph.start(incident["id"])

    assert state["error"] == "" and graph.pending(incident["id"])["plan"]["ok"]


def test_closed_incidents_are_not_diagnosed_again(agent):
    _, _, store, incidents, install = agent("p1-throughput")
    incident = incidents["S02"]
    install(ScriptedLLM(proposal([{"action": "NO_ACTION"}], diagnosis="a transient")))
    graph.start(incident["id"])

    with pytest.raises(ValueError, match="NO_ACTION"):
        graph.start(incident["id"])


def test_recover_queues_new_incidents(agent):
    _, _, store, incidents, install = agent("p1-throughput")
    install(ScriptedLLM(*[proposal([{"action": "NO_ACTION"}])] * 3))    # S01, S02, S03 are NEW

    assert graph.recover() == len(incidents) == 3
    graph._executor().shutdown(wait=True)
    graph._pool = None

    assert {i["status"] for i in store.incidents()} == {"NO_ACTION"}


def test_pending_review_survives_a_restart(agent):
    _, _, store, incidents, install = agent("p1-throughput")
    incident = incidents["S01"]
    install(ScriptedLLM(proposal(S01_FIX)))

    graph.submit(incident).result(timeout=60)
    proposal_id = graph.pending(incident["id"])["proposal_id"]

    graph.reset()                                   # a new app process, same checkpoint file
    assert graph.pending(incident["id"])["proposal_id"] == proposal_id
    assert graph.start(incident["id"])["proposal_id"] == proposal_id   # no second diagnosis

    state = graph.resume(incident["id"], {"decision": "approve"})
    assert state["result"]["status"] == "APPLIED"
