"""
The offline replay (eval/replay.py): rounds of scenarios on an isolated
mock tenant, auto-approved there only, scored like the live agent.
"""

import json

import pytest

import config
from flows import packs
from nifi_healer import llm
from nifi_healer.agent import graph
from nifi_healer.cdp import client as clients
from nifi_healer.cdp.mock_client import MockClient
from nifi_healer.eval import replay, scorecard as sc
from nifi_healer.rag import ingest, retriever
from nifi_healer.ui import gradio_app, scorecard


@pytest.fixture
def replayed(tenant, tmp_path, monkeypatch):
    """The replay's directory; config, the world, client and store are put back afterwards."""

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


class OutOfBoundsLLM:
    """Always proposes 99 concurrent tasks, which no repair makes valid."""

    model_name = "out-of-bounds"

    def invoke(self, messages):
        return type("Reply", (), {"content": json.dumps({
            "diagnosis": "too few tasks", "root_cause_component": "S01 Consumer",
            "steps": [{"action": "SET_CONCURRENT_TASKS", "target": "S01 Consumer", "tasks": 99}],
            "rationale": "more is better", "citations": [], "confidence": 0.5, "expected_effect": "faster"})})()


def test_oracle_run_scores_the_design_fixes(replayed):
    first = replay.run(["S01", "S02", "S19"], oracle=True)
    snapshot = replay.load()
    found = replay.cases(snapshot)

    assert snapshot["runs"][0]["id"] == first and snapshot["runs"][0]["model"] == "oracle"
    assert sorted(c["scenario"] for c in found) == ["S01", "S02"]
    assert all(c["match"] == sc.EXPECTED and c["applied"] and c["result"] == sc.WORKED for c in found)
    assert {r["reviewer"] for r in snapshot["record"]["reviews"]} == {replay.REVIEWER}
    assert replay.detection(snapshot) == {"runs": 1, "faults": 2, "detected": 2, "missed": [],
                                          "controls": 1, "quiet": 1}

    # p5's other faults opened incidents too, but were not trials.
    assert any(i["scenario"] == "S17" for i in snapshot["record"]["incidents"])

    # A second run keeps the first and is told apart from it.
    replay.run(["S01"], oracle=True)
    again = replay.load()

    assert len(again["runs"]) == 2 and again["now"] >= snapshot["now"]
    assert len(sc.table(replay.cases(again), "Replay run")) == 2
    assert "S01 Slow Consumer" in replay.report(again, again["runs"][1]["id"])


def test_an_invalid_proposal_is_left_at_review(replayed):
    llm.set_chat_model(OutOfBoundsLLM())
    replay.run(["S01"])
    snapshot = replay.load()
    [case] = replay.cases(snapshot)

    assert not case["valid"] and not case["applied"] and case["review"] == sc.UNDECIDED
    assert snapshot["record"]["reviews"] == [] and snapshot["record"]["actions"] == []
    assert graph.pending(case["incident"])


def test_it_isolates_a_live_project(replayed, tmp_path):
    # A CAI project's environment: live client and CRNs, the app's data dir, disarmed.
    config.DF_CLIENT, config.MOCK, config.ARMED = "live", False, False
    config.SERVICE_CRN, config.ENVIRONMENT_CRN = "crn:cdp:df:live:service:x", "crn:cdp:environments:live:env:x"
    config.DB_PATH = str(tmp_path / "healer.db")
    replay.isolate(replayed)

    assert config.MOCK and isinstance(clients.get_client(), MockClient)
    assert (config.SERVICE_CRN, config.ENVIRONMENT_CRN) == (config.MOCK_SERVICE_CRN, config.MOCK_ENVIRONMENT_CRN)
    assert all(getattr(config, name).startswith(replayed) for name in
               ("DB_PATH", "CHECKPOINT_PATH", "FLEET_FILE", "MOCK_WORLD_FILE", "EDITS_DIR", "CHROMA_DIR"))


def test_it_approves_on_the_mock_only(replayed, monkeypatch):
    replay.isolate(replayed)
    monkeypatch.setattr(clients, "_CLIENT", object())

    with pytest.raises(RuntimeError, match="mock tenant only"):
        replay.approve("INC-20261013-000000-S01-0000")


def test_the_scorecard_shows_the_replay(replayed):
    assert "No replay runs yet" in scorecard.render(scorecard.model(None, 0.0, "All", source=scorecard.REPLAY))

    replay.run(["S01", "S19"], oracle=True)
    html = gradio_app.show_scorecard("All", "Replay run", "Replay")

    assert "Replay scorecard" in html and "Faults detected" in html and "approved automatically" in html
    assert "control quiet in 1 of 1" in html and "S01 Slow Consumer" in html
    assert "Agent scorecard" in gradio_app.show_scorecard("All", "Scenario")


def test_chosen_scenarios():
    assert replay.chosen(["S19", "S01"]) == ["S01", "S19"]
    assert replay.chosen(pack_names=["p1-throughput"]) == ["S01", "S02", "S03", "S04"]
    assert len(replay.chosen()) == len(packs.SCENARIOS) > 21     # every wave

    with pytest.raises(SystemExit):
        replay.chosen(["S99"])
