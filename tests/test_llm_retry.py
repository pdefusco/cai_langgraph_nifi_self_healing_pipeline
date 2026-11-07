"""A diagnosis the LLM didn't answer is tried again, on a backoff live and within the round in the replay (NFH-68)."""

import threading
import time

import pytest

import config
from nifi_healer import llm
from nifi_healer.agent import autonomy, graph, nodes
from nifi_healer.eval import replay, scorecard as sc
from nifi_healer.ui import gradio_app, views

from test_agent_graph import S01_FIX, ScriptedLLM, agent, proposal  # noqa: F401 (agent is a fixture)
from test_replay import replayed  # noqa: F401 (a fixture)


def timeout(messages):
    raise TimeoutError("Request timed out.")


def drain():
    for future in list(graph._waiting.values()):
        future.result()


def test_a_timed_out_diagnosis_is_tried_again_later(agent):
    _, client, store, incidents, install = agent("p1-throughput")
    incident = incidents["S01"]
    install(ScriptedLLM(timeout, timeout, proposal(S01_FIX)))

    state = graph.start(incident["id"])
    first = graph.retry_at(incident["id"])

    assert state["error"] == "LLM call failed: Request timed out."
    assert store.incident(incident["id"])["status"] == "NEW" and graph.llm_failures(incident["id"]) == 1
    assert first == pytest.approx(time.time() + 60 * config.LLM_RETRY_MINUTES[0], abs=5)
    assert "tries again by itself at " + views.clock(first) in views.review(store, client, incident["id"])["proposal"]

    # Not before its wait is over; then once, and the wait grows.
    assert graph.retry_failed(now=first - 1) == 0
    assert graph.retry_failed(now=first) == 1
    drain()
    second = graph.retry_at(incident["id"])

    assert graph.llm_failures(incident["id"]) == 2
    assert second == pytest.approx(time.time() + 60 * config.LLM_RETRY_MINUTES[1], abs=5)

    assert graph.retry_failed(now=second) == 1
    drain()

    assert store.incident(incident["id"])["status"] == "PENDING_REVIEW" and graph.pending(incident["id"])
    assert graph.llm_failures(incident["id"]) == 0 and graph.retry_at(incident["id"]) is None
    assert graph.retry_failed(now=second + 3600) == 0


def test_a_run_that_raises_goes_back_to_new_with_why_and_is_tried_again(agent, monkeypatch):
    """Live, S50 on p22 stayed DIAGNOSING with nothing running it and a panel saying it was diagnosing (NFH-101)."""

    _, client, store, incidents, install = agent("p1-throughput")
    incident = incidents["S01"]
    install(ScriptedLLM(proposal(S01_FIX)))
    plan = nodes.executor.plan
    monkeypatch.setattr(nodes.executor, "plan", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))

    graph.submit(incident).result()
    due = graph.retry_at(incident["id"])
    panel = views.review(store, client, incident["id"])

    assert store.incident(incident["id"])["status"] == "NEW" and graph.run_failure(incident["id"]) == "RuntimeError: boom"
    assert "**The last run stopped:** RuntimeError: boom" in panel["proposal"] and panel["can_diagnose"]
    assert due == pytest.approx(time.time() + 60 * config.LLM_RETRY_MINUTES[0], abs=5)

    # The retry continues at the step that raised: the LLM is not asked again.
    monkeypatch.setattr(nodes.executor, "plan", plan)
    assert graph.retry_failed(now=due) == 1
    drain()

    assert store.incident(incident["id"])["status"] == "PENDING_REVIEW" and graph.pending(incident["id"])
    assert graph.run_failure(incident["id"]) == "" and graph.retry_at(incident["id"]) is None


def test_a_diagnosing_incident_with_nothing_running_says_so(agent):
    _, client, store, incidents, _ = agent("p1-throughput")
    incident = incidents["S01"]
    store.update_incident(incident["id"], time.time(), status="DIAGNOSING")
    panel = views.review(store, client, incident["id"])

    assert "stopped before it proposed a fix" in panel["proposal"] and panel["can_diagnose"]


def test_the_wait_stops_growing_at_the_last_step(agent):
    _, _, _, incidents, install = agent("p1-throughput")
    incident = incidents["S01"]
    install(ScriptedLLM(*[timeout] * (len(config.LLM_RETRY_MINUTES) + 1)))

    for _ in range(len(config.LLM_RETRY_MINUTES) + 1):
        graph.start(incident["id"])

    assert graph.retry_at(incident["id"]) == pytest.approx(time.time() + 60 * config.LLM_RETRY_MINUTES[-1], abs=5)


def test_an_incident_no_longer_new_is_forgotten(agent):
    _, _, store, incidents, install = agent("p1-throughput")
    incident = incidents["S01"]
    install(ScriptedLLM(timeout))

    graph.start(incident["id"])
    store.update_incident(incident["id"], time.time(), status="NO_ACTION")

    assert graph.retry_failed(now=time.time() + 86400) == 0 and graph.retry_at(incident["id"]) is None


def test_every_kpi_poll_retries_when_auto_diagnose_is_on(monkeypatch):
    calls = []
    monkeypatch.setattr(graph, "retry_failed", lambda: calls.append("retry"))
    monkeypatch.setattr(autonomy, "tick", lambda client, store: calls.append("autonomy"))

    gradio_app._tick(None, None)
    monkeypatch.setattr(config, "AUTO_DIAGNOSE", False)
    gradio_app._tick(None, None)

    assert calls == ["retry", "autonomy", "autonomy"]


def test_the_timeout_is_a_setting(monkeypatch):
    monkeypatch.setattr(config, "LLM_TIMEOUT_SECONDS", 420)
    chat = llm._build_chat("not-a-token")

    assert chat.request_timeout == 420


class FirstCallTimesOut(replay.OracleLLM):
    """The oracle, but its first answer never comes."""

    def __init__(self):
        super().__init__()
        self.lock, self.calls = threading.Lock(), 0

    def invoke(self, messages):
        with self.lock:
            self.calls += 1
            first = self.calls == 1

        if first:
            raise TimeoutError("Request timed out.")

        return super().invoke(messages)


def test_the_replay_asks_again_within_the_round(replayed, monkeypatch):
    monkeypatch.setattr(replay, "LLM_PAUSE_SECONDS", 0)
    model = FirstCallTimesOut()
    llm.set_chat_model(model)

    replay.run(["S01", "S02"])
    found = replay.cases(replay.load())

    assert model.calls == 3
    assert sorted(c["scenario"] for c in found) == ["S01", "S02"]
    assert all(c["match"] == sc.EXPECTED and c["applied"] for c in found)
