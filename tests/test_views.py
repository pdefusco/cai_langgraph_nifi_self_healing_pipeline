"""
The UI's views on a mock tenant: fleet table and chart, the review panel
through a whole approve, history, system, and the Blocks layout itself.
"""

import gradio as gr

import config
from nifi_healer import llm
from nifi_healer.agent import graph
from nifi_healer.runtime.poller import Poller
from nifi_healer.ui import gradio_app, views

from test_agent_graph import S01_FIX, ScriptedLLM, agent, proposal  # noqa: F401 (agent is a fixture)


def test_fleet_views(agent):
    _, client, store, incidents, _ = agent("p1-throughput")
    poller = Poller(client, store)
    poller.poll_now()
    snapshot = poller.snapshot()

    header = views.fleet_header(snapshot)
    assert "**1** deployment(s)" in header and "breaching" in header

    rows = views.fleet_rows(snapshot, store)
    assert rows and all(len(r) == len(views.FLEET_HEADERS) for r in rows)
    assert any(r[3] == "BREACHING" and incidents["S01"]["id"] in r[7] for r in rows)

    choices = views.kpi_choices(snapshot)
    breaching = {f"p1-throughput|{k}" for k, s in snapshot["packs"]["p1-throughput"]["kpis"].items()
                 if s["status"] != "ok"}
    assert breaching and {v for _, v in choices[:len(breaching)]} == breaching

    frame = views.kpi_frame(snapshot, choices[0][1])
    assert len(frame) and set(frame["series"]) >= {"threshold"}
    assert views.kpi_frame(snapshot, None).empty


def test_review_approve_and_history(agent):
    _, client, store, incidents, install = agent("p1-throughput")
    incident = incidents["S01"]

    panel = views.review(store, client, incident["id"])
    assert "Not diagnosed yet" in panel["proposal"] and not panel["pending"]
    assert incident["id"] in [v for _, v in views.queue_choices(store)]

    install(ScriptedLLM(proposal(S01_FIX)))
    graph.start(incident["id"])
    panel = views.review(store, client, incident["id"])

    assert panel["pending"] and panel["can_approve"] and panel["status"] == "PENDING_REVIEW"
    assert "S01 Consumer" in panel["proposal"] and panel["diff"]
    assert "import-flow-definition-version" in panel["commands"]
    assert "change-flow-version-in-deployment" in panel["commands"]

    assert "not valid JSON" in views.decide(incident["id"], "edit", steps_text="[{")
    assert views.decide(incident["id"], "reject").startswith("Not accepted")
    assert "Applied" in views.decide(incident["id"], "approve", reviewer="ops")
    assert "not waiting" in views.decide(incident["id"], "approve")

    row = next(r for r in views.history_rows(store) if r[0] == incident["id"])
    assert row[3] == "VERIFYING" and "APPLIED" in row[6] and row[7] == "ops"

    detail = views.incident_detail(store, incident["id"])
    assert "**Review:** approve by ops" in detail and "cdp df change-flow-version-in-deployment" in detail


def test_edit_and_dry_run_messages(agent, monkeypatch):
    _, client, store, incidents, install = agent("p1-throughput")
    monkeypatch.setattr(config, "ARMED", False)
    incident = incidents["S01"]
    install(ScriptedLLM(proposal(S01_FIX)))
    graph.start(incident["id"])

    edited = '[{"action": "SET_CONCURRENT_TASKS", "target": "S01 Consumer", "value": 40}]'
    assert "failed validation" in views.decide(incident["id"], "edit", steps_text=edited)
    assert not views.review(store, client, incident["id"])["can_approve"]

    fixed = views.review(store, client, incident["id"])["steps"].replace("40", "6")
    assert "validated" in views.decide(incident["id"], "edit", steps_text=fixed)
    assert "disarmed" in views.review(store, client, incident["id"])["proposal"]
    assert views.decide(incident["id"], "approve").startswith("Dry run recorded")


def test_diagnose_and_system(agent, monkeypatch):
    _, client, store, incidents, _ = agent("p1-throughput")
    monkeypatch.setattr(llm, "chat_available", lambda: (False, "no endpoint configured"))

    assert "not available" in views.diagnose(incidents["S01"]["id"])
    assert views.diagnose(None) == "Select an incident first."

    text = views.system(Poller(client, store).snapshot(), client)
    assert "**ARMED**" in text and "**unavailable**" in text and "Mock clock" in text


def test_review_shows_why_the_last_run_stopped(agent, monkeypatch):
    _, client, store, incidents, _ = agent("p1-throughput")
    incident = incidents["S01"]

    assert "Not diagnosed yet" in views.review(store, client, incident["id"])["proposal"]

    with monkeypatch.context() as down:
        down.setattr(llm, "chat_available", lambda: (False, "the endpoint answered 401: Token has expired"))
        graph.start(incident["id"])

    shown = views.review(store, client, incident["id"])
    assert shown["status"] == "NEW" and "last run stopped" in shown["proposal"]
    assert "Token has expired" in shown["proposal"]


def test_blocks_build_and_handlers(agent):
    _, _, store, incidents, _ = agent("p1-throughput")

    assert isinstance(gradio_app.build(), gr.Blocks)

    out = gradio_app.show_review(incidents["S01"]["id"])
    assert len(out) == 9 and "Not diagnosed yet" in out[1]

    table, picker = gradio_app.refresh_history(None)
    assert len(table) == len(incidents) and picker.value in {i["id"] for i in incidents.values()}
    assert "packs" in gradio_app._snapshot()
