"""
The UI's views on a mock tenant: fleet table and chart, the review panel
through a whole approve, history, system, and the Blocks layout itself.
"""

import json
import sqlite3
import threading
import time

import gradio as gr
import pytest

import config
from nifi_healer import llm, provisioning
from nifi_healer.agent import graph, nodes
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

    # The deployment row reads the charts every deployment has, without a KPI configured.
    deployments = views.deployment_rows(snapshot, store)
    assert len(deployments) == 1 and len(deployments[0]) == len(views.DEPLOYMENT_HEADERS)
    assert deployments[0][3] != "—" and deployments[0][4] != "—" and deployments[0][5] == "2"
    assert deployments[0][8] == str(len(incidents))

    groups = views.group_rows(snapshot, store)
    assert len(groups) == 4 and all(len(r) == len(views.GROUP_HEADERS) for r in groups)
    assert all(r[3] != "—" and r[4] != "—" for r in groups)
    assert any(r[1].startswith("S01") and r[2] == "BREACHING" and incidents["S01"]["id"] in r[7] for r in groups)

    # The roll-up's chart-only KPIs are in the group rows, not the KPI table.
    assert not any("S01 Source · flowfiles_out" in r[2] for r in rows)

    assert "p1-throughput|group:S01" in [v for _, v in choices]
    frame = views.kpi_frame(snapshot, "p1-throughput|group:S01")
    assert set(frame["series"]) == {"in (S01 Source)", "out (S01 Consumer)"}


def test_history_shows_each_kpi_before_and_after(agent):
    world, client, store, incidents, install = agent("p1-throughput")
    incident = incidents["S01"]
    install(ScriptedLLM(proposal(S01_FIX)))

    graph.start(incident["id"])
    graph.resume(incident["id"], {"decision": "approve", "reviewer": "test"})
    world.skip(config.VERIFY_SETTLE_SECONDS + config.VERIFY_WINDOW_SECONDS + 120)
    assert [o["verdict"] for o in Poller(client, store).evaluate_outcomes()] == ["IMPROVED"]

    group = store.outcomes(incident["id"])[-1]["details"]["group"]
    assert group["edges"] == {"in": ["S01 Source"], "out": ["S01 Consumer"]}
    assert group["ratio_after"] > group["ratio_before"]

    row = next(r for r in views.history_rows(store) if r[0] == incident["id"])
    assert len(row) == len(views.HISTORY_HEADERS)
    assert row[-1].startswith("s01-backlog ") and "better" in row[-1]

    detail = views.incident_detail(store, incident["id"])
    assert "| KPI | Decides |" in detail and "| s01-backlog · queued_count | primary |" in detail
    assert "**Group in/out per bucket:** in (S01 Source)" in detail and "out (S01 Consumer)" in detail


def test_change_reads_in_the_good_direction():
    assert views.change(417, 0) == "417 → 0 (−100%, better)"
    assert views.change(0, 427, "up") == "0 → 427 (from 0)"
    assert views.change(10, 20, "up").endswith("(+100%, better)")
    assert views.change(10, 20).endswith("(+100%, worse)")
    assert views.change(10, 10) == "10 → 10 (no change)"
    assert views.change(None, 3) == "—"


def applied(incident_id: str) -> None:
    """Wait for an approval being applied in the background."""

    future = graph._applying.get(incident_id)

    if future is not None:
        future.result(timeout=30)

    assert graph.run_state(incident_id) is None


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
    assert "in the background" in views.decide(incident["id"], "approve", reviewer="ops")
    applied(incident["id"])
    assert "not waiting" in views.decide(incident["id"], "approve")

    panel = views.review(store, client, incident["id"])
    assert "**Last action:** SET_CONCURRENT_TASKS **APPLIED**" in panel["proposal"]
    assert incident["id"] in [v for _, v in views.queue_choices(store, keep=incident["id"])]
    assert incident["id"] not in [v for _, v in views.queue_choices(store)]

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


def test_an_approval_in_cooldown_keeps_the_review_open(agent, monkeypatch):
    _, client, store, incidents, install = agent("p1-throughput")
    incident = incidents["S01"]
    install(ScriptedLLM(proposal(S01_FIX)))
    graph.start(incident["id"])
    store.add_action("INC-earlier", "p1-throughput", incident["deployment_crn"], "SET_RUN_SCHEDULE",
                     dry_run=False, status="APPLIED", started_at=client.now() - 60)

    message = views.decide(incident["id"], "approve")
    assert message.startswith("Not applied yet, cooldown") and "review stays open" in message
    assert graph.run_state(incident["id"]) is None and graph.pending(incident["id"])
    assert views.review(store, client, incident["id"])["can_approve"]
    assert store.actions(incident_id=incident["id"]) == []


def test_the_panel_follows_an_approval_being_applied(agent, monkeypatch):
    _, client, store, incidents, install = agent("p1-throughput")
    incident = incidents["S01"]
    install(ScriptedLLM(proposal(S01_FIX)))
    graph.start(incident["id"])
    gate, entered = threading.Event(), threading.Event()
    resume = graph.resume

    def slow(*args):
        entered.set()
        gate.wait(30)

        return resume(*args)

    monkeypatch.setattr(graph, "resume", slow)
    before = views.review_key(store, incident["id"])

    try:
        views.decide(incident["id"], "approve")
        assert entered.wait(10)
        panel = views.review(store, client, incident["id"])
        assert "Applying the approved change" in panel["proposal"]
        assert not panel["can_approve"] and not panel["pending"]
        assert "being applied" in views.decide(incident["id"], "approve")
        assert views.review_key(store, incident["id"]) != before

    finally:
        gate.set()
        applied(incident["id"])

    assert store.incident(incident["id"])["status"] == "VERIFYING"


def test_an_approval_that_raises_in_the_background_says_so(agent, monkeypatch):
    _, client, store, incidents, install = agent("p1-throughput")
    incident = incidents["S01"]
    install(ScriptedLLM(proposal(S01_FIX)))
    graph.start(incident["id"])

    def broken(*args):
        raise sqlite3.OperationalError("database is locked")

    with monkeypatch.context() as patched:
        patched.setattr(graph, "resume", broken)
        views.decide(incident["id"], "approve")
        applied(incident["id"])

    panel = views.review(store, client, incident["id"])
    assert "last approval failed" in panel["proposal"] and "database is locked" in panel["proposal"]
    assert panel["can_approve"] and store.actions(incident_id=incident["id"]) == []

    views.decide(incident["id"], "approve")
    applied(incident["id"])
    assert graph.apply_failure(incident["id"]) == ""
    assert "last approval failed" not in views.review(store, client, incident["id"])["proposal"]


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


def test_queued_incidents_show_their_place_and_are_not_queued_twice(agent, monkeypatch):
    _, client, store, incidents, install = agent("p1-throughput")
    monkeypatch.setattr(config, "AGENT_WORKERS", 1)
    graph._pool = None
    gate = threading.Event()

    def slow(messages):
        gate.wait(30)

        return proposal(S01_FIX)(messages)

    install(ScriptedLLM(slow, *[proposal([{"action": "NO_ACTION"}])] * 2))
    s01, s02, s03 = (incidents[k]["id"] for k in ("S01", "S02", "S03"))

    try:
        first = graph.submit(incidents["S01"])
        deadline = time.time() + 10

        while graph.run_state(s01) != ("running", 0, 0) and time.time() < deadline:
            time.sleep(0.01)

        second = graph.submit(incidents["S02"])
        graph.submit(incidents["S03"])

        assert graph.submit(incidents["S02"]) is second
        assert graph.run_state(s02) == ("queued", 1, 2) and graph.run_state(s03) == ("queued", 2, 2)

        panel = views.review(store, client, s03)
        assert "Queued for diagnosis: **2** of 2" in panel["proposal"] and not panel["can_diagnose"]
        assert "agent is diagnosing" in views.review(store, client, s01)["proposal"]
        assert "2 of 2 waiting" in views.diagnose(s03)

    finally:
        gate.set()
        graph._executor().shutdown(wait=True)
        graph._pool = None

    assert first.done() and graph.run_state(s01) is None and graph.run_state(s03) is None
    assert graph.pending(s01)["plan"]["ok"] and not views.review(store, client, s01)["can_diagnose"]


def test_an_unusable_reply_is_shown_for_review(agent):
    _, client, store, incidents, install = agent("p1-throughput")
    incident = incidents["S01"]
    install(ScriptedLLM(*["I think the consumer is slow."] * 2 * nodes.MAX_ATTEMPTS))
    graph.start(incident["id"])

    panel = views.review(store, client, incident["id"])
    assert panel["status"] == "PENDING_REVIEW" and panel["pending"] and not panel["can_approve"]
    assert "No usable proposal" in panel["proposal"] and "I think the consumer is slow." in panel["proposal"]
    assert "Submit edit" in panel["proposal"] and panel["steps"] == "[]"

    assert views.decide(incident["id"], "reject", reason="no proposal").endswith("rejected.")


def test_a_review_whose_checkpoint_was_lost_can_be_picked_up_again(agent, monkeypatch):
    _, client, store, incidents, install = agent("p1-throughput")
    incident = incidents["S01"]
    install(ScriptedLLM(proposal(S01_FIX)))

    def locked(payload):
        raise sqlite3.OperationalError("database is locked")

    with monkeypatch.context() as broken:
        broken.setattr(nodes, "interrupt", locked)

        with pytest.raises(sqlite3.OperationalError):
            graph.start(incident["id"])

    panel = views.review(store, client, incident["id"])
    assert panel["status"] == "PENDING_REVIEW" and not panel["pending"] and panel["can_diagnose"]
    assert "no paused review" in panel["proposal"] and "S01 Consumer" in panel["proposal"]
    assert graph.orphaned(store.incident(incident["id"]))

    try:
        assert views.diagnose(incident["id"]).startswith("Diagnosing")

    finally:
        graph._executor().shutdown(wait=True)
        graph._pool = None

    panel = views.review(store, client, incident["id"])
    assert panel["pending"] and panel["can_approve"] and not panel["can_diagnose"]


def test_a_review_diagnosed_on_a_version_no_longer_deployed_is_diagnosed_again(agent):
    # Live: S17 stayed open across a rebuilt baseline (v1 -> v2) and kept its v1 proposal.
    world, client, store, incidents, install = agent("p1-throughput")
    incident = incidents["S01"]
    model = install(ScriptedLLM(proposal(S01_FIX), proposal(S01_FIX)))
    graph.start(incident["id"])

    panel = views.review(store, client, incident["id"])
    assert panel["pending"] and not panel["can_diagnose"] and "Stale" not in panel["proposal"]
    first = graph.pending(incident["id"])["proposal_id"]

    provisioning.import_flow(client, "p1-throughput", new_baseline=True)
    world.skip(31 * 60)
    provisioning.reset_pack(client, "p1-throughput", dry_run=False, flow_only=True,
                            wait=lambda: world.settle(incident["deployment_crn"]))
    Poller(client, store).poll_now()

    panel = views.review(store, client, incident["id"])
    assert panel["pending"] and panel["can_diagnose"]
    assert "**Stale.** This was diagnosed on flow v1, and the deployment now runs v2" in panel["proposal"]

    try:
        assert views.diagnose(incident["id"], "ops").startswith("Diagnosing")

    finally:
        graph._executor().shutdown(wait=True)
        graph._pool = None

    assert len(model.calls) == 2
    review = graph.pending(incident["id"])
    assert review and review["proposal_id"] != first and review["plan"]["ok"]
    assert store.incident(incident["id"])["expected_flow_version_crn"] == \
        client.flow_facts(incident["deployment_crn"], incident["deployed_flow_crn"])["flow_version_crn"]
    assert [p["status"] for p in store.proposals(incident["id"])] == ["SUPERSEDED", "VALID"]
    assert [(r["decision"], r["reviewer"]) for r in store.reviews(incident["id"])] == [("rediagnose", "ops")]

    panel = views.review(store, client, incident["id"])
    assert panel["can_approve"] and not panel["can_diagnose"] and "Stale" not in panel["proposal"]


def test_a_locked_database_names_itself_and_another_app_process(agent, monkeypatch, tmp_path):
    agent("p1-throughput")
    monkeypatch.setattr(config, "DATA_DIR", str(tmp_path))
    gradio_app._claim_data_dir()

    def locked(*_):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(views, "review", locked)

    with pytest.raises(gr.Error, match="database was busy") as raised:
        gradio_app.show_review("INC-1")

    assert "Another app process" not in raised.value.message

    (tmp_path / "app.owner").write_text(json.dumps({"host": "other-pod", "pid": 7}))

    with pytest.raises(gr.Error, match=r"Another app process \(other-pod pid 7\)"):
        gradio_app.show_review("INC-1")


def test_the_reviewer_is_the_cai_login_and_viewers_cannot_decide(agent):
    _, client, store, incidents, install = agent("p1-throughput")
    incident = incidents["S01"]
    install(ScriptedLLM(proposal(S01_FIX)))
    graph.start(incident["id"])

    class Request:
        def __init__(self, **headers):
            self.headers = headers

    viewer = Request(**{"remote-user": "guest", "remote-user-perm": "RO"})
    owner = Request(**{"remote-user": "pdefusco", "remote-user-perm": "RW"})
    reject = gradio_app._decide("reject")

    assert gradio_app.caller(None) == ("", "") and gradio_app.who_is_reviewing(owner) == "pdefusco"
    assert "can only view" in reject(incident["id"], "", "no", "", request=viewer)[0]
    assert graph.pending(incident["id"])

    assert reject(incident["id"], "", "not now", "", request=owner)[0].endswith("rejected.")
    assert [r["reviewer"] for r in store.reviews(incident["id"])] == ["pdefusco"]


def test_a_stale_queue_tick_leaves_the_pick_alone_and_decisions_check_the_panel(agent):
    # Live: a tick that read S18 landed after the reviewer picked S17, and their S17 edit went to S18.
    _, client, store, incidents, install = agent("p1-throughput")
    s01, s02 = incidents["S01"]["id"], incidents["S02"]["id"]

    tick = gradio_app.refresh_queue(s02)
    assert "value" not in tick and s02 in [v for _, v in tick["choices"]]
    assert gradio_app.refresh_queue("INC-gone")["value"] == tick["choices"][0][1]
    assert gradio_app.refresh_queue(None)["value"] == tick["choices"][0][1]

    install(ScriptedLLM(proposal(S01_FIX)))
    graph.start(s01)
    steps = views.review(store, client, s01)["steps"]
    shown_s02 = gradio_app.select_review(s02)[-1]

    out = gradio_app._decide("edit")(s01, "", "", steps, shown_s02)
    assert "showing another incident" in out[0] and "nothing was sent" in out[0]
    assert out[-2]["value"] == s01 and out[-1] == views.review_key(store, s01)
    assert store.reviews(s01) == [] and graph.pending(s01)
    assert "showing another incident" in gradio_app.diagnose(s02, gradio_app.select_review(s01)[-1])[0]

    out = gradio_app._decide("edit")(s01, "", "", steps, gradio_app.select_review(s01)[-1])
    assert "validated" in out[0] and [r["decision"] for r in store.reviews(s01)] == ["edit"]


def test_blocks_build_and_handlers(agent):
    _, _, store, incidents, _ = agent("p1-throughput")

    assert isinstance(gradio_app.build(), gr.Blocks)

    out = gradio_app.show_review(incidents["S01"]["id"])
    assert len(out) == gradio_app.PANEL_OUTPUTS and "Not diagnosed yet" in out[1]

    *panel, key = gradio_app.select_review(incidents["S01"]["id"])
    assert panel[1] == out[1] and key
    assert all(isinstance(o, dict) and o.get("__type__") == "update" and len(o) == 1
               for o in gradio_app.follow_review(incidents["S01"]["id"], key))
    assert gradio_app.follow_review(incidents["S01"]["id"], "")[-1] == key

    table, picker, detail = gradio_app.refresh_history(None)
    assert len(table) == len(incidents) and picker.value in {i["id"] for i in incidents.values()}
    assert f"### {picker.value}" in detail
    assert "packs" in gradio_app._snapshot()

    # Live: S17-9147's detail still said VERIFYING after Refresh showed it RESOLVED in the table.
    store.update_incident(picker.value, time.time(), status="RESOLVED", verdict="IMPROVED")
    assert "**RESOLVED** · verdict **IMPROVED**" in gradio_app.refresh_history(picker.value)[2]
