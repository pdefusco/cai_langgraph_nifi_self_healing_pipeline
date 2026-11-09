"""
The UI's views on a mock tenant: fleet table and chart, the review panel
through a whole approve, history, system, and the Blocks layout itself.
"""

import json
import sqlite3
import threading
import time
import types

import gradio as gr
import pytest

import config
from flows import packs
from nifi_healer import llm, provisioning
from nifi_healer.actions import catalog
from nifi_healer.agent import graph, nodes
from nifi_healer.flow import inventory
from nifi_healer.kpi import outcomes
from nifi_healer.runtime.poller import Poller
from nifi_healer.sim.world import groups_from_definition
from nifi_healer.ui import gradio_app, monitor, views

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


def test_a_pack_newer_than_the_app_says_so_instead_of_staying_silent(tenant, monkeypatch):
    # Live: p23 was provisioned from a commit after the app started; its faults breached
    # for 40 min with no incident, as the app's build had no S51-S77 to place them (NFH-99).
    _, client, store = tenant("p1-throughput", history=3600)
    every = packs.SCENARIOS
    monkeypatch.setattr(packs, "SCENARIOS", {k: v for k, v in every.items() if k != "S01"})
    opened = []
    poller = Poller(client, store, on_incident=opened.append)
    poller.poll_now()
    snapshot = poller.snapshot()

    assert opened and "S01" not in {i["scenario"] for i in opened}
    assert snapshot["packs"]["p1-throughput"]["unknown_scenarios"] == ["S01"]
    header = views.fleet_header(snapshot)
    assert header.startswith("⚠ **Detection off** for p1-throughput: this build of the app does not know S01,")
    assert "restart the app (pull_and_restart.py)" in header

    monkeypatch.setattr(packs, "SCENARIOS", every)    # the app restarted on a build that knows it
    poller.poll_now()

    assert poller.snapshot()["packs"]["p1-throughput"]["unknown_scenarios"] == []
    assert "Detection off" not in views.fleet_header(poller.snapshot())


def test_history_shows_each_kpi_before_and_after(agent):
    # Two hours of history: the 1 h window's before side spans the 1.5 h before the change.
    world, client, store, incidents, install = agent("p1-throughput", history=2 * 3600)
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
    assert row[5] == "15 min improved · 1 h pending · 12 h pending"

    detail = views.incident_detail(store, incident["id"])
    assert "| KPI | Decides |" in detail and "| s01-backlog · queued_count | primary |" in detail
    assert "for action 1, 15 min window: **IMPROVED** (LAST_ONE_HOUR)" in detail

    # The hour after it, from the 12 h chart; the incident was closed by the first window already.
    action = store.actions(incident_id=incident["id"])[-1]
    world.skip(outcomes.due_at(action["finished_at"], 3600, action["started_at"]) - client.now() + 60)
    hour = Poller(client, store).evaluate_outcomes()
    assert [(o["window_seconds"], o["verdict"]) for o in hour] == [(3600, "IMPROVED")]
    assert store.incident(incident["id"])["status"] == "RESOLVED"
    assert "for action 1, 1 h window: **IMPROVED** (LAST_TWELVE_HOURS)" in views.incident_detail(store, incident["id"])
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
    assert "S01 Consumer" in panel["proposal"] and panel["change"].startswith("- S01 Consumer: concurrent tasks 1 → ")
    assert "import-flow-definition-version" in panel["commands"]
    assert "change-flow-version-in-deployment" in panel["commands"]

    assert "not valid JSON" in views.decide(incident["id"], "edit", steps="[{")
    assert views.decide(incident["id"], "reject").startswith("Not accepted")
    assert "in the background" in views.decide(incident["id"], "approve", reviewer="ops")
    applied(incident["id"])
    assert "not waiting" in views.decide(incident["id"], "approve")

    panel = views.review(store, client, incident["id"])
    assert "**Last action:** SET_CONCURRENT_TASKS **APPLIED**" in panel["proposal"]
    assert incident["id"] in [v for _, v in views.queue_choices(store, keep=incident["id"])]
    assert incident["id"] not in [v for _, v in views.queue_choices(store)]

    row = next(r for r in views.history_rows(store) if r[0] == incident["id"])
    assert row[3] == "VERIFYING" and "APPLIED" in row[7] and row[8] == "ops"
    assert row[5] == "15 min pending · 1 h pending · 12 h pending"

    detail = views.incident_detail(store, incident["id"])
    assert "**Review:** approve by ops" in detail and "cdp df change-flow-version-in-deployment" in detail


def test_edit_and_dry_run_messages(agent, monkeypatch):
    _, client, store, incidents, install = agent("p1-throughput")
    monkeypatch.setattr(config, "ARMED", False)
    incident = incidents["S01"]
    install(ScriptedLLM(proposal(S01_FIX)))
    graph.start(incident["id"])

    edited = '[{"action": "SET_CONCURRENT_TASKS", "target": "S01 Consumer", "value": 40}]'
    assert "failed validation" in views.decide(incident["id"], "edit", steps=edited)
    assert not views.review(store, client, incident["id"])["can_approve"]

    # The reviewer's fix, through the form (NFH-58): the row reads back the step they change.
    panel = views.review(store, client, incident["id"])
    row = views.step_form(panel["steps"], panel["targets"])[0]
    assert row["action"] == "SET_CONCURRENT_TASKS" and row["target"]["value"] == "S01 Consumer"
    assert "S01 Consumer" in panel["targets"]["processors"] and "s01-backlog" in panel["targets"]["connections"]
    assert row["fields"][0]["label"] == "Concurrent tasks"
    empty = ["", None] + [None] * views.FORM_FIELDS
    values = [row["action"], row["target"]["value"], "6", None, None] + empty * 3
    assert "validated" in views.decide(incident["id"], "edit", steps=views.form_steps(values))
    assert "concurrent tasks 1 → 6" in views.review(store, client, incident["id"])["change"]
    assert "no steps" in views.decide(incident["id"], "edit", steps=[])
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
    assert "Submit edit" in panel["proposal"] and panel["steps"] == []

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
    assert "stale" not in {v: k for k, v in views.queue_choices(store)}[incident["id"]]
    assert "stale" not in views.queue_summary(store)
    first = graph.pending(incident["id"])["proposal_id"]

    provisioning.import_flow(client, "p1-throughput", new_baseline=True)
    world.skip(31 * 60)
    provisioning.reset_pack(client, "p1-throughput", dry_run=False, flow_only=True,
                            wait=lambda: world.settle(incident["deployment_crn"]))
    Poller(client, store).poll_now()

    panel = views.review(store, client, incident["id"])
    assert panel["pending"] and panel["can_diagnose"]
    assert "**Stale.** This was diagnosed on flow v1, and the deployment now runs v2" in panel["proposal"]
    # The queue says so before the review is opened.
    label = {v: k for k, v in views.queue_choices(store)}[incident["id"]]
    assert "· awaiting review · stale · " in label
    assert "· 1 stale, diagnosed on a flow no longer deployed" in views.queue_summary(store)

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


def test_a_review_waiting_to_be_diagnosed_again_says_so_and_takes_no_decision(agent):
    # Live (NFH-110): S49's v1 proposal, queued to be diagnosed again, read as the new one, its buttons live.
    world, client, store, incidents, install = agent("p1-throughput")
    incident = incidents["S01"]
    install(ScriptedLLM(proposal(S01_FIX), proposal(S01_FIX)))
    graph.start(incident["id"])
    first = graph.pending(incident["id"])["proposal_id"]

    provisioning.import_flow(client, "p1-throughput", new_baseline=True)
    world.skip(31 * 60)
    provisioning.reset_pack(client, "p1-throughput", dry_run=False, flow_only=True,
                            wait=lambda: world.settle(incident["deployment_crn"]))
    Poller(client, store).poll_now()

    # Every worker busy, so the run waits its turn.
    busy = threading.Event()
    blockers = [graph._executor().submit(busy.wait) for _ in range(config.AGENT_WORKERS)]

    try:
        assert views.diagnose(incident["id"], "ops").startswith("Queued")
        panel = views.review(store, client, incident["id"])

        assert "**Being diagnosed again:** queued, 1 of 1 waiting." in panel["proposal"]
        assert "Stale" not in panel["proposal"]
        assert not (panel["pending"] or panel["can_approve"] or panel["can_diagnose"] or panel["can_hold"])

        for decision in ("approve", "reject", "edit"):
            assert "is being diagnosed again" in views.decide(incident["id"], decision, "ops", "no", S01_FIX)

        assert graph.pending(incident["id"])["proposal_id"] == first and not store.reviews(incident["id"])

    finally:
        busy.set()
        graph._executor().shutdown(wait=True)
        graph._pool = None

    assert all(b.done() for b in blockers)
    panel = views.review(store, client, incident["id"])
    assert graph.pending(incident["id"])["proposal_id"] != first
    assert panel["can_approve"] and "Being diagnosed again" not in panel["proposal"]


def test_a_review_being_diagnosed_again_still_says_so_once_its_run_is_running(agent):
    # Live (NFH-110): once a worker took S09's run the graph was past its pause, so the panel showed the old
    # proposal from the store with nothing saying it was being replaced.
    world, client, store, incidents, install = agent("p1-throughput")
    incident = incidents["S01"]
    started, release = threading.Event(), threading.Event()

    def held(messages):
        started.set()
        release.wait(10)
        return proposal(S01_FIX, diagnosis="The new diagnosis.")(messages)

    install(ScriptedLLM(proposal(S01_FIX), held))
    graph.start(incident["id"])
    provisioning.import_flow(client, "p1-throughput", new_baseline=True)
    world.skip(31 * 60)
    provisioning.reset_pack(client, "p1-throughput", dry_run=False, flow_only=True,
                            wait=lambda: world.settle(incident["deployment_crn"]))
    Poller(client, store).poll_now()

    try:
        assert views.diagnose(incident["id"], "ops").startswith("Diagnosing")
        assert started.wait(10) and graph.run_state(incident["id"])[0] == "running"
        assert graph.pending(incident["id"]) is None
        panel = views.review(store, client, incident["id"])

        assert panel["proposal"].index("**Being diagnosed again.**") < panel["proposal"].index("**Diagnosis.**")
        assert "S01 Consumer is the bottleneck" in panel["proposal"]
        assert not (panel["pending"] or panel["can_approve"] or panel["can_diagnose"] or panel["can_hold"])

    finally:
        release.set()
        graph._executor().shutdown(wait=True)
        graph._pool = None

    panel = views.review(store, client, incident["id"])
    assert panel["can_approve"] and "The new diagnosis." in panel["proposal"]
    assert "Being diagnosed again" not in panel["proposal"]


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


def test_the_queue_puts_the_oldest_review_first_and_filters_by_deployment(agent):
    _, client, store, incidents, _ = agent("p1-throughput", "p5-parameters")
    now = client.now()
    s01, s02, s17 = (incidents[k]["id"] for k in ("S01", "S02", "S17"))
    store.update_incident(s01, now, status="PENDING_REVIEW", opened_at=now - 600)
    store.update_incident(s17, now, status="PENDING_REVIEW", opened_at=now - 5 * 3600)
    store.update_incident(s02, now, status="DIAGNOSING")
    others = [i["id"] for i in incidents.values() if i["id"] not in (s01, s02, s17)]

    for incident_id in others:
        store.update_incident(incident_id, now, status="REJECTED")

    choices = views.queue_choices(store)
    assert [v for _, v in choices] == [s17, s01, s02]
    assert choices[0][0] == f"{s17} · p5-parameters › {incidents['S17']['process_group']} · awaiting review · 5 h old"
    assert choices[1][0].endswith("· awaiting review · < 1 h old") and "· diagnosing ·" in choices[2][0]

    assert [v for _, v in views.queue_choices(store, pack="p1-throughput")] == [s01, s02]
    assert [v for _, v in views.queue_choices(store, keep=s17, pack="p1-throughput")] == [s01, s02, s17]

    assert views.queue_summary(store) == (f"**2** awaiting review · oldest 5 h old ({s17}, p5-parameters) · "
                                          "1 being diagnosed")
    assert views.queue_summary(store, "p1-throughput").startswith("**1** awaiting review · oldest < 1 h old")
    assert views.queue_summary(store, "p9-nothing") == "Nothing waiting on p9-nothing."
    assert views.queue_packs(store, {"p1-throughput": {}, "p5-parameters": {}, "p8-quiet": {}}) == [
        ("All deployments (3 open)", ""), ("p1-throughput (2 open)", "p1-throughput"),
        ("p5-parameters (1 open)", "p5-parameters"), ("p8-quiet (0 open)", "p8-quiet")]

    picker, line = gradio_app.filter_queue("p1-throughput", s17)
    assert picker["value"] == s01 and [v for _, v in picker["choices"]] == [s01, s02]
    assert line.startswith("**1** awaiting review")
    assert gradio_app.filter_queue("p1-throughput", s02)[0]["value"] == s02

    packs, line = gradio_app.refresh_queue_packs("p1-throughput")
    assert "value" not in packs and "p1-throughput" in [v for _, v in packs["choices"]]
    assert gradio_app.refresh_queue_packs("p9-gone")[0]["value"] == ""

    picker, line = gradio_app.tick_queue(s01, "p1-throughput")
    assert "value" not in picker and [v for _, v in picker["choices"]] == [s01, s02]


def test_open_incidents_lead_the_fleet_overview():
    def row(name, open_=0, over=0, state="GOOD_HEALTH"):
        return {"name": name, "open": open_, "over": over, "near": 0, "bad_buckets": 0, "deployment": {"state": state}}

    rows = [row("a"), row("b", over=3), row("c", open_=1), row("d", state="BAD_HEALTH"), row("e", open_=2)]
    assert [r["name"] for r in sorted(rows, key=monitor._sort_key("Needs attention"))] == ["e", "c", "b", "d", "a"]


def test_a_run_that_moves_on_while_the_panel_renders_is_followed(agent, monkeypatch):
    # Live: a rollback reached review while its panel rendered; the panel kept DIAGNOSING and no Approve.
    _, client, store, incidents, _ = agent("p1-throughput")
    incident_id = incidents["S01"]["id"]
    render = views.review

    def moving_on(*args):
        view = render(*args)
        store.update_incident(incident_id, client.now(), status="DIAGNOSING")

        return view

    monkeypatch.setattr(views, "review", moving_on)
    *_, key = gradio_app.select_review(incident_id)
    monkeypatch.setattr(views, "review", render)

    out = gradio_app.follow_review(incident_id, key)
    assert "stopped before it proposed a fix" in out[1] and out[-1] == views.review_key(store, incident_id)


def test_a_follow_that_lands_after_the_next_pick_is_dropped(agent, monkeypatch):
    # Live: a slow follow of S51's panel, just approved, landed after S52 was picked and showed S51 over it.
    _, client, store, incidents, _ = agent("p1-throughput")
    s01, s02 = incidents["S01"]["id"], incidents["S02"]["id"]
    browser, other = types.SimpleNamespace(session_hash="a"), types.SimpleNamespace(session_hash="b")
    render = views.review

    *_, seen = gradio_app.select_review(s01, browser)
    store.update_incident(s01, client.now(), status="DIAGNOSING")

    def picked_meanwhile(store_, client_, incident_id):
        if incident_id == s01:
            assert gradio_app.select_review(s02, browser)[-1] == views.review_key(store, s02)

        return render(store_, client_, incident_id)

    monkeypatch.setattr(views, "review", picked_meanwhile)
    skipped = (gr.skip(),) * (gradio_app.PANEL_OUTPUTS + 1)
    assert gradio_app.follow_review(s01, seen, browser) == skipped
    assert gradio_app.follow_review(s01, seen, browser) == skipped  # nor once it has moved on

    # Another browser's pick is its own.
    assert gradio_app.select_review(s01, other)[-1] == views.review_key(store, s01)
    assert gradio_app.follow_review(s02, "", browser)[-1] == views.review_key(store, s02)


def test_a_select_that_starts_after_the_next_pick_does_not_keep_the_panel(agent):
    # Live (NFH-109): the dropdown named S49 and the panel stayed on S01, the queue's first.
    _, _, store, incidents, _ = agent("p1-throughput")
    s01, s02 = incidents["S01"]["id"], incidents["S02"]["id"]
    browser = types.SimpleNamespace(session_hash="a")
    skipped = (gr.skip(),) * (gradio_app.PANEL_OUTPUTS + 1)

    gradio_app.select_review(s02, browser)
    *_, seen = gradio_app.select_review(s01, browser)  # sent before the pick of S02, started after it

    # The first follow may be one sent before the pick; the second is the dropdown as it is.
    assert gradio_app.follow_review(s02, seen, browser) == skipped
    *_, seen = gradio_app.follow_review(s02, seen, browser)
    assert seen == views.review_key(store, s02)
    assert gradio_app.follow_review(s02, seen, browser) == skipped


def test_only_the_open_tabs_timers_tick_and_a_tab_loads_when_opened(agent):
    # Live, every tab refreshed at once and their reads queued in front of the Review Queue's (NFH-111).
    agent("p1-throughput")
    config = gradio_app.build().config
    timers = [c for c in config["components"] if c["type"] == "timer"]
    tabs = {c["id"]: c["props"]["label"] for c in config["components"] if c["type"] == "tabitem"}
    selects = {tabs[target]: d for d in config["dependencies"] for target, event in d["targets"]
               if target in tabs and event == "select"}
    loads = {d["api_name"].rstrip("_0123456789") for d in config["dependencies"]
             if d["targets"] and d["targets"][0][1] == "load"}

    assert [t["props"]["value"] for t in timers if t["props"]["active"]] == [1.0, 15.0]  # the Fleet tab's
    assert set(selects) == set(tabs.values())
    assert all(sorted(d["outputs"]) == sorted(t["id"] for t in timers) for d in selects.values())
    assert loads == {"refresh_kpi_choices", "who_is_reviewing"}  # the page itself loads only the Fleet tab

    groups = {"Fleet": [1, 2], "Review Queue": [3], "Soak": [4]}
    active = [update.active for update in gradio_app._ticking(groups, "Fleet")()]
    assert active == [True, True, False, False]
    assert [update.active for update in gradio_app._ticking(groups, "History")()] == [False] * 4


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


def test_a_reviewer_labels_a_fix_and_rechecks_a_window(agent, monkeypatch):
    world, client, store, incidents, install = agent("p1-throughput", history=2 * 3600)
    incident_id = incidents["S01"]["id"]
    install(ScriptedLLM(proposal(S01_FIX)))
    graph.start(incident_id)
    graph.resume(incident_id, {"decision": "approve", "reviewer": "test"})
    world.skip(config.VERIFY_SETTLE_SECONDS + config.VERIFY_WINDOW_SECONDS + 120)
    poller = Poller(client, store)
    assert [o["verdict"] for o in poller.evaluate_outcomes()] == ["IMPROVED"]
    action = store.actions(incident_id=incident_id)[-1]
    assert views.correction_choices(store, incident_id) == [
        (f"Action {action['id']} · SET_CONCURRENT_TASKS · {views.clock(action['finished_at'])}", action["id"])]

    # The reviewer's call sits beside what was measured, and needs a reason.
    assert "reason is required" in views.label_fix(incident_id, action["id"], "WRONG_FIX", "  ")
    assert "applied actions" in views.label_fix(incident_id, 999, "WRONG_FIX", "x")
    assert "Choose one of" in views.label_fix(incident_id, action["id"], "MAYBE", "x")
    assert "**wrong fix**" in views.label_fix(incident_id, action["id"], "WRONG_FIX",
                                              "the backlog drained on its own", "ops")
    row = next(r for r in views.history_rows(store) if r[0] == incident_id)
    assert row[4] == "IMPROVED · wrong fix" and store.incident(incident_id)["verdict"] == "IMPROVED"
    assert "**Reviewer's call:** wrong fix (by ops" in views.incident_detail(store, incident_id)

    class Request:
        def __init__(self, **headers):
            self.headers = headers

    viewer = Request(**{"remote-user": "guest", "remote-user-perm": "RO"})
    owner = Request(**{"remote-user": "pdefusco", "remote-user-perm": "RW"})
    out = gradio_app.label_fix(incident_id, action["id"], "RIGHT_FIX", "it was", "", request=viewer)
    assert "can only view" in out[0] and len(out) == 3
    assert store.labels(incident_id)[action["id"]]["label"] == "WRONG_FIX"
    gradio_app.label_fix(incident_id, action["id"], "RIGHT_FIX", "it was after all", "", request=owner)
    assert store.labels(incident_id)[action["id"]]["reviewer"] == "pdefusco"

    # A re-check from two minutes after the fix, through the poller; the old verdict is kept.
    assert "not running" in views.recheck(incident_id, action["id"], 900, "", runner=None)
    assert "Measure from:" in views.recheck(incident_id, action["id"], 900, "soon", runner=poller)
    world.skip(600)
    monkeypatch.setattr(gradio_app.poller_module, "get", lambda: poller)
    assert "can only view" in gradio_app.recheck(incident_id, action["id"], 900, "+2", "", request=viewer)[0]
    assert store.superseded_outcomes(incident_id) == []

    message, _, detail = gradio_app.recheck(incident_id, action["id"], 900, "+2", "", request=owner)
    assert "Re-checked the 15 min window" in message and "(was IMPROVED)" in message
    assert "**Re-checked** by pdefusco" in detail and "it was IMPROVED" in detail
    assert f"after {views.clock(action['finished_at'] + 120)}" in detail and "- Superseded: IMPROVED" in detail
    assert [o["verdict"] for o in store.superseded_outcomes(incident_id)] == ["IMPROVED"]


def test_measure_from_reads_minutes_and_utc_times():
    finished = 1_760_000_000.0          # 2025-10-09 08:53:20 UTC

    assert views.parse_start("", finished) is None
    assert views.parse_start("+30", finished) == finished + 1800
    assert views.parse_start("09:10", finished) == finished + 1000
    assert views.parse_start("08:53", finished) == finished          # the minute it finished in
    assert views.parse_start("08:00", finished) == finished - 3200 + 86400
    assert views.parse_start("2025-10-09 09:10", finished) == finished + 1000

    with pytest.raises(ValueError):
        views.parse_start("soon", finished)


def test_the_step_form_reads_a_fix_back_as_it_was():
    # NFH-58: S52's route, as the Review Queue's form shows it, offering the group's own names.
    definition = packs.build("p23-siblings")
    s52 = packs.SCENARIOS["S52"]
    targets = inventory.targets(inventory.inventory(definition, s52.group_name, groups_from_definition(definition)))
    steps = [catalog.normalize(s) for s in s52.expected]
    rows = views.step_form(steps, targets)

    assert [r["visible"] for r in rows] == [True, True, False, False] and rows[1]["action"] == ""
    route = rows[0]
    assert route["target"]["value"] == "S52 Validator" and "S52 Validator" in route["target"]["choices"]
    assert [(f["label"], f["value"]) for f in route["fields"][:2]] == [
        ("Relationship (commas between several)", "failure"), ("DLQ", "S52 DLQ")]
    assert "failure" in route["fields"][0]["choices"] and route["fields"][1]["choices"] == ["S52 DLQ"]
    assert not route["fields"][2]["visible"]

    values = [v for r in rows for v in (r["action"], r["target"]["value"], *(f["value"] for f in r["fields"]))]
    assert [catalog.normalize(s) for s in views.form_steps(values)] == steps

    # Every action the catalog has is named, and fits the form.
    assert set(catalog.CATALOG) <= set(views.ACTION_NAMES)
    assert all(len(a.required + a.optional) <= views.FORM_FIELDS for a in catalog.CATALOG.values())
    assert [v for _, v in views.action_choices()][1:] == [a for a in catalog.CATALOG if a != "ROLLBACK"]


def test_choosing_an_action_names_its_fields_and_shows_the_next_step():
    targets = {"processors": {"S01 Consumer": {"relationships": ["success", "failure"], "properties": []}},
               "connections": ["s01-backlog"], "dlqs": [], "parameters": {}}
    target, *fields, next_row = gradio_app.pick_action(False)("SET_BACKPRESSURE", targets)

    assert target["label"] == "Connection" and target["choices"] == ["s01-backlog"]
    assert [f["label"] for f in fields[:2]] == ["Back pressure object threshold", "Back pressure size threshold"]
    assert not fields[2]["visible"] and next_row["visible"]
    assert gradio_app.pick_action(True)("", targets)[0]["visible"] is False

    fields = gradio_app.pick_target("SET_RETRY_POLICY", "S01 Consumer", targets, None, None, None)
    assert [f["choices"] for f in fields[:1]] == [list(catalog.BACKOFF_MECHANISMS)]
