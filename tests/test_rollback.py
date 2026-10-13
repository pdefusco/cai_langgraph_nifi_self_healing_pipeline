"""Rollback proposals: planned from the action they undo, guarded, opened by the poller or a reviewer."""

import sqlite3

import pytest

import config
from flows import packs
from nifi_healer.actions import executor, rollback
from nifi_healer.agent import graph
from nifi_healer.eval import scorecard as sc
from nifi_healer.kpi import outcomes
from nifi_healer.runtime.poller import Poller
from nifi_healer.store.db import Store
from nifi_healer.ui import views

from test_agent_graph import S01_FIX, ScriptedLLM, agent, proposal  # noqa: F401 (agent is a fixture)
from test_executor import incident_for
from test_scorecard import _action, _incident, _record

FIRST = config.VERIFY_WINDOWS[0]


@pytest.fixture(autouse=True)
def no_cooldown(monkeypatch):
    monkeypatch.setattr(config, "COOLDOWN_SECONDS", 0)
    monkeypatch.setattr(config, "MAX_ACTIONS_PER_WINDOW", 10)


def _reviewed(client, store, incident, steps) -> dict:
    """A proposal as the graph stores it: the steps and the validated plan, diff included."""

    report = executor.public(executor.plan(client, incident, steps, store))
    proposal_id = store.add_proposal(incident["id"], 0.0, action=list(steps), status="VALID", validation=report)

    return {"id": proposal_id, "action": list(steps)}


def _applied(world, client, store, scenario_id, steps=None):
    incident = incident_for(client, store, scenario_id)
    steps = steps or packs.SCENARIOS[scenario_id].expected
    action = executor.execute(client, store, incident, _reviewed(client, store, incident, steps),
                              dry_run=False, wait=lambda: world.settle(incident["deployment_crn"]))
    assert action["status"] == "APPLIED", action["error"]
    store.update_incident(incident["id"], client.now(), status="UNRESOLVED")

    return incident, action


def _undo(world, client, store, action, dry_run=False):
    incident = rollback.open_for(store, action, "test", client.now())
    steps = [{"action": "ROLLBACK", "action_id": action["id"]}]
    report = executor.plan(client, incident, steps, store)
    done = executor.execute(client, store, incident, _reviewed(client, store, incident, steps),
                            dry_run=dry_run, wait=lambda: world.settle(incident["deployment_crn"]))

    return incident, report, done


def test_a_flow_edit_is_undone_by_deploying_the_version_it_replaced(tenant):
    world, client, store = tenant("p1-throughput")
    fixed, action = _applied(world, client, store, "S01")

    incident, report, done = _undo(world, client, store, action)

    assert rollback.is_rollback(incident) and incident["rollback_of_action_id"] == action["id"]
    assert incident["process_group"] == fixed["process_group"]
    assert report["ok"], report["problems"]
    assert report["family"] == "ROLLBACK" and report["rollback_family"] == "FLOW_EDIT"
    assert len(report["commands"]) == 1 and "change-flow-version-in-deployment" in report["commands"][0]
    assert report["changes"][0].startswith(f"roll back action {action['id']}")
    applied = rollback.proposal_of(store, action)["validation"]["diff"]
    assert applied and [(d["before"], d["after"]) for d in report["diff"]] == \
        [(d["after"], d["before"]) for d in applied]

    assert done["status"] == "APPLIED", done["error"]
    assert done["family"] == "FLOW_EDIT" and [a[1] for a in done["argv"]] == ["change-flow-version-in-deployment"]
    assert done["to_flow_version_crn"] == action["from_flow_version_crn"]
    facts = client.flow_facts(fixed["deployment_crn"], fixed["deployed_flow_crn"])
    assert facts["flow_version_crn"] == action["from_flow_version_crn"]
    assert store.incident(incident["id"])["status"] == "VERIFYING"

    # Done once; the rollback itself is not rolled back.
    assert "already proposes" in rollback.blocked(store, store.action(action["id"]), manual=True)
    store.update_incident(incident["id"], client.now(), status="RESOLVED")
    assert "already rolled back" in rollback.blocked(store, store.action(action["id"]), manual=True)
    assert "itself a rollback" in rollback.unfit(store, done)


def test_a_flow_edit_rollback_is_refused_once_the_deployment_moved_on(tenant):
    world, client, store = tenant("p1-throughput")
    _, action = _applied(world, client, store, "S01")
    _applied(world, client, store, "S02")

    _, report, done = _undo(world, client, store, action)

    assert not report["ok"] and "would undo that later change too" in report["problems"][0]
    assert done["status"] == "REFUSED"

    Poller(client, store).poll_now()
    assert "now runs" in rollback.blocked(store, store.action(action["id"]), manual=True)


def test_a_dry_run_rollback_changes_nothing(tenant):
    world, client, store = tenant("p1-throughput")
    fixed, action = _applied(world, client, store, "S01")

    _, _, done = _undo(world, client, store, action, dry_run=True)

    assert done["status"] == "DRY_RUN" and len(done["argv"]) == 1
    facts = client.flow_facts(fixed["deployment_crn"], fixed["deployed_flow_crn"])
    assert facts["flow_version_crn"] == action["to_flow_version_crn"]


def _parameters(client, incident):
    groups = client.flow_configuration(incident["deployment_crn"], incident["deployed_flow_crn"])

    return {p["name"]: p.get("value") for g in groups["parameterGroups"] for p in g["parameters"]}


def test_a_parameter_is_set_back_while_it_is_still_the_value_the_action_set(tenant):
    world, client, store = tenant("p5-parameters")
    before = _parameters(client, incident_for(client, store, "S17"))["s17.target.url"]
    fixed, action = _applied(world, client, store, "S17")
    assert _parameters(client, fixed)["s17.target.url"] == "http://localhost:9876/ingest"

    _, report, done = _undo(world, client, store, action)

    assert report["ok"], report["problems"]
    assert report["commands"][0].startswith("cdp dfworkload update-flow-in-deployment")
    assert done["status"] == "APPLIED" and done["family"] == "PARAMETER", done["error"]
    assert _parameters(client, fixed)["s17.target.url"] == before


def test_a_parameter_changed_since_is_not_set_back(tenant):
    world, client, store = tenant("p5-parameters")
    fixed, action = _applied(world, client, store, "S17")
    step = dict(packs.SCENARIOS["S17"].expected[0], value="http://localhost:9877/ingest")
    _applied(world, client, store, "S17", [step])

    _, report, done = _undo(world, client, store, action)

    assert not report["ok"] and "someone changed it since" in report["problems"][0]
    assert done["status"] == "REFUSED"
    assert _parameters(client, fixed)["s17.target.url"] == "http://localhost:9877/ingest"


def test_sizing_is_set_back_and_refused_once_resized_again(tenant):
    world, client, store = tenant("p7-sizing-overprov")
    fixed, action = _applied(world, client, store, "S21")

    _, report, done = _undo(world, client, store, action)

    assert report["ok"] and report["risk"] == "high", report["problems"]
    assert done["status"] == "APPLIED" and done["family"] == "SIZING", done["error"]
    sizing = client.deployment_configuration(fixed["deployment_crn"])
    assert sizing["clusterSize"]["name"] == "MEDIUM"

    # Resized again by a later fix: setting the earlier one back would undo that too.
    _, second = _applied(world, client, store, "S21")
    _applied(world, client, store, "S21", [{"action": "SCALE_DEPLOYMENT", "cluster_size": "SMALL", "nodes": 1}])
    _, report, _ = _undo(world, client, store, second)
    assert not report["ok"] and "someone changed it since" in report["problems"][0]


def test_rollback_is_only_planned_for_its_own_incident(tenant):
    world, client, store = tenant("p1-throughput")
    fixed, action = _applied(world, client, store, "S01")
    steps = [{"action": "ROLLBACK", "action_id": action["id"]}]

    report = executor.plan(client, fixed, steps, store)
    assert not report["ok"] and "only planned for that action's rollback incident" in report["problems"][0]

    dry = executor.execute(client, store, incident_for(client, store, "S02"),
                           _reviewed(client, store, fixed, packs.SCENARIOS["S02"].expected), dry_run=True)
    assert dry["status"] == "DRY_RUN"
    assert "not applied to the tenant" in rollback.unfit(store, dry)
    assert rollback.unfit(store, None) == "There is no such action."


# ---------------------------------------------------------------------
# Who opens one
# ---------------------------------------------------------------------


def _judged(store, client, action, verdict, window=FIRST, regression=False):
    store.add_outcome(action["incident_id"], action["id"], verdict, "LAST_ONE_HOUR",
                      {"regression": True} if regression else {}, client.now(), window=window)


def test_the_poller_proposes_a_rollback_of_a_fix_measured_worse(tenant):
    world, client, store = tenant("p1-throughput")
    opened = []
    poller = Poller(client, store, on_incident=opened.append)
    fixed, action = _applied(world, client, store, "S01")
    store.update_incident(fixed["id"], client.now(), status="VERIFYING")
    _judged(store, client, action, outcomes.WORSENED)

    # Its own incident is still open; the poller waits for it.
    assert poller.propose_rollbacks(client.now()) == []
    assert "it goes first" in poller._not_rolled_back[action["id"]]

    store.update_incident(fixed["id"], client.now(), status="UNRESOLVED")
    found = poller.propose_rollbacks(client.now())

    assert [i["id"] for i in found] == [i["id"] for i in opened]
    assert found[0]["rollback_of_action_id"] == action["id"]
    assert f"{FIRST // 60} min verify window measured WORSENED" in found[0]["summary"]

    # Once proposed, never again by the poller, even after a rejection; a reviewer may.
    store.update_incident(found[0]["id"], client.now(), status="REJECTED")
    assert poller.propose_rollbacks(client.now()) == []
    assert "proposed before" in rollback.blocked(store, store.action(action["id"]))
    assert rollback.blocked(store, store.action(action["id"]), manual=True) == ""


def test_the_poller_proposes_one_on_a_regression_but_not_for_the_right_fix(tenant):
    world, client, store = tenant("p1-throughput")
    poller = Poller(client, store)
    later = config.VERIFY_WINDOWS[-1]
    _, improved = _applied(world, client, store, "S01")
    _judged(store, client, improved, outcomes.IMPROVED)
    assert poller.propose_rollbacks(client.now()) == []

    _judged(store, client, improved, outcomes.WORSENED, window=later, regression=True)
    store.add_label(improved["incident_id"], improved["id"], "RIGHT_FIX", "it is", "test", client.now())
    assert poller.propose_rollbacks(client.now()) == []

    store.add_label(improved["incident_id"], improved["id"], "WRONG_FIX", "it was not", "test", client.now())
    found = poller.propose_rollbacks(client.now())
    assert len(found) == 1 and "after it had resolved the incident" in found[0]["summary"]

    # A verdict older than ROLLBACK_WITHIN_SECONDS is left alone.
    _, old = _applied(world, client, store, "S02")
    _judged(store, client, old, outcomes.WORSENED)
    world.skip(config.ROLLBACK_WITHIN_SECONDS + 60)
    assert poller.propose_rollbacks(client.now()) == []


def test_a_reviewer_proposes_a_rollback_from_history(tenant):
    world, client, store = tenant("p1-throughput")
    fixed, action = _applied(world, client, store, "S01")
    graph.reset()

    try:
        assert "A reason is required" in views.propose_rollback(fixed["id"], action["id"], " ")
        assert "Pick one of" in views.propose_rollback(fixed["id"], 999, "worse")

        said = views.propose_rollback(fixed["id"], action["id"], "the queue grew", reviewer="pat")
        assert "to roll back action" in said and "Review Queue" in said
        incident = store.rollbacks_of(action["id"])[0]
        assert "pat asked: the queue grew" in incident["summary"]
        assert "already proposes rolling back" in views.propose_rollback(fixed["id"], action["id"], "again")

        graph.submit(incident).result(timeout=60)
        assert graph.pending(incident["id"])["plan"]["ok"]
        detail = views.incident_detail(store, fixed["id"])
        assert f"**Rollback proposed:** {incident['id']}" in detail
        assert "**Rollback** of action" in views.incident_detail(store, incident["id"])

    finally:
        graph.reset()


def test_a_rollback_goes_through_review_without_the_llm(agent):
    world, client, store, incidents, install = agent("p1-throughput")
    fixed = incidents["S01"]
    model = install(ScriptedLLM(proposal(S01_FIX)))
    graph.start(fixed["id"])
    action = graph.resume(fixed["id"], {"decision": "approve", "reviewer": "test"})["result"]
    store.update_incident(fixed["id"], client.now(), status="UNRESOLVED")
    Poller(client, store).poll_now()

    incident = rollback.open_for(store, store.action(action["id"]), "test", client.now())
    graph.start(incident["id"])
    review = graph.pending(incident["id"])

    assert len(model.calls) == 1                                   # the fix's diagnosis only
    assert review and review["plan"]["ok"] and review["plan"]["rollback_family"] == "FLOW_EDIT"
    assert store.incident(incident["id"])["status"] == "PENDING_REVIEW"
    assert views.review(store, client, incident["id"])["can_approve"]
    assert store.proposals(incident["id"])[-1]["agent_version"] == "rollback"

    graph.resume(incident["id"], {"decision": "edit", "steps": S01_FIX, "reviewer": "test"})
    assert "approved or rejected as proposed" in graph.pending(incident["id"])["error"]

    state = graph.resume(incident["id"], {"decision": "approve", "reviewer": "test"})
    assert state["result"]["status"] == "APPLIED", state["result"]["error"]
    facts = client.flow_facts(fixed["deployment_crn"], fixed["deployed_flow_crn"])
    assert facts["flow_version_crn"] == action["from_flow_version_crn"]


# ---------------------------------------------------------------------
# The scorecard and the store
# ---------------------------------------------------------------------


def test_a_rollback_is_not_a_case_but_flags_the_fix_it_undid():
    record = _record()
    record["incidents"].append(dict(_incident(9, "S01", "VERIFYING", 9000), kind="rollback",
                                    rollback_of_action_id=3))
    record["actions"].append(_action(6, 9, 11))

    found = {c["incident"][-1]: c for c in sc.cases(record)}

    assert "9" not in found and len(found) == 8
    assert found["3"]["rolled_back"] and not found["1"]["rolled_back"]
    assert sc.summarize(list(found.values()))["rolled_back"] == 1

    # Proposed but not applied: nothing was rolled back.
    record["actions"][-1] = _action(6, 9, 11, status="REFUSED")
    assert not any(c["rolled_back"] for c in sc.cases(record))


def test_an_older_store_gains_the_rollback_columns(tmp_path):
    path = str(tmp_path / "old.db")
    Store(path).conn.close()
    conn = sqlite3.connect(path)

    for column in ("kind", "rollback_of_action_id"):
        conn.execute(f"ALTER TABLE incidents DROP COLUMN {column}")

    conn.commit()
    conn.close()

    store = Store(path)
    columns = {r["name"] for r in store.conn.execute("PRAGMA table_info(incidents)")}

    assert {"kind", "rollback_of_action_id"} <= columns
