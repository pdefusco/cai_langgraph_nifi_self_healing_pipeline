"""Planning and running each action family against a mock tenant."""

import json
import os

import pytest

import config
from flows import packs
from flows.builder.lib import walk_groups
from nifi_healer import fleet
from nifi_healer.actions import executor


def incident_for(client, store, scenario_id: str) -> dict:
    scenario = packs.SCENARIOS[scenario_id]
    pack = packs.pack_of(scenario_id).name
    entry = fleet.pack(pack)
    facts = client.flow_facts(entry["deployment_crn"], entry["deployed_flow_crn"])

    return store.create_incident(
        pack=pack, deployment_crn=entry["deployment_crn"], deployed_flow_crn=entry["deployed_flow_crn"],
        scenario=scenario_id, process_group=scenario.group_name, signals=[], summary="test",
        expected_flow_version_crn=facts["flow_version_crn"], now=client.now())


def proposal_for(store, incident: dict, steps) -> dict:
    proposal_id = store.add_proposal(incident["id"], 0.0, action=list(steps), status="VALID")

    return {"id": proposal_id, "action": list(steps)}


def test_flow_edit_plan_shows_diff_and_commands(tenant):
    _, client, store = tenant("p1-throughput")
    incident = incident_for(client, store, "S01")
    report = executor.plan(client, incident, packs.SCENARIOS["S01"].expected)

    assert report["ok"], report["problems"]
    assert report["family"] == "FLOW_EDIT" and report["risk"] == "low"
    assert any(d["after"] == 4 for d in report["diff"])
    assert report["commands"][0].startswith("cdp df import-flow-definition-version")
    assert "--strategy ONLY_RESTART_AFFECTED_COMPONENTS" in report["commands"][1]
    assert "_definition" not in executor.public(report)


def test_plan_reports_bad_targets(tenant):
    _, client, store = tenant("p1-throughput")
    incident = incident_for(client, store, "S01")
    report = executor.plan(client, incident, [{"action": "SET_CONCURRENT_TASKS",
                                               "target": "S02 Drain", "value": 4}])

    assert not report["ok"] and "no processor named 'S02 Drain'" in report["problems"][0]

    report = executor.plan(client, incident, [{"action": "SET_CONCURRENT_TASKS",
                                               "target": "S01 Consumer", "value": 40}])
    assert not report["ok"] and "1 to 8" in report["problems"][0]


def test_a_route_per_relationship_plans_as_one_step(tenant):
    _, client, store = tenant("p10-iot")
    incident = incident_for(client, store, "S26")
    [fix] = packs.SCENARIOS["S26"].expected
    report = executor.plan(client, incident, [dict(fix, relationship=r) for r in fix["relationship"]])

    # S26's first-round proposal, which validation used to refuse as "appears twice".
    assert report["ok"], report["problems"]
    assert report["steps"] == [dict(fix, relationship=sorted(fix["relationship"]))]


def test_flow_edit_dry_run_records_commands(tenant):
    world, client, store = tenant("p1-throughput")
    incident = incident_for(client, store, "S01")
    proposal = proposal_for(store, incident, packs.SCENARIOS["S01"].expected)
    before = client.flow_facts(incident["deployment_crn"], incident["deployed_flow_crn"])

    action = executor.execute(client, store, incident, proposal, dry_run=True)

    assert action["status"] == "DRY_RUN" and action["dry_run"] == 1
    assert [a[1] for a in action["argv"]] == ["import-flow-definition-version",
                                              "change-flow-version-in-deployment"]
    assert executor.NEW_VERSION in action["argv"][1]
    assert store.incident(incident["id"])["status"] == "DRY_RUN"
    after = client.flow_facts(incident["deployment_crn"], incident["deployed_flow_crn"])
    assert after["flow_version_crn"] == before["flow_version_crn"]


def test_flow_edit_armed_deploys_a_new_version(tenant):
    world, client, store = tenant("p1-throughput")
    incident = incident_for(client, store, "S01")
    proposal = proposal_for(store, incident, packs.SCENARIOS["S01"].expected)

    action = executor.execute(client, store, incident, proposal, dry_run=False,
                              wait=lambda: world.settle(incident["deployment_crn"]))

    assert action["status"] == "APPLIED", action["error"]
    assert action["to_flow_version_crn"] and action["to_flow_version_crn"] != action["from_flow_version_crn"]
    assert store.incident(incident["id"])["status"] == "VERIFYING"
    facts = client.flow_facts(incident["deployment_crn"], incident["deployed_flow_crn"])
    assert facts["flow_version_crn"] == action["to_flow_version_crn"]

    edited = client.flow_definition(action["to_flow_version_crn"])
    consumer = next(p for _, g in walk_groups(edited["flowContents"])
                    for p in g.get("processors") or [] if p["name"] == "S01 Consumer")
    assert consumer["concurrentlySchedulableTaskCount"] == 4
    assert os.path.exists(os.path.join(config.EDITS_DIR, f"{incident['id']}-p{proposal['id']}.json"))
    assert any(v["source"] == "agent" for v in store.flow_versions("p1-throughput"))

    # The same proposal again: the deployment moved on, and the cooldown holds.
    again = executor.execute(client, store, incident, proposal, dry_run=False,
                             wait=lambda: world.settle(incident["deployment_crn"]))
    assert again["status"] == "REFUSED" and "cooldown" in again["error"]


def test_drifted_version_is_refused(tenant):
    world, client, store = tenant("p1-throughput")
    incident = incident_for(client, store, "S01")
    stale = dict(incident, expected_flow_version_crn="crn:cdp:df:mock:flow-version:old")
    report = executor.plan(client, stale, packs.SCENARIOS["S01"].expected)

    assert not report["ok"] and "diagnose again" in report["problems"][0]


def test_a_fix_on_top_of_this_apps_own_fix_is_planned_again_and_keeps_both(tenant, monkeypatch):
    world, client, store = tenant("p1-throughput")
    monkeypatch.setattr(config, "COOLDOWN_SECONDS", 0)
    s01, s02 = incident_for(client, store, "S01"), incident_for(client, store, "S02")
    settle = lambda: world.settle(s01["deployment_crn"])  # noqa: E731
    reviewed = dict(proposal_for(store, s02, packs.SCENARIOS["S02"].expected),
                    diff=executor.plan(client, s02, packs.SCENARIOS["S02"].expected)["diff"])

    first = executor.execute(client, store, s01, proposal_for(store, s01, packs.SCENARIOS["S01"].expected),
                             dry_run=False, wait=settle)
    assert first["status"] == "APPLIED", first["error"]

    # S02 was reviewed on the version before S01's fix; a different diff is not what was reviewed.
    wrong = executor.execute(client, store, s02, dict(reviewed, diff=[]), dry_run=False, wait=settle)
    assert wrong["status"] == "REFUSED" and "no longer makes exactly the reviewed change" in wrong["error"]

    store.update_incident(s02["id"], client.now(), status="PENDING_REVIEW")
    second = executor.execute(client, store, s02, reviewed, dry_run=False, wait=settle)
    assert second["status"] == "APPLIED", second["error"]
    assert second["from_flow_version_crn"] == first["to_flow_version_crn"]

    edited = client.flow_definition(second["to_flow_version_crn"])
    consumer = next(p for _, g in walk_groups(edited["flowContents"])
                    for p in g.get("processors") or [] if p["name"] == "S01 Consumer")
    assert consumer["concurrentlySchedulableTaskCount"] == 4

    # Two of this app's versions back is still its own line; a base it did not come from is not.
    s03 = incident_for(client, store, "S03")
    assert executor.rebased(client, store, dict(s03, expected_flow_version_crn=first["from_flow_version_crn"])
                            )["rebased_by"] == s02["id"]
    foreign = dict(s03, expected_flow_version_crn="crn:cdp:df:mock:flow-version:someone-elses")
    assert "rebased_from" not in executor.rebased(client, store, foreign)
    current = second["to_flow_version_crn"]
    assert not executor.stale(store, dict(s03, expected_flow_version_crn=first["from_flow_version_crn"]), current)
    assert executor.stale(store, foreign, current) and not executor.stale(store, foreign, "")
    refused = executor.execute(client, store, foreign, proposal_for(store, foreign, packs.SCENARIOS["S03"].expected),
                               dry_run=False, wait=settle)
    assert refused["status"] == "REFUSED" and "someone changed it" in refused["error"]


def test_parameter_update(tenant):
    world, client, store = tenant("p5-parameters")
    incident = incident_for(client, store, "S17")
    steps = packs.SCENARIOS["S17"].expected
    report = executor.plan(client, incident, steps)

    assert report["ok"], report["problems"]
    assert report["commands"][0].startswith("cdp dfworkload update-flow-in-deployment")

    # S18's parameter is not S17's to change.
    other = executor.plan(client, incident, packs.SCENARIOS["S18"].expected)
    assert not other["ok"] and "not referenced" in other["problems"][0]

    action = executor.execute(client, store, incident, proposal_for(store, incident, steps),
                              dry_run=False, wait=lambda: world.settle(incident["deployment_crn"]))
    assert action["status"] == "APPLIED", action["error"]

    groups = client.flow_configuration(incident["deployment_crn"], incident["deployed_flow_crn"])
    values = {p["name"]: p.get("value") for g in groups["parameterGroups"] for p in g["parameters"]}
    assert values["s17.target.url"] == "http://localhost:9876/ingest"
    assert values["s18.copies"] == "50"


def test_scale_deployment(tenant):
    world, client, store = tenant("p7-sizing-overprov")
    incident = incident_for(client, store, "S21")
    steps = packs.SCENARIOS["S21"].expected
    report = executor.plan(client, incident, steps)

    assert report["ok"] and report["risk"] == "high"
    assert json.loads(report["commands"][0].split("--cluster-size ")[1].split(" --")[0].strip("'")) == \
        {"name": "EXTRA_SMALL"}

    action = executor.execute(client, store, incident, proposal_for(store, incident, steps),
                              dry_run=False, wait=lambda: world.settle(incident["deployment_crn"]))
    assert action["status"] == "APPLIED", action["error"]

    sizing = client.deployment_configuration(incident["deployment_crn"])
    assert sizing["clusterSize"]["name"] == "EXTRA_SMALL" and sizing["staticNodeCount"] == 1

    same = executor.plan(client, incident, steps)
    assert not same["ok"] and "already" in same["problems"][0]


def test_unarmed_default_is_a_dry_run(tenant, monkeypatch):
    world, client, store = tenant("p7-sizing-overprov")
    monkeypatch.setattr(config, "ARMED", False)
    incident = incident_for(client, store, "S21")
    action = executor.execute(client, store, incident,
                              proposal_for(store, incident, packs.SCENARIOS["S21"].expected))

    assert action["status"] == "DRY_RUN"
    assert client.deployment_configuration(incident["deployment_crn"])["clusterSize"]["name"] == "MEDIUM"


@pytest.mark.parametrize("deployment", ["crn:cdp:df:us-west-1:mock:deployment:someone-else"])
def test_foreign_deployment_is_refused(tenant, deployment):
    world, client, store = tenant("p7-sizing-overprov")
    incident = incident_for(client, store, "S21")
    incident = dict(incident, deployment_crn=deployment)
    action = executor.execute(client, store, incident,
                              proposal_for(store, incident, packs.SCENARIOS["S21"].expected), dry_run=True)

    assert action["status"] == "REFUSED"
