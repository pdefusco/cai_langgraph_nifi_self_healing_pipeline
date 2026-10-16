"""
The chaos injector (nifi_healer/chaos.py): it plants a fault again only
on a quiet deployment, once the scenario has rested, at most one per
tick and within its own caps; a plant swaps back that one group (or its
parameters), is in the ledger without values, and shows as chaos, not
as an operator's change.
"""

import json

import pytest

import config
from flows import packs
from flows.scenarios.p5_parameters import CORRECT_URL
from nifi_healer import chaos, fleet, llm
from nifi_healer.agent import graph
from nifi_healer.flow.inventory import find_group
from nifi_healer.kpi import changes
from nifi_healer.runtime.poller import Poller
from nifi_healer.store.db import OPEN_STATUSES
from nifi_healer.ui import monitor, views

from test_memory import FixLLM, _judged, fleet as fleet_  # noqa: F401 (a fixture)


@pytest.fixture
def on(monkeypatch):
    monkeypatch.setattr(config, "CHAOS", True)
    monkeypatch.setattr(config, "CHAOS_REST", 0)
    monkeypatch.setattr(config, "CHAOS_SPACING", 0)


def _quiet(client, store):
    """Every open incident closed as if a person had dismissed it."""

    for incident in store.incidents(OPEN_STATUSES, limit=1000):
        store.update_incident(incident["id"], client.now(), status="NO_ACTION")


def _wait(world, pack):
    crn = fleet.pack(pack)["deployment_crn"]

    return lambda: world.settle(crn)


def _deployed(client, pack):
    entry = fleet.pack(pack)

    return client.flow_definition(client.flow_facts(entry["deployment_crn"],
                                                    entry["deployed_flow_crn"])["flow_version_crn"])


def _parameters(client, pack):
    entry = fleet.pack(pack)
    configuration = client.flow_configuration(entry["deployment_crn"], entry["deployed_flow_crn"])

    return {p["name"]: p["value"] for g in configuration["parameterGroups"] for p in g["parameters"]}


def _set_parameters(world, client, pack, values):
    entry = fleet.pack(pack)
    crn, flow = entry["deployment_crn"], entry["deployed_flow_crn"]
    configuration = client.flow_configuration(crn, flow)
    group = configuration["parameterGroups"][0]["name"]
    client.dfw("update-flow-in-deployment", "--deployment-crn", crn, "--deployed-flow-crn", flow,
               "--configuration-version", configuration["configurationVersion"], "--parameter-groups",
               json.dumps([{"name": group, "parameters": [{"name": k, "value": v} for k, v in values.items()]}]))
    world.settle(crn)


# ---------------------------------------------------------------------
# Picking a candidate
# ---------------------------------------------------------------------


def test_it_waits_for_a_quiet_deployment_and_a_rested_scenario(fleet_, on, monkeypatch):
    world, client, store, incidents = fleet_("p1-throughput")
    found = chaos.candidates(store, client.now())

    assert {c["scenario"] for c in found} == {"S01", "S02", "S03", "S04"}
    assert all(c["blocked"].startswith("INC-") and " is " in c["blocked"] for c in found)

    _quiet(client, store)
    monkeypatch.setattr(config, "CHAOS_REST", 3600)
    world.skip(600)
    found = {c["scenario"]: c for c in chaos.candidates(store, client.now())}

    for sid, candidate in found.items():
        if sid in incidents:  # rests from when its incident closed
            assert candidate["blocked"] == "resting another 50 min"

        else:
            assert candidate["blocked"] == ""

    world.skip(3000)
    assert all(not c["blocked"] for c in chaos.candidates(store, client.now()))


def test_a_control_is_never_a_candidate(tenant, on):
    _, client, store = tenant("p5-parameters")

    assert [c["scenario"] for c in chaos.candidates(store, client.now())] == ["S17", "S18"]


def test_a_fix_waiting_to_approve_itself_keeps_the_deployment_busy(fleet_, on):
    world, client, store, incidents = fleet_("p1-throughput")
    s01 = incidents["S01"]
    _quiet(client, store)
    store.add_auto_approval(s01["id"], None, "SET_CONCURRENT_TASKS 'S01 Consumer'", "test",
                            client.now() + 600, client.now())

    assert {c["blocked"] for c in chaos.candidates(store, client.now())} == \
        {f"{s01['id']}'s fix is waiting to approve itself"}


def test_the_per_day_cap_and_the_spacing(tenant, on, monkeypatch):
    _, client, store = tenant("p5-parameters")
    crn = fleet.pack("p5-parameters")["deployment_crn"]
    now = client.now()
    monkeypatch.setattr(config, "CHAOS_SPACING", 7200)

    store.add_injection("p5-parameters", crn, "S17", "PARAMETER", chaos.DRY_RUN, now - 600)
    assert chaos.candidates(store, now)[0]["blocked"] == \
        "the last plant on this deployment was 10 min ago (at least 120 min apart)"

    # What wrote nothing counts toward neither.
    store.add_injection("p5-parameters", crn, "S18", "PARAMETER", chaos.UNCHANGED, now - 60)
    store.add_injection("p5-parameters", crn, "S18", "PARAMETER", chaos.REFUSED, now - 30)
    assert "10 min ago" in chaos.candidates(store, now)[0]["blocked"]

    for hours in (20, 12):
        store.add_injection("p5-parameters", crn, "S18", "PARAMETER", chaos.FAILED, now - hours * 3600)

    later = now + 7200
    assert {c["blocked"] for c in chaos.candidates(store, later)} == \
        {"3 plants on this deployment in the last day (at most 3)"}
    assert not any(c["blocked"] for c in chaos.candidates(store, now + 12 * 3600))


# ---------------------------------------------------------------------
# Planting
# ---------------------------------------------------------------------


def test_a_plant_swaps_back_its_group_only_and_shows_as_chaos(fleet_, on, monkeypatch):
    world, client, store, incidents = fleet_("p1-throughput")
    llm.set_chat_model(FixLLM())
    _judged(world, client, store, incidents["S01"])
    _quiet(client, store)
    entry = fleet.pack("p1-throughput")
    crn = entry["deployment_crn"]
    fixed = _deployed(client, "p1-throughput")
    baseline = client.flow_definition(entry["baseline_flow_version_crn"])
    group = packs.SCENARIOS["S01"].group_name
    assert find_group(fixed, group) != find_group(baseline, group)
    world.skip(600)

    row = chaos.tick(client, store, wait=_wait(world, "p1-throughput"))

    assert row["scenario"] == "S01" and row["status"] == chaos.PLANTED, row
    assert [s["kind"] for s in row["steps"]] == ["import", "version"] and all(s["performed"] for s in row["steps"])
    planted = _deployed(client, "p1-throughput")
    assert client.flow_facts(crn, entry["deployed_flow_crn"])["flow_version_crn"] == row["to_flow_version_crn"]
    assert row["from_flow_version_crn"] != row["to_flow_version_crn"]

    for scenario in packs.PACKS["p1-throughput"].scenarios:
        want = baseline if scenario.id == "S01" else fixed
        assert find_group(planted, scenario.group_name) == find_group(want, scenario.group_name), scenario.id

    # One plant per tick: S04 (never fixed, so the longest waiting) was tried first and found its fault in
    # place; the next tick finds S02's and S03's in place too, and S01's episode still open.
    unchanged = lambda: {r["scenario"] for r in store.injections(deployment_crn=crn)  # noqa: E731
                         if r["status"] == chaos.UNCHANGED}
    assert unchanged() == {"S04"}
    assert chaos.tick(client, store, wait=_wait(world, "p1-throughput")) is None
    assert unchanged() == {"S02", "S03", "S04"}
    assert {c["scenario"]: c["blocked"] for c in chaos.candidates(store, client.now())}["S01"] == \
        "its last plant is still open"

    # Drift handling knows the version as this app's; the change is chaos, not an operator's.
    versions = {v["crn"]: v for v in store.flow_versions("p1-throughput")}
    assert versions[row["to_flow_version_crn"]]["source"] == "chaos"
    world.skip(1800)
    poller = Poller(client, store)
    poller.poll_events("p1-throughput", entry)
    found = changes.attribute(store.changes(crn), store.actions(deployment_crn=crn), store.injections(deployment_crn=crn))
    assert [c["source"] for c in found if c["kind"] == "flow_version"] == ["agent", "chaos"]
    assert "operator" not in {c["source"] for c in found}
    segments = monitor.model(store, "p1-throughput", entry, client.now())["segments"]
    assert segments[-1]["who"] == "Chaos" and segments[-1]["what"].startswith("Re-planted S01")

    # The fault is back: an incident opens, and its fix applies (a chaos version is not drift).
    opened = []
    Poller(client, store, on_incident=opened.append).poll_now()
    again = next(i for i in opened if i["scenario"] == "S01")
    assert again["opened_at"] >= row["at"]
    graph.start(again["id"])
    graph.resume(again["id"], {"decision": "approve", "reviewer": "test"})
    assert [a["status"] for a in store.actions(incident_id=again["id"])] == ["APPLIED"]


def test_parameters_are_planted_by_name_and_their_values_never_kept(tenant, on, monkeypatch):
    world, client, store = tenant("p5-parameters")
    monkeypatch.setattr(config, "ARMED", True)
    faulty = _parameters(client, "p5-parameters")
    _set_parameters(world, client, "p5-parameters", {"s17.target.url": CORRECT_URL})
    world.skip(600)

    row = chaos.tick(client, store, wait=_wait(world, "p5-parameters"))

    assert (row["scenario"], row["status"]) == ("S17", chaos.PLANTED)
    assert [s["kind"] for s in row["steps"]] == ["parameters"]
    assert _parameters(client, "p5-parameters")["s17.target.url"] == faulty["s17.target.url"]

    kept = json.dumps(store.injections(), default=str)
    assert "s17.target.url" in kept
    assert CORRECT_URL not in kept and faulty["s17.target.url"] not in kept


def test_disarmed_it_is_a_dry_run_and_pause_stops_it(tenant, on, monkeypatch):
    world, client, store = tenant("p5-parameters")
    monkeypatch.setattr(config, "ARMED", True)
    _set_parameters(world, client, "p5-parameters", {"s17.target.url": CORRECT_URL})
    monkeypatch.setattr(config, "ARMED", False)

    chaos.pause(store, "ops", client.now())
    assert chaos.tick(client, store) is None and store.injections() == [] and chaos.state(store) == "paused"
    assert chaos.since(store) is None  # the soak starts on the first tick that runs

    chaos.unpause(store, "ops", client.now())
    row = chaos.tick(client, store, wait=_wait(world, "p5-parameters"))

    assert row["status"] == chaos.DRY_RUN and row["steps"] == [
        {"label": row["steps"][0]["label"], "kind": "parameters", "performed": False}]
    assert _parameters(client, "p5-parameters")["s17.target.url"] == CORRECT_URL
    assert chaos.since(store) == pytest.approx(row["at"], abs=1)
    assert "**on** since" in views._chaos_state(store, client.now())

    monkeypatch.setattr(config, "CHAOS", False)
    assert chaos.tick(client, store) is None and chaos.state(store) == "off"


def test_a_failure_keeps_no_command_or_output():
    error = SystemExit("p5-parameters: update failed: rejected\n  command: cdp df ... s17.target.url=secret\n"
                       "  stderr: value secret is bad")

    assert chaos._safe(error) == "p5-parameters: update failed: rejected"


# ---------------------------------------------------------------------
# Changes
# ---------------------------------------------------------------------


def test_a_planted_change_is_chaos_and_an_unmatched_one_still_shows():
    injection = {"id": 1, "scenario": "S01", "at": 1000.0, "finished_at": 1100.0, "steps": [
        {"label": "S01 -> baseline: import", "kind": "import", "performed": True},
        {"label": "S01 -> baseline", "kind": "version", "performed": True},
        {"label": "parameters", "kind": "parameters", "performed": False}]}
    events = [{"event_crn": "e1", "t": 1150.0, "kind": "flow_version", "name": "Change Flow Version Completed"},
              {"event_crn": "e2", "t": 5000.0, "kind": "flow_version", "name": "Change Flow Version Completed"}]

    found = changes.attribute(events, [], [injection])

    assert [(c["event_crn"], c["source"]) for c in found] == [("e1", "chaos"), ("e2", "operator")]
    assert changes.attribute([], [], [injection])[0]["name"] == "chaos S01"
