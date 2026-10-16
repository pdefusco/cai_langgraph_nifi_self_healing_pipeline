"""
Fault episodes (eval/soak.py) over a scripted soak on the mock: chaos
plants S20 again after its fix, the poller detects it, a person approves
the fix, its KPIs clear; it rests, and chaos plants it once more. The
numbers are the script's.
"""

import pytest

import config
from flows import packs
from nifi_healer import chaos, fleet, llm
from nifi_healer.agent import graph
from nifi_healer.eval import scorecard as sc, soak
from nifi_healer.runtime.poller import Poller

from test_chaos import _quiet, _wait
from test_memory import FixLLM, _judged, fleet as fleet_  # noqa: F401 (a fixture)

PACK = "p6-sizing-cpu"
STEP = 120


@pytest.fixture
def soaked(fleet_, monkeypatch):
    monkeypatch.setattr(config, "CHAOS", True)
    monkeypatch.setattr(config, "CHAOS_REST", 0)
    monkeypatch.setattr(config, "CHAOS_SPACING", 0)
    world, client, store, incidents = fleet_(PACK)
    llm.set_chat_model(FixLLM())
    _judged(world, client, store, incidents["S20"])
    _quiet(client, store)

    return world, client, store


def _until(world, poller, found, most: float):
    """Skip and poll a step at a time until found() is truthy (its value), or `most` seconds have gone by."""

    for _ in range(int(most // STEP)):
        world.skip(STEP)
        poller.poll_now()
        poller.evaluate_outcomes()
        value = found()

        if value:
            return value

    return None


def _opened(store, row):
    return next((i for i in store.incidents(pack=PACK) if i["opened_at"] >= row["at"]
                 and i.get("kind") != "rollback"), None)


def test_a_planted_fault_is_detected_fixed_cleared_and_planted_again(soaked, monkeypatch):
    world, client, store = soaked
    scenario = packs.SCENARIOS["S20"]
    poller = Poller(client, store)
    crn = fleet.pack(PACK)["deployment_crn"]
    world.skip(600)

    first = chaos.tick(client, store, wait=_wait(world, PACK))
    assert (first["scenario"], first["status"]) == ("S20", chaos.PLANTED)
    [episode] = soak.episodes(store, client.now())
    assert episode["status"] == soak.OPEN and episode["detected_at"] is None and episode["ended_at"] is None

    incident = _until(world, poller, lambda: _opened(store, first), scenario.detect_within)
    assert incident, "the planted fault was not detected"
    graph.start(incident["id"])
    graph.resume(incident["id"], {"decision": "approve", "reviewer": "test"})
    [episode] = soak.episodes(store, client.now())
    assert episode["detected_at"] == incident["opened_at"] and episode["mttd"] == incident["opened_at"] - first["at"]
    assert (episode["decision"], episode["decided_by"], episode["fixes"]) == ("approve", "person", 1)
    assert episode["match"] == sc.EXPECTED and episode["incidents"] == [incident["id"]]

    ended = _until(world, poller, lambda: soak.episodes(store, client.now())[0]["ended_at"],
                   config.VERIFY_SETTLE_SECONDS + 3 * scenario.clear_within + 1800)
    [episode] = soak.episodes(store, client.now())
    fix = store.actions(incident_id=incident["id"])[0]
    assert episode["status"] == soak.CLEARED and ended == episode["cleared_at"] + scenario.clear_within
    assert episode["cleared_at"] >= fix["finished_at"] + config.VERIFY_SETTLE_SECONDS
    assert episode["time_to_clear"] == episode["cleared_at"] - first["at"]

    # Its incident is busy until its verdict is in; then it rests from the episode's end, and is planted again.
    assert chaos.next_candidate(store, client.now())["blocked"] == f"{incident['id']} is verifying"
    assert _until(world, poller, lambda: store.incident(incident["id"])["status"] == "RESOLVED", 7200)
    monkeypatch.setattr(config, "CHAOS_REST", client.now() - ended + 1200)
    assert chaos.tick(client, store) is None
    assert chaos.next_candidate(store, client.now())["blocked"] == "resting another 20 min"
    world.skip(1200)
    second = chaos.tick(client, store, wait=_wait(world, PACK))
    assert (second["scenario"], second["status"]) == ("S20", chaos.PLANTED)

    found = soak.episodes(store, client.now())
    assert [e["injection"] for e in found] == [first["id"], second["id"]]
    assert [e["status"] for e in found] == [soak.CLEARED, soak.OPEN]
    assert soak.false_alarms(store, found, client.now()) == []

    numbers = soak.summary(found, [], chaos.since(store), client.now())
    assert numbers["injections"] == 2 and numbers["open"] == 1
    assert numbers["detected"] == 1.0 and numbers["mttd"] == found[0]["mttd"]
    assert numbers["design_fix"] == 1.0 and numbers["cleared"] == 1.0
    assert numbers["time_to_clear"] == found[0]["time_to_clear"] and numbers["fixes_to_clear"] == 1
    assert numbers["auto"] == 0.0 and numbers["false_alarms"] == 0
    assert numbers["regressions"] == 0 and numbers["rollbacks"] == 0
    assert store.injections(deployment_crn=crn, statuses=(chaos.PLANTED,))[-1]["id"] == second["id"]


def test_an_undetected_plant_is_missed_and_rests_from_its_deadline(soaked):
    world, client, store = soaked
    scenario = packs.SCENARIOS["S20"]
    row = chaos.tick(client, store, wait=_wait(world, PACK))

    # Nothing polls: nothing is detected.
    world.skip(2 * scenario.detect_within + 60)
    [episode] = soak.episodes(store, client.now())

    assert (episode["status"], episode["ended_at"]) == (soak.MISSED, row["at"] + 2 * scenario.detect_within)
    assert soak.summary([episode], [], chaos.since(store), client.now())["detected"] == 0.0
    assert chaos.next_candidate(store, client.now())["blocked"] == ""


def test_what_no_fault_explains_is_a_false_alarm(soaked):
    world, client, store = soaked
    row = chaos.tick(client, store, wait=_wait(world, PACK))
    incident = _until(world, Poller(client, store), lambda: _opened(store, row), 1800)
    assert incident, "the planted fault was not detected"

    # The episode ended 10 min after its incident opened: what opens on its group after that, or on a
    # control's at any time, no fault explains.
    episode = dict(soak.episodes(store, client.now())[0], ended_at=incident["opened_at"] + 600)
    control = packs.SCENARIOS["S19"]
    record = store.record(chaos.since(store))
    record["incidents"] += [
        dict(incident, id="INC-later", opened_at=incident["opened_at"] + 1200),
        dict(incident, id="INC-control", pack="p5-parameters", scenario="S19", process_group=control.group_name,
             opened_at=incident["opened_at"] + 60)]
    store.record = lambda since=0.0: record

    alarms = soak.false_alarms(store, [episode], client.now())

    assert [(a["incident"], a["group"], a["why"]) for a in alarms] == [
        ("INC-control", control.group_name, "a control has no fault"),
        ("INC-later", packs.SCENARIOS["S20"].group_name, "no fault was planted on its group then")]
    assert soak.summary([episode], alarms, chaos.since(store), chaos.since(store) + 43200)["false_alarms_per_day"] == 4
