"""
Fix track records (rag/priors.py): how often each fix worked on the
incident's group and on other groups with the same symptom, on the
review and as a warning when it keeps failing. The agent never sees them.
"""

import config
from nifi_healer import llm
from nifi_healer.agent import graph
from nifi_healer.eval import replay, scorecard as sc
from nifi_healer.rag import priors
from nifi_healer.ui import views

from test_memory import FixLLM, _judged, fleet, replayed  # noqa: F401 (fixtures)

# Concurrency on a processor whose run schedule is the bottleneck: approved, applied, and no help.
TRAP = [{"action": "SET_CONCURRENT_TASKS", "target": "S02 Drain", "value": 8}]


def test_a_fix_that_keeps_failing_is_flagged(replayed):
    model = FixLLM(TRAP)
    llm.set_chat_model(model)
    replay.run(["S02"], trials=3)
    snapshot = replay.load()
    found = replay.cases(snapshot)

    assert [c["result"] for c in found] == [sc.NO_HELP] * 3
    assert not any("Track record" in p or "worked (" in p for p in model.prompts)

    # 8 tasks every 10 sec is short of the intake: sent back twice, then reviewed with the warning.
    proposals = snapshot["record"]["proposals"]
    assert [p["status"] for p in proposals] == ["SUPERSEDED", "SUPERSEDED", "VALID"] * 3
    warnings = [p["validation"]["warnings"] for p in proposals if p["status"] == "VALID"]
    assert all(any("below the intake" in w for w in ws) for ws in warnings)

    # One failure is a hint; two on the same group warn the reviewer.
    assert [any("has not worked on this process group" in w for w in ws) for ws in warnings] == \
        [False, False, True]
    assert "this fix has not worked on this process group before: 0 of 2 worked (2 didn't help)" in warnings[2]


def test_tries_count_what_ran_and_what_was_rejected(fleet):
    world, client, store, incidents = fleet("p1-throughput")
    llm.set_chat_model(FixLLM())

    _judged(world, client, store, incidents["S01"])
    graph.start(incidents["S02"]["id"])
    graph.resume(incidents["S02"]["id"], {"decision": "reject", "reviewer": "test", "reason": "not now"})
    found = {t["process_group"]: t for t in priors.tries(store.record())}

    s01, s02 = found[incidents["S01"]["process_group"]], found[incidents["S02"]["process_group"]]
    assert (s01["outcome"], s01["group_fix"], s01["fleet_fix"]) == \
        (sc.WORKED, "SET_CONCURRENT_TASKS 'S01 Consumer'", "SET_CONCURRENT_TASKS")
    assert (s02["outcome"], s02["group_fix"]) == ("rejected", "SET_RUN_SCHEDULE 'S02 Drain'")
    assert s01["symptom"] == s02["symptom"] == "Flow Files Queued over its threshold"
    assert priors.tries(store.record(), skip=incidents["S01"]["id"]) == [s02]

    # S03 has the same symptom: both are records from other groups.
    graph.start(incidents["S03"]["id"])
    review = graph.pending(incidents["S03"]["id"])
    shown = views.review(store, client, incidents["S03"]["id"])["proposal"]
    assert "**Track record of these fixes:**\n" \
        "- On other process groups, with Flow Files Queued over its threshold, SET_CONCURRENT_TASKS: 1 of 1 worked\n" \
        "- On other process groups, with Flow Files Queued over its threshold, SET_RUN_SCHEDULE: 0 of 1 worked " \
        "(1 rejected)" in shown
    assert review["plan"]["warnings"] == []


def test_the_review_shows_the_record_as_it_stands_now(fleet):
    world, client, store, incidents = fleet("p1-throughput")
    llm.set_chat_model(FixLLM())
    graph.start(incidents["S02"]["id"])

    assert "Track record of these fixes:** none yet" in views.review(store, client, incidents["S02"]["id"])["proposal"]

    # S01's fix is judged after S02's proposal was made; S02's open review shows it.
    _judged(world, client, store, incidents["S01"])
    assert "SET_CONCURRENT_TASKS: 1 of 1 worked" in views.review(store, client, incidents["S02"]["id"])["proposal"]


def test_priors_off_and_the_top_k(fleet, monkeypatch):
    world, client, store, incidents = fleet("p1-throughput")
    llm.set_chat_model(FixLLM())
    _judged(world, client, store, incidents["S01"])
    graph.start(incidents["S02"]["id"])
    graph.resume(incidents["S02"]["id"], {"decision": "reject", "reviewer": "test", "reason": "not now"})

    assert len(priors.for_incident(incidents["S03"], store, k=1)) == 1

    monkeypatch.setattr(config, "PRIORS", False)
    assert priors.for_incident(incidents["S03"], store) == []


def test_symptom_and_warning():
    signals = [{"type": "trend_to_breach", "severity": "warning", "metric": "queued_count"},
               {"type": "flow_imbalance", "severity": "critical", "metric": "flowfiles_in"},
               {"type": "kpi_breach", "severity": "critical", "metric": "nonesuch"}]

    assert priors.symptom({"signals": signals}) == "nonesuch over its threshold"
    assert priors.symptom({"signals": signals[:2]}).endswith("out of balance (in vs out)")
    assert priors.symptom({}) == "no signal"

    row = {"scope": priors.GROUP, "fix": "SET_RETRY_POLICY 'X'", "worked": 0, "no_help": 1, "worse": 0,
           "rejected": 1, "tries": 2}
    assert priors.warning([row], [{"action": "SET_RETRY_POLICY", "target": "X", "value": 3}]) == \
        "this fix has not worked on this process group before: 0 of 2 worked (1 didn't help, 1 rejected)"
    assert priors.warning([dict(row, worked=1, tries=3)], [{"action": "SET_RETRY_POLICY", "target": "X"}]) == ""
    assert priors.warning([row], [{"action": "SET_RETRY_POLICY", "target": "Y"}]) == ""
