"""
The offline replay (eval/replay.py): rounds of scenarios on an isolated
mock tenant, auto-approved there only, scored like the live agent.
"""

import json
import time

import pytest

import config
from flows import packs
from nifi_healer import llm
from nifi_healer.agent import graph, prompts
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
                                          "controls": 1, "quiet": 1, "watched": 0, "cleared": 0,
                                          "fixes_to_clear": None}

    # p5's other faults opened incidents too, but were not trials.
    assert any(i["scenario"] == "S17" for i in snapshot["record"]["incidents"])

    # A second run keeps the first and is told apart from it.
    replay.run(["S01"], oracle=True)
    again = replay.load()

    assert len(again["runs"]) == 2 and again["now"] >= snapshot["now"]
    assert len(sc.table(replay.cases(again), "Replay run")) == 2
    assert "S01 Slow Consumer" in replay.report(again, again["runs"][1]["id"])


def test_a_compound_fault_clears_after_its_second_fix(replayed):
    run = replay.run(["S30", "S31"], oracle=True, fixes=3)
    snapshot = replay.load()
    trials = {t["scenario"]: t for t in snapshot["runs"][0]["trials"]}
    found = replay.cases(snapshot)

    assert snapshot["runs"][0]["fixes"] == 3
    assert all(t["fixes"] == 2 and t["cleared"] and len(t["incidents"]) == 2 for t in trials.values())
    assert trials["S31"]["incident"] == trials["S31"]["incidents"][0]

    # The second incident got the second fault's fix, and was scored against the scenario.
    assert sorted((c["scenario"], c["fix"], c["match"]) for c in found) == [
        ("S30", 1, sc.EXPECTED), ("S30", 2, sc.ACCEPTABLE), ("S31", 1, sc.EXPECTED), ("S31", 2, sc.ACCEPTABLE)]
    assert all(c["applied"] for c in found)

    seen = replay.detection(snapshot)
    assert (seen["watched"], seen["cleared"], seen["fixes_to_clear"]) == (2, 2, 2)
    assert "cleared 2 of 2 detected faults, after 2 fix(es) (median)" in replay.report(snapshot, run)


def test_the_oracle_fixes_a_compound_fault_one_fault_an_incident():
    oracle, scenario = replay.OracleLLM(), packs.SCENARIOS["S32"]

    assert oracle.steps(scenario, "INC-1") == [scenario.faults[0]]
    assert oracle.steps(scenario, "INC-2") == [scenario.faults[1]]
    assert oracle.steps(scenario, "INC-1") == [scenario.faults[0]]      # asked again, same answer
    assert oracle.steps(scenario, "INC-3") == [scenario.faults[1]]      # no third fault
    assert oracle.steps(packs.SCENARIOS["S01"], "INC-4") == list(packs.SCENARIOS["S01"].expected)

    oracle.forget()
    assert oracle.steps(scenario, "INC-5") == [scenario.faults[0]]


def test_an_invalid_proposal_is_left_at_review(replayed):
    llm.set_chat_model(OutOfBoundsLLM())
    replay.run(["S01"])
    snapshot = replay.load()
    [case] = replay.cases(snapshot)

    assert not case["valid"] and not case["applied"] and case["review"] == sc.UNDECIDED
    assert snapshot["record"]["reviews"] == [] and snapshot["record"]["actions"] == []
    assert graph.pending(case["incident"])


def test_two_runs_side_by_side(replayed, tmp_path):
    replay.run(["S01", "S02"], oracle=True, directory=str(tmp_path / "before"))
    llm.set_chat_model(OutOfBoundsLLM())
    replay.run(["S01"], directory=str(tmp_path / "after"))

    header, s01, s02, total, shared, value, first, worked, same = replay.compare(
        replay.load(str(tmp_path / "before")), replay.load(str(tmp_path / "after"))).splitlines()

    def cells(line):
        return line[32:].split()

    assert header.split()[:4] == ["scenario", "cases", "expected", "value"] and "valid 1st" in header
    assert s01.startswith("S01 Slow Consumer") and cells(s01)[:7] == [
        "1→1", "100%→0%", "100%→0%", "100%→0%", "100%→0%", "1.0→3.0", "100%→-"]
    assert cells(s02)[:2] == ["1→-", "100%→-"] and cells(s02)[-1].endswith("→-")     # S02 not in the after run
    assert total.startswith("all") and cells(total)[:2] == ["1→1", "100%→0%"]     # S01 alone: in both
    assert shared == "all: the 1 scenario(s) in both; only before: S02"
    # One case a side, on the same tenant: a shuffle swaps them half the time, so luck alone makes the whole change.
    assert value == ("value     0 better, 1 worse (S01), 0 same; 100%→0% over 1 scenario(s), -100 pts, "
                     "luck alone ±100 pts (1 paired): within luck")
    assert first.startswith("valid 1st 0 better, 1 worse (S01)") and worked == "worked    nothing to compare"
    # The same first prompt (a stepped clock, NFH-84), answered differently by another model.
    assert same == "same tenant: 1 pair(s), 1 of 1 with the same prompt, and 0 of 1 of those with the same first proposal"


def case(scenario, right):
    return {"scenario": scenario, "proposed": True, "match": sc.EXPECTED if right else sc.OTHER,
            "valid_first": True, "result": sc.WORKED if right else sc.NO_HELP}


def test_luck_is_the_change_a_shuffle_makes():
    counted = dict(replay.TALLIED)["value"]
    ten = {f"S{n:02}": ([case(f"S{n:02}", False)], [case(f"S{n:02}", True)]) for n in range(1, 11)}
    two = {s: pair for s, pair in list(ten.items())[:2]}
    same = {"S01": ([case("S01", True)] * 3, [case("S01", True)] * 3)}

    assert replay.tally("value", counted, ten).endswith("+100 pts, luck alone ±60 pts: beyond luck")
    assert replay.tally("value", counted, two).endswith("+100 pts, luck alone ±100 pts: within luck")
    assert replay.tally("value", counted, same).endswith("0 better, 0 worse, 1 same; 100%→100% over 1 scenario(s), "
                                                         "+0 pts, luck alone ±0 pts: within luck")


def test_a_pair_on_the_same_tenant_is_dealt_together():
    # Each tenant decides its case alike both times: paired, no shuffle moves the rate; dealt freely, they do.
    counted = dict(replay.TALLIED)["value"]

    def on(world, scenario, right):
        return dict(case(scenario, right), world=world, fix=1)

    paired = {f"S{n:02}": ([on([0, n], f"S{n:02}", True), on([1, n], f"S{n:02}", False)],
                           [on([1, n], f"S{n:02}", False), on([0, n], f"S{n:02}", True)]) for n in range(1, 6)}
    loose = {s: ([dict(c, world=None) for c in b], [dict(c, world=None) for c in a]) for s, (b, a) in paired.items()}

    assert replay.tally("value", counted, paired).endswith("+0 pts, luck alone ±0 pts (10 paired): within luck")
    assert not replay.tally("value", counted, loose).endswith("±0 pts: within luck")
    assert replay.sameness({}, {}, loose).startswith("same tenant: no pairs")


def test_a_stepped_clock_moves_only_when_told(replayed):
    world = replay.isolate(config.REPLAY_DIR)
    world.reset(epoch=replay.EPOCH)
    start = world.now()
    time.sleep(0.05)

    assert start == replay.EPOCH and world.now() == start
    clients.get_client().sleep(20)          # a wait on the mock, such as a deployment's
    assert world.now() == start + 20


def test_every_run_sees_the_same_rounds(replayed, tmp_path):
    for name in ("one", "two"):
        replay.run(["S01", "S03"], 2, str(tmp_path / name), oracle=True)

    one, two = (replay.load(str(tmp_path / name)) for name in ("one", "two"))
    shown = [[(t["scenario"], t["round"], t["world"], t["prompts"]) for t in r["runs"][0]["trials"]] for r in (one, two)]

    assert shown[0] == shown[1] and all(t[3][0] for t in shown[0])
    assert shown[0][0][2] != shown[0][2][2] and shown[0][0][3] != shown[0][2][3]     # round 2 is another tenant
    assert replay.sameness(one, two, {s: tuple([c for c in replay.cases(r) if c["scenario"] == s] for r in (one, two))
                                      for s in ("S01", "S03")}) == (
        "same tenant: 4 pair(s), 4 of 4 with the same prompt, and 4 of 4 of those with the same first proposal")

    # A second run in a directory is a day on, apart from the first's incidents in the store.
    replay.run(["S01"], 1, str(tmp_path / "one"), oracle=True)
    again = replay.load(str(tmp_path / "one"))["runs"]
    assert again[1]["trials"][0]["at"] - again[0]["trials"][0]["at"] == replay.DAY
    assert again[1]["trials"][0]["world"] == again[0]["trials"][0]["world"]


class TooManyTasksLLM(OutOfBoundsLLM):
    """Always proposes 99 concurrent tasks, as a well-formed step that validation sends back."""

    def invoke(self, messages):
        return type("Reply", (), {"content": json.dumps({
            "diagnosis": "too few tasks", "root_cause_component": "S01 Consumer",
            "steps": [{"action": "SET_CONCURRENT_TASKS", "target": "S01 Consumer", "value": 99}],
            "rationale": "more is better", "citations": [], "confidence": 0.5, "expected_effect": "faster"})})()


def test_what_the_agent_proposed(replayed):
    llm.set_chat_model(TooManyTasksLLM())
    replay.run(["S01", "S02"])
    snapshot = replay.load()
    shown = replay.show(snapshot, ["S01"])

    assert shown.startswith("=== S01 Slow Consumer · round 1 · ") and "=== S02" not in shown
    assert "  designed fix: SET_CONCURRENT_TASKS S01 Consumer" in shown
    assert [line.strip() for line in shown.splitlines() if line.startswith("  attempt")] == [
        "attempt 1 · SUPERSEDED", "attempt 2 · SUPERSEDED", "attempt 3 · INVALID"]
    assert shown.count('steps: SET_CONCURRENT_TASKS S01 Consumer {"value": 99}') == 3
    assert shown.count("    sent back:") == 3 and "more is better" in shown
    assert "=== S02" in replay.show(snapshot)


def test_what_each_draw_would_have_been_sent_back_for(replayed):
    llm.set_chat_model(TooManyTasksLLM())
    replay.run(["S01"], samples=3)
    shown = replay.show(replay.load())

    # The first attempt's three draws, each out of bounds; the retries are one draw each.
    assert shown.count('    drawn: SET_CONCURRENT_TASKS S01 Consumer {"value": 99}\n      would be sent back:') == 3
    assert "· valid" not in shown


class PromptKeepingOracle(replay.OracleLLM):
    def __init__(self):
        super().__init__()
        self.prompts = []

    def invoke(self, messages):
        self.prompts.append("\n".join(prompts.text_of(m) for m in messages))
        return super().invoke(messages)


def test_the_agent_reads_the_history_the_app_backfills(replayed):
    oracle = PromptKeepingOracle()
    llm.set_chat_model(oracle)
    replay.run(["S01"])

    # A source back pressure throttles shows its earlier peak, as the app's would.
    [prompt] = oracle.prompts
    assert "S01 Source (a source): 1/s out, up to" in prompt and "in the last 2 h" in prompt


class SlowOracle(PromptKeepingOracle):
    """Takes wall time to answer, as an LLM does, and notes the mock's clock either side."""

    def __init__(self):
        super().__init__()
        self.clock = []

    def invoke(self, messages):
        before = clients.get_client().now()
        time.sleep(0.2)
        self.clock.append((before, clients.get_client().now()))

        return super().invoke(messages)


def test_the_clock_stops_while_the_agent_thinks(replayed):
    # At 20x, a minute an LLM takes is 20 sim minutes in which nothing samples
    # the flows: the incidents queued behind it read "no data" for every KPI.
    oracle = SlowOracle()
    llm.set_chat_model(oracle)
    replay.run(["S01", "S02"])

    assert len(oracle.clock) == 2 and len({at for pair in oracle.clock for at in pair}) == 1
    assert all("no data in the last" not in p for p in oracle.prompts)
    assert all(c["applied"] for c in replay.cases(replay.load()))       # and runs again to apply and judge


def test_it_isolates_a_live_project(replayed, tmp_path):
    # A CAI project's environment: live client and CRNs, the app's data dir, disarmed.
    config.DF_CLIENT, config.MOCK, config.ARMED = "live", False, False
    config.SERVICE_CRN, config.ENVIRONMENT_CRN = "crn:cdp:df:live:service:x", "crn:cdp:environments:live:env:x"
    config.DB_PATH = str(tmp_path / "healer.db")
    config.COUNT_WINDOW_SECONDS = 300        # live charts processor counts per 5 minutes
    replay.isolate(replayed)

    assert config.MOCK and isinstance(clients.get_client(), MockClient)
    assert config.COUNT_WINDOW_SECONDS == 1  # the mock's are per second: a rate read live would be 300x low
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


def test_it_waits_as_long_as_the_slowest_fault_takes_to_show():
    assert replay.window(["S01"]) == replay.DETECT_SECONDS
    assert replay.window(["S01"], 7200) == 7200
    assert replay.window(["S01", "S39"]) == packs.SCENARIOS["S39"].detect_within == 86400


def test_the_critic_lets_the_design_fixes_through(replayed):
    oracle = PromptKeepingOracle()
    llm.set_chat_model(oracle)
    replay.run(["S01"], oracle=False, critic=True)
    snapshot = replay.load()
    [case] = replay.cases(snapshot)

    assert snapshot["runs"][0]["critic"] and snapshot["runs"][0]["agent_version"].endswith(
        f"/critic:{prompts.CRITIC_VERSION}")
    assert case["match"] == sc.EXPECTED and case["applied"]
    assert sum(prompts.CRITIC_SYSTEM in p for p in oracle.prompts) == 1
    assert "    second reading:\n      no objections" in replay.show(snapshot)
