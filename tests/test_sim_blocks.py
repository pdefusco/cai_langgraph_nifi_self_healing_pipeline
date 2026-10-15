"""
The building blocks the larger packs need: CRON scheduling, and the
DistributeLoad, ControlRate, SplitText and ReplaceText processors, in
the builder and the simulator; and what a scenario declares about how
its fault presents.
"""

import calendar
import time

import pytest

from flows.builder.lib import ProcessGroup, check, flow_definition
from flows.packs import SCENARIOS
from flows.scenarios.base import Scenario
from flows.scenarios.common import generator, sink
from nifi_healer.sim import cron
from nifi_healer.sim.engine import DT, Simulation
from nifi_healer.sim.model import compile_flow

MONDAY = calendar.timegm((2026, 10, 12, 0, 0, 0))     # 00:00 UTC


def _flow(build) -> dict:
    root = ProcessGroup("root", "root")
    build(root.group("g", "G"))

    return flow_definition(root)


def _simulation(definition: dict, start: float = MONDAY - 1) -> Simulation:
    # Starting a second early, each step covers one aligned 15 s window.
    assert check(definition)["problems"] == []

    return Simulation(definition, {}, "EXTRA_SMALL", 1, start)


def _per_step(sim: Simulation, name: str, metric: str = "flowfiles_in") -> float:
    return sim.latest(f"processor|{name}|{metric}") * DT


def test_cron_parses_the_shared_subset():
    every_quarter = cron.parse("0 */15 * * * ?")
    assert every_quarter.seconds == {0} and every_quarter.minutes == {0, 15, 30, 45}

    office = cron.parse("0 0 9-17/4 ? * MON-FRI")
    assert office.hours == {9, 13, 17} and office.weekdays == {0, 1, 2, 3, 4} and office.days is None

    listed = cron.parse("0,30 5 0 1,15 JAN-MAR ?")
    assert listed.seconds == {0, 30} and listed.days == {1, 15} and listed.months == {1, 2, 3}
    assert listed.weekdays is None

    assert cron.parse("5/20 * * * * ?").seconds == {5, 25, 45}

    for bad, says in (("0 0 * * *", "six fields"), ("0 0 0 1 * MON", "not both"),
                      ("0 0 0 ? * 2", "use a name"), ("0 61 * * * ?", "outside 0-59"),
                      ("0 */0 * * * ?", "positive"), ("? 0 0 * * ?", "only for day"),
                      ("0 0 0 L * ?", "not a number")):
        with pytest.raises(ValueError, match=says):
            cron.parse(bad)


def test_cron_counts_firings_in_a_window():
    assert cron.firings("* * * * * ?", MONDAY, MONDAY + DT) == 15
    assert cron.firings("*/5 * * * * ?", MONDAY, MONDAY + DT) == 3
    assert cron.firings("0 0 * * * ?", MONDAY - 1, MONDAY + 86400 - 1) == 24
    assert cron.firings("0 0 12 ? * SAT,SUN", MONDAY, MONDAY + 7 * 86400) == 2
    assert cron.firings("0 0 0 1 * ?", MONDAY, MONDAY + 31 * 86400) == 1      # 1 November


def test_a_cron_burst_loads_only_inside_its_window():
    def build(pg):
        generator(pg, "Burst Source", period="* 0-14 */2 * * ?", strategy="CRON_DRIVEN")
        sink(pg, "sink", "Burst Sink", period="0 sec")
        pg.connect("gen", "sink", ["success"], "Burst Queue")

    definition = _flow(build)
    source = next(p for p in definition["flowContents"]["processGroups"][0]["processors"]
                  if p["name"] == "Burst Source")
    assert source["schedulingStrategy"] == "CRON_DRIVEN" and source["schedulingPeriod"] == "* 0-14 */2 * * ?"

    sim = _simulation(definition)
    by_hour = [0.0] * 4

    for _ in range(int(4 * 3600 / DT)):
        sim.step()
        produced = _per_step(sim, "Burst Source", "flowfiles_out")
        window_start = time.gmtime(sim.t - DT + 1)
        by_hour[window_start.tm_hour] += produced

        if window_start.tm_min >= 15:
            assert produced == 0

        elif window_start.tm_hour % 2 == 0:
            assert produced == pytest.approx(15)

    assert by_hour == pytest.approx([900, 0, 900, 0])


def test_a_cron_consumer_runs_once_per_firing():
    def build(pg):
        generator(pg, "Steady Source")
        sink(pg, "sink", "Every Other Second", period="*/2 * * * * ?")
        pg.processors["sink"]["schedulingStrategy"] = "CRON_DRIVEN"
        pg.connect("gen", "sink", ["success"], "Waiting")

    sim = _simulation(_flow(build))
    sim.advance(sim.t + 20 * DT)

    assert _per_step(sim, "Every Other Second") == pytest.approx(7, abs=1)
    assert sim.latest("connection|Waiting|queued_count") > 100


def test_a_bad_cron_expression_fails_the_build_and_never_runs():
    def build(pg):
        generator(pg, "Source", period="0 0 0 ? * 2", strategy="CRON_DRIVEN")
        sink(pg, "sink", "Sink")
        pg.connect("gen", "sink", ["success"], "Q")

    definition = _flow(build)
    assert any("use a name" in p for p in check(definition)["problems"])
    assert any("CRON schedule" in p for p in compile_flow(definition).problems)

    with pytest.raises(ValueError, match="scheduling strategy"):
        ProcessGroup("x", "X").processor("gen", "Gen", "GenerateFlowFile", strategy="EVENT_DRIVEN")


def _distribute(properties: dict, *, full_third: bool = False):
    def build(pg):
        generator(pg, "Source", batch=10)
        pg.processor("split", "Spread", "DistributeLoad",
                     {"Number of Relationships": "3", "Distribution Strategy": "round robin", **properties},
                     period="0 sec")

        for n in ("1", "2", "3"):
            stopped = full_third and n == "3"
            sink(pg, f"s{n}", f"Sink {n}", period="0 sec", scheduled_state="DISABLED" if stopped else "ENABLED")
            pg.connect("split", f"s{n}", [n], f"To {n}", object_threshold=10 if stopped else 10000)

        pg.connect("gen", "split", ["success"], "Into Spread")

    return build


def test_distribute_load_splits_by_weight_and_skips_a_full_relationship():
    sim = _simulation(_flow(_distribute({})))
    sim.advance(sim.t + 10 * DT)
    taken = [_per_step(sim, f"Sink {n}") for n in "123"]
    assert taken == pytest.approx([50, 50, 50])
    assert sum(taken) == pytest.approx(_per_step(sim, "Spread"))

    weighted = _simulation(_flow(_distribute({"1": "2"})))
    weighted.advance(weighted.t + 10 * DT)
    assert [_per_step(weighted, f"Sink {n}") for n in "123"] == pytest.approx([75, 37.5, 37.5])

    blocked = _simulation(_flow(_distribute({}, full_third=True)))
    blocked.advance(blocked.t + 10 * DT)
    assert blocked.latest("connection|To 3|queued_count") <= 11
    assert [_per_step(blocked, f"Sink {n}") for n in "12"] == pytest.approx([75, 75])
    assert blocked.latest("connection|Into Spread|queued_count") == 150         # one step's, not a backlog

    # Its relationships are "1".."N": one more than is connected is unhandled.
    unhandled = _flow(_distribute({"Number of Relationships": "4"}))
    assert any("'4' is unhandled" in p for p in check(unhandled)["problems"])
    assert any("'4' is not connected" in p for p in compile_flow(unhandled).problems)


def _rated(properties: dict):
    def build(pg):
        generator(pg, "Source", batch=10, size="1 KB")
        pg.processor("rate", "Throttle", "ControlRate", properties, period="0 sec",
                     auto_terminate=("failure",))
        sink(pg, "sink", "Sink", period="0 sec")
        pg.connect("gen", "rate", ["success"], "Throttled")
        pg.connect("rate", "sink", ["success"], "Passed")

    return _flow(build)


def test_control_rate_caps_throughput_and_the_rest_waits():
    sim = _simulation(_rated({"Rate Control Criteria": "flowfile count", "Maximum Rate": "60",
                              "Time Duration": "1 min"}))
    sim.advance(sim.t + 20 * DT)
    assert _per_step(sim, "Sink") == pytest.approx(15)
    assert sim.latest("connection|Throttled|queued_count") == pytest.approx(20 * (150 - 15), rel=0.05)

    by_size = _simulation(_rated({"Rate Control Criteria": "data rate", "Maximum Rate": "2 KB",
                                  "Time Duration": "1 sec"}))
    by_size.advance(by_size.t + 10 * DT)
    assert _per_step(by_size, "Sink") == pytest.approx(30)

    broken = compile_flow(_rated({"Rate Control Criteria": "flowfile count", "Maximum Rate": "fast"}))
    assert any("Maximum Rate must be a positive number" in p for p in broken.problems)


def test_split_text_fans_out_by_line_count():
    def build(pg):
        generator(pg, "Source", text="id,value\na,1\nb,2\nc,3\nd,4\ne,5")
        pg.processor("split", "Split", "SplitText", {"Line Split Count": "2", "Header Line Count": "1"},
                     period="0 sec", auto_terminate=("failure", "original"))
        sink(pg, "sink", "Lines", period="0 sec")
        pg.connect("gen", "split", ["success"], "Files")
        pg.connect("split", "sink", ["splits"], "Splits")

    sim = _simulation(_flow(build))
    sim.advance(sim.t + 10 * DT)
    assert _per_step(sim, "Split") == pytest.approx(15)
    assert _per_step(sim, "Lines") == pytest.approx(45)          # 5 lines in twos: 3 splits each


def test_replace_text_passes_through_unless_the_content_overflows_its_buffer():
    def build(mode: str, buffer: str):
        def inner(pg):
            generator(pg, "Source", size="2 KB")
            pg.processor("replace", "Rewrite", "ReplaceText",
                         {"Evaluation Mode": mode, "Maximum Buffer Size": buffer, "Regular Expression": "a",
                          "Replacement Value": "b"}, period="0 sec")
            sink(pg, "sink", "Rewritten", period="0 sec")
            sink(pg, "dlq", "Rewrite DLQ", period="0 sec")
            pg.connect("gen", "replace", ["success"], "Raw")
            pg.connect("replace", "sink", ["success"], "Done")
            pg.connect("replace", "dlq", ["failure"], "Failed")

        return _flow(inner)

    lines = _simulation(build("Line-by-Line", "1 KB"))
    lines.advance(lines.t + 10 * DT)
    assert _per_step(lines, "Rewritten") == pytest.approx(15) and _per_step(lines, "Rewrite DLQ") == 0

    whole = _simulation(build("Entire text", "1 KB"))
    whole.advance(whole.t + 10 * DT)
    assert _per_step(whole, "Rewritten") == 0 and _per_step(whole, "Rewrite DLQ") == pytest.approx(15)


def _merging(threshold: int):
    def build(pg):
        generator(pg, "Source", batch=100)
        pg.processor("stage", "Tag", "UpdateAttribute", {"Store State": "Do not store state"}, period="0 sec")
        pg.processor("merge", "Merge", "MergeContent", {
            "Merge Strategy": "Bin-Packing Algorithm", "Minimum Number of Entries": "20000",
            "Maximum Number of Entries": "30000", "Max Bin Age": "10 min",
        }, period="0 sec", auto_terminate=("original", "failure"))
        sink(pg, "sink", "Merged", period="0 sec")
        pg.connect("gen", "stage", ["success"], "Ingest")
        pg.connect("stage", "merge", ["success"], "Into Merge", object_threshold=threshold)
        pg.connect("merge", "sink", ["merged"], "Bundles")

    return _flow(build)


def test_a_merge_bin_counts_toward_its_input_back_pressure():
    # The bin needs 20,000 but its input holds 10,000: the merge waits for
    # its 10 minute age, and the backlog backs up to the source.
    stuck = _simulation(_merging(10000))
    merged = 0.0

    for _ in range(int(400 / DT)):
        stuck.step()
        merged += _per_step(stuck, "Merged")

    assert stuck.latest("connection|Into Merge|queued_count") == pytest.approx(10000, rel=0.02)
    assert stuck.latest("connection|Ingest|queued_count") == pytest.approx(10000, rel=0.02)
    assert _per_step(stuck, "Source", "flowfiles_out") == 0 and merged == 0

    for _ in range(int(300 / DT)):
        stuck.step()
        merged += _per_step(stuck, "Merged")

    assert merged == pytest.approx(1)                   # aged out, once

    # Room for a full bin: one bundle per 200 s, and no backlog.
    flowing = _simulation(_merging(50000))
    merged = 0.0

    for _ in range(int(700 / DT)):
        flowing.step()
        merged += _per_step(flowing, "Merged")

    assert merged == pytest.approx(3)
    assert flowing.latest("connection|Ingest|queued_count") == 1500       # one step's, not a backlog


def test_a_scenario_says_how_its_fault_presents():
    assert SCENARIOS["S19"].kind == "control" and SCENARIOS["S01"].kind == "classic"
    assert SCENARIOS["S01"].detect_within == 1800 and SCENARIOS["S01"].clear_within == 900

    def scenario(**fields):
        return Scenario(**{"id": "S99", "title": "T", "symptom": "backlog", "fault": "f", "build": None,
                           "kpis": (), "family": "FLOW_EDIT", **fields})

    assert scenario(kind="compound", faults=({"action": "A"}, {"action": "B"})).faults

    for fields, says in (({"kind": "chaotic"}, "kind must be one of"),
                         ({"kind": "compound"}, "two or more fixes"),
                         ({"faults": ({"action": "A"},)}, "only a compound"),
                         ({"kind": "control"}, "only a control")):
        with pytest.raises(ValueError, match=says):
            scenario(**fields)
