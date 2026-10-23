"""
Whether a throughput fix is big enough: the capacity a SET_CONCURRENT_TASKS
or SET_RUN_SCHEDULE step leaves a processor with, against the intake it
has to keep up with (NFH-61).

    problems = check(report, incident, store, now, nodes)

`report` is executor.plan's full report for a flow edit, with the group's
definition before and after the edit, so a tasks step and a schedule step
on one processor are judged together. From the stored KPI samples
(kpi/readings.py), no CDF call.

Capacity after the change, in FlowFiles per second across the nodes:
  - a timer-driven schedule over 0 sec has a ceiling, tasks / run
    schedule (one FlowFile per run, or what the processor is measured to
    take per run if that is more);
  - a processor below its ceiling with a backlog in front is limited by
    its work per FlowFile: its measured rate per task, times the tasks.
    Without its own rate KPI that is unknown.

Intake, the highest of what is measured:
  - its own rate in, plus the growth of the queue in front of it;
  - the sources upstream of it, through a plain chain of stages (no
    branch on the way, and no split or merge but one whose output is
    measured, where the chain then starts): their highest rate in the
    last 2 h when back pressure may be holding them back, else their
    average (a burst is not an intake, S04), or what a GenerateFlowFile is
    set to send, summed.

Every estimate leans towards passing: when the capacity or the intake
isn't known, or the step lowers capacity, nothing is said. The problems go
back to the agent as validation problems while it has attempts left, and
show as warnings after that (agent/nodes.py).

    lines = analysis(inv, incident, store, now, nodes)

The same sums before the agent proposes (NFH-82), for the prompt: each
stage with a backlog in front, its ceiling, rate and intake, which limit
it is at, and the smallest change check() would pass.
"""

import math

import config
from nifi_healer.actions import catalog
from nifi_healer.flow.inventory import inventory
from nifi_healer.kpi import readings
from nifi_healer.kpi.detectors import REGROUPING
from nifi_healer.sim.model import parse_duration

STEPS = ("SET_CONCURRENT_TASKS", "SET_RUN_SCHEDULE")

# Capacity must clear the intake by this much: the headroom the prompt asks for.
HEADROOM = 1.1

# A processor whose rate is this close to its ceiling is waiting between runs.
AT_CEILING = 0.9

# analysis() also sizes a fix to drain the backlog by the end of the first verify window.
DRAIN_SECONDS = config.VERIFY_SETTLE_SECONDS + config.VERIFY_WINDOW_SECONDS

RATES = ("flowfiles_in", "flowfiles_out")


def _number(value: float) -> str:
    return f"{value:.0f}" if abs(value) >= 10 else f"{value:.2g}"


def _int(value, default: int = 1) -> int:
    try:
        return max(1, int(value))

    except (TypeError, ValueError):
        return default


def _period(processor: dict) -> float:
    return parse_duration(processor.get("run_schedule"), 0.0)


def _ceiling(processor: dict, nodes: int) -> float | None:
    """tasks / run schedule across the nodes, or None for no ceiling (0 sec)."""

    period = _period(processor)

    return _int(processor.get("concurrent_tasks")) / period * nodes if period > 0 else None


def _rate(rows: dict, name: str) -> float | None:
    """The processor's own measured rate, in if charted, else out."""

    for metric in RATES:
        row = rows.get(("processor", name, metric))

        if row and row["latest"] is not None:
            return row["latest"]

    return None


def _inbound(inv: dict, name: str) -> list:
    return [c for c in inv["connections"] if c["destination"] == name and not c["self_loop"]]


def _backlog(rows: dict, connection: dict, signalled: set) -> bool | None:
    """
    Whether a backlog waits in this queue; None when it has no readings, or
    expires FlowFiles (S25), so it holds steady however far behind it is.
    A queue whose KPI opened the incident has one, though a sawtooth (S04)
    may have dipped under the alert since.
    """

    row = rows.get(("connection", connection["name"], "queued_count"))

    if not row or row["latest"] is None or parse_duration(connection.get("expiration"), 0.0) > 0:
        return None

    return (row["key"] in signalled or bool(readings.full(row, connection)) or (row["state"] or 0) >= 1
            or row["trend"] == "rising")


def _sources(inv: dict, rows: dict, name: str, seen: frozenset = frozenset()) -> list | None:
    """
    The processors that feed `name` through a plain chain: those with no
    input, or a split or merge whose output is measured, as what it sends
    on is what reaches `name`. None when the way from them branches or
    loops, or a regrouping is not measured, so their rate is not what
    reaches it. A dead-letter branch doesn't count.
    """

    processors = {p["name"]: p for p in inv["processors"]}
    found = []

    for connection in _inbound(inv, name):
        upstream = processors.get(connection["source"])

        if upstream is None or upstream["name"] in seen:
            return None

        onward = [c for c in inv["connections"] if c["source"] == upstream["name"] and not c["self_loop"]
                  and c["destination"] not in inv["dlqs"]]

        if len(onward) != 1:
            return None

        if upstream["kind"] in REGROUPING:
            if ("processor", upstream["name"], "flowfiles_out") not in rows:
                return None

            found.append(upstream)
            continue

        if not _inbound(inv, upstream["name"]):
            found.append(upstream)
            continue

        more = _sources(inv, rows, upstream["name"], seen | {name})

        if more is None:
            return None

        found += more

    return found or None


def _configured(source: dict, nodes: int) -> float:
    """What a GenerateFlowFile is set to send per second; 0 for anything else."""

    period = _period(source)

    if source["kind"] != "GenerateFlowFile" or period <= 0 or source.get("scheduling") == "CRON_DRIVEN":
        return 0.0

    batch = _int((source.get("properties") or {}).get("Batch Size"))
    spread = 1 if source.get("execution_node") == "PRIMARY" else nodes

    return batch * _int(source.get("concurrent_tasks")) / period * spread


def _intake(inv: dict, rows: dict, name: str, nodes: int) -> float | None:
    estimates = []
    own = _rate(rows, name)

    if own is not None:
        growth = 0.0

        for connection in _inbound(inv, name):
            row = rows.get(("connection", connection["name"], "queued_count"))
            growth += max(0.0, (row or {}).get("per_minute") or 0.0) / 60

        estimates.append(own + growth)

    sources = _sources(inv, rows, name)

    if sources:
        rates = []

        for source in sources:
            row = rows.get(("processor", source["name"], "flowfiles_out")) or {}
            measured = row.get("peak") if readings.held(inv, rows, source["name"]) else row.get("mean")
            rates.append(max(measured or row.get("latest") or 0.0, _configured(source, nodes)))

        if all(rates):
            estimates.append(sum(rates))

    return max(estimates) if estimates else None


def _capacity(before: dict, after: dict, rows: dict, backlog: bool, nodes: int) -> tuple:
    """(FlowFiles per second after the change or None, how it was worked out)."""

    tasks = _int(after.get("concurrent_tasks"))
    ceiling, was = _ceiling(after, nodes), _ceiling(before, nodes)
    rate = _rate(rows, before["name"])
    per_run = max(1.0, rate / was) if rate is not None and was else 1.0
    found = (ceiling * per_run, f"{tasks} task(s) every {after.get('run_schedule')}") if ceiling else (None, "")

    if rate is not None and rate > 0 and backlog and (was is None or rate < AT_CEILING * was):
        # Busy, not waiting between runs: each task does what it does now.
        per_task = rate / _int(before.get("concurrent_tasks"))
        worked = per_task * tasks

        if found[0] is None or worked < found[0]:
            found = (worked, f"{tasks} task(s) at about {_number(per_task)}/s each")

    return found


def check(report: dict, incident: dict, store, now: float, nodes: int = 1) -> list:
    if report.get("family") != catalog.FLOW_EDIT or "_base_definition" not in report:
        return []

    names = list(dict.fromkeys(s.get("target") for s in report["steps"] if s.get("action") in STEPS))

    if not names:
        return []

    group = incident["process_group"]
    old, new = inventory(report["_base_definition"], group), inventory(report["_definition"], group)

    if old.get("error") or new.get("error"):
        return []

    rows = {(r["kind"], r["component"], r["metric"]): r for r in readings.for_incident(incident, store, now)}
    before = {p["name"]: p for p in old["processors"]}
    after = {p["name"]: p for p in new["processors"]}
    nodes = max(1, int(nodes or 1))
    signalled = {s.get("kpi") for s in incident.get("signals") or []}
    problems, considered, measured, backed = [], 0, 0, 0

    for name in names:
        was, now_is = before.get(name), after.get(name)

        if not was or not now_is or now_is.get("scheduling") == "CRON_DRIVEN":
            continue

        more_tasks = _int(now_is.get("concurrent_tasks")) > _int(was.get("concurrent_tasks"))
        period, period_was = _period(now_is), _period(was)

        if not more_tasks and not period < period_was:
            continue    # a step that lowers capacity is for something else (a runaway source, the CPU)

        queues = [_backlog(rows, c, signalled) for c in _inbound(new, name)]

        if not queues:
            continue    # a source: nothing waits in front of it

        considered += 1

        if any(q is not None for q in queues):
            measured += 1

        backlog = any(queues)
        backed += backlog
        capacity, how = _capacity(was, now_is, rows, backlog, nodes)
        intake = _intake(new, rows, name, nodes)

        if capacity is not None and intake and capacity < HEADROOM * intake:
            short = "below" if capacity < intake else f"within {100 * (HEADROOM - 1):.0f}% of"
            problems.append(f"{name}: {how} is about {_number(capacity)}/s, {short} the intake of about "
                            f"{_number(intake)}/s. Size it so the new capacity exceeds the intake with room to "
                            f"drain the backlog.")

    if measured and measured == considered and not backed:
        problems.append("None of the processors changed has a backlog in front of it; find the stage whose "
                        "input queue is full and whose output queue is not.")

    return problems


def _rows(incident: dict, store, now: float) -> dict:
    return {(r["kind"], r["component"], r["metric"]): r for r in readings.for_incident(incident, store, now)}


def _clears(processor: dict, rows: dict, need: float, nodes: int, **change) -> bool:
    capacity, _ = _capacity(processor, dict(processor, **change), rows, True, nodes)

    return capacity is not None and capacity >= need


def _options(processor: dict, rows: dict, need: float, nodes: int, at_ceiling: bool) -> list:
    """The fewest tasks, and at the ceiling the longest schedule, that reach `need` per second."""

    tasks = _int(processor.get("concurrent_tasks"))
    enough = next((n for n in range(tasks + 1, catalog.MAX_TASKS + 1)
                   if _clears(processor, rows, need, nodes, concurrent_tasks=n)), None)
    found = [f"{enough} task(s)"] if enough else []

    if at_ceiling:
        # tasks / schedule × per run × nodes ≥ need, to the millisecond below.
        ceiling = _ceiling(processor, nodes)
        rate = _rate(rows, processor["name"]) or 0.0
        per_run = max(1.0, rate / ceiling) if ceiling else 1.0
        ms = math.floor(1000 * tasks * per_run * nodes / need)

        if ms >= 1 and _clears(processor, rows, need, nodes, run_schedule=f"{ms} ms"):
            found.append(f"a run schedule of {ms} ms or shorter")

    return found


def _sized(processor: dict, inv: dict, rows: dict, intake: float, nodes: int, at_ceiling: bool) -> str:
    """
    What check() would pass, the least that keeps up with the intake, and
    what also drains the queue in front by the end of the first verify
    window, which is what the outcome is judged on.
    """

    least = _options(processor, rows, HEADROOM * intake, nodes, at_ceiling)
    text = (" or ".join(least) if least else "nothing the catalog allows") + \
        f" keeps up with the intake with {100 * (HEADROOM - 1):.0f}% headroom"
    queued = sum((rows.get(("connection", c["name"], "queued_count")) or {}).get("latest") or 0
                 for c in _inbound(inv, processor["name"]))

    if least and queued:
        need = max(HEADROOM * intake, intake + queued / DRAIN_SECONDS)
        drains = _options(processor, rows, need, nodes, at_ceiling)

        if drains != least:
            text += f"; to drain the {_number(queued)} queued within {DRAIN_SECONDS // 60} min, " + (
                " or ".join(drains) if drains else "nothing the catalog allows is enough")

    return text


def _runaway(inv: dict, rows: dict, name: str) -> dict | None:
    """A GenerateFlowFile feeding `name` on a 0 sec schedule: it sends as fast as back pressure lets it."""

    for source in _sources(inv, rows, name) or []:
        if source["kind"] == "GenerateFlowFile" and source.get("scheduling") != "CRON_DRIVEN" \
                and _period(source) <= 0 and not _inbound(inv, source["name"]):
            return source

    return None


def _small(inv: dict, name: str, intake: float) -> str:
    """Its input queues that back-pressure at under a minute of the intake: a burst fills them."""

    found = []

    for connection in _inbound(inv, name):
        try:
            limit = float(connection.get("object_threshold"))

        except (TypeError, ValueError):
            continue

        if 0 < limit < 60 * intake:
            found.append(f" {connection['name']} back-pressures at {limit:.0f} FlowFiles, about "
                         f"{_number(limit / intake)} s of the intake.")

    return "".join(found)


def _stage(processor: dict, inv: dict, rows: dict, nodes: int) -> str:
    name, tasks = processor["name"], _int(processor.get("concurrent_tasks"))
    ceiling, rate = _ceiling(processor, nodes), _rate(rows, name)
    intake = _intake(inv, rows, name, nodes)
    text = f"- {name}: {tasks} task(s) every {processor.get('run_schedule')}, " + (
        f"a ceiling of about {_number(ceiling)}/s" if ceiling else "no ceiling (0 sec)")
    text += f"; it runs at about {_number(rate)}/s" if rate is not None else "; its rate is not measured"
    text += f"; the intake is about {_number(intake)}/s" if intake else "; the intake is not measured"
    runaway = _runaway(inv, rows, name)

    if processor.get("state") in ("DISABLED", "STOPPED"):
        return text + f". It is {processor['state']}: it takes nothing in."

    if rate == 0:
        return text + ". It sends nothing: a stall, a failure or a misconfiguration, not capacity."

    if runaway:
        return text + (f". Its source {runaway['name']} runs every {runaway.get('run_schedule')}, as fast as back "
                       "pressure lets it: no number of tasks keeps up, so the source is what to slow.")

    if processor["kind"] == "ControlRate":
        return text + ". A ControlRate passes what its rate properties allow, whatever its tasks."

    if not intake or rate is None and ceiling is None:
        return text + "."

    if _clears(processor, rows, HEADROOM * intake, nodes):
        return text + ". It already has the capacity for the intake: the limit is something else." \
            + _small(inv, name, intake)

    if rate is None:
        return text + f". If the schedule is the limit, {_sized(processor, inv, rows, intake, nodes, True)}."

    if ceiling is not None and rate >= AT_CEILING * ceiling:
        return text + f". At its ceiling, the schedule is the limit: {_sized(processor, inv, rows, intake, nodes, True)}."

    if ceiling is not None and ceiling >= HEADROOM * intake:
        return text + (f". Its ceiling already clears the intake, so it waits for FlowFiles or works slowly on each; "
                       f"if slowly, {_sized(processor, inv, rows, intake, nodes, False)}." + _small(inv, name, intake))

    return text + (f". Below its ceiling, the work per FlowFile is the limit (about {_number(rate / tasks)}/s a task): "
                   f"{_sized(processor, inv, rows, intake, nodes, False)}; a shorter schedule does nothing.")


def analysis(inv: dict | None, incident: dict, store, now: float, nodes: int = 1) -> list:
    """
    One line per stage with a backlog in front of it, in flow order: one
    whose own way on is at back pressure is held by the stage after it;
    any other gets the sums. Then the deployment's CPU, when it is high.
    [] without an inventory or a backlog.
    """

    if not inv or inv.get("error"):
        return []

    rows = _rows(incident, store, now)
    signalled = {s.get("kpi") for s in incident.get("signals") or []}
    nodes = max(1, int(nodes or 1))
    depth, _ = readings.flow_order(inv)
    lines = []

    for processor in sorted(inv["processors"], key=lambda p: depth.get(p["name"], len(depth))):
        name = processor["name"]
        inbound = _inbound(inv, name)

        if processor.get("scheduling") == "CRON_DRIVEN" or not any(_backlog(rows, c, signalled) for c in inbound):
            continue

        held = [c["name"] for c in inv["connections"] if c["source"] == name and not c["self_loop"]
                and c["destination"] not in inv["dlqs"]
                and readings.full(rows.get(("connection", c["name"], "queued_count")), c)]

        lines.append(f"- {name}: held back, its way on ({', '.join(held)}) is at back pressure; look at the stage "
                     "after it." if held else _stage(processor, inv, rows, nodes))

    cpu = next((r for r in rows.values() if r["kind"] == "system" and r["metric"] == "cpu"), None)

    if lines and cpu and cpu["latest"] is not None and (cpu["state"] or 0) >= 1:
        lines.append(f"- The deployment's CPU is at {cpu['latest']:.0f}%: more tasks will not help, it is sizing.")

    return lines
