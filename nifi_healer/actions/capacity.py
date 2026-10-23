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
"""

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
