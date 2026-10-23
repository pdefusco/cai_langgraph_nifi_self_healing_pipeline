"""
What every KPI of an incident's process group reads now, for the
diagnosis prompt. The signals name only what breached; in a cascade the
cause is a stage away from the alert, so the agent also needs the queues
and rates that have not breached.

    rows = for_incident(incident, store, now)
    text = render(rows, inv)        # inv: flow/inventory.py's inventory of the group

From the samples the poller stored, over the last WINDOW_SECONDS (no CDF
call). Each row has the latest value, its alert threshold, a state (over,
near or fine, as the Monitor tab colours it) and a trend (and its slope
per minute, for actions/capacity.py). Processor
counts are shown per second (config.COUNT_WINDOW_SECONDS). Back pressure
throttles what a source sends, which hides the intake, so a source that
sent more in the last PEAK_SECONDS shows that peak too, and so does a
split (a fan-out), whose output is the intake of the stages after it.

Only the KPIs the pack defines; NFH-65 makes the coverage uniform. Given
the group's inventory, render() lists every connection in flow order,
each against its back-pressure limit and with its expiration, and says
so of one no KPI measures: in a cascade the full queue nobody watches
would otherwise read as empty.
"""

import config
from flows import packs
from nifi_healer import fleet
from nifi_healer.kpi import detectors, metrics
from nifi_healer.sim.model import parse_duration, parse_size

WINDOW_SECONDS = 900
PEAK_SECONDS = 7200

# Processor metrics charted as totals per COUNT_WINDOW_SECONDS.
RATES = ("flowfiles_in", "flowfiles_out")


def _per_second(kpi: dict, series: list) -> list:
    if kpi.get("kind") == "processor" and kpi.get("metric") in RATES:
        return [(t, v / config.COUNT_WINDOW_SECONDS) for t, v in series]

    return series


def _trend(series: list) -> str:
    if detectors.rising(series):
        return "rising"

    if detectors.falling(series):
        return "falling"

    return "flat"


def for_incident(incident: dict, store, now: float) -> list:
    """One row per KPI of the incident's scenario, in the pack's order."""

    scenario = packs.SCENARIOS.get(incident.get("scenario") or "")
    kpis = [k for k in fleet.pack(incident.get("pack") or "").get("kpis") or []
            if scenario and k.get("scenario") == scenario.id]
    edges = scenario.flow_edges() if scenario else {"in": [], "split": []}
    crn = incident.get("deployment_crn") or ""
    rows = []

    for kpi in kpis:
        raw = store.samples(crn, kpi["key"], now - PEAK_SECONDS)
        recent = [(t, v) for t, v in raw if t >= now - WINDOW_SECONDS]
        shown = _per_second(kpi, recent)
        latest = shown[-1][1] if shown else None
        sends = kpi.get("metric") == "flowfiles_out"
        source = sends and kpi.get("component") in edges["in"]
        # A split's output is the intake of the stages after it, throttled the same way.
        feeds = source or sends and kpi.get("component") in edges["split"]

        rows.append({
            "key": kpi["key"], "kind": kpi.get("kind"), "component": kpi.get("component") or "",
            "metric": kpi.get("metric"), "latest": latest,
            "above": kpi.get("above"), "below": kpi.get("below"),
            "state": metrics.kpi_state(kpi, latest),
            # Fitted on the stored values: a live total is the larger number, so its noise floor is lower.
            "trend": _trend(recent) if recent else "",
            "per_minute": metrics.trend(shown),
            "source": source,
            "peak": max(v for _, v in _per_second(kpi, raw)) if feeds and raw else None,
        })

    return rows


def _number(value) -> str:
    return f"{value:.0f}" if abs(value) >= 100 else f"{value:.3g}"


def _bytes(value) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(value) < 1024 or unit == "GB":
            return f"{_number(value)} {unit}"

        value /= 1024


# Each metric's value as shown, and the word after it.
_SHOWN = {
    "queued_count": (_number, "queued"),
    "queued_bytes": (_bytes, "queued"),
    "percent_full": (lambda v: f"{_number(v)}%", "full"),
    "flowfiles_in": (lambda v: f"{_number(v)}/s", "in"),
    "flowfiles_out": (lambda v: f"{_number(v)}/s", "out"),
    "bytes_out": (_bytes, "sent"),
    "cpu": (lambda v: f"{_number(v)}%", "CPU"),
}


def _connection(connection: dict) -> str:
    return f"connection {connection['name']} ({connection['source']} -> {connection['destination']})"


def _expiry(connection: dict | None) -> str:
    expiration = (connection or {}).get("expiration")

    return f"; FlowFiles expire after {expiration}" if parse_duration(expiration) > 0 else ""


def _fill(row: dict, connection: dict | None) -> str:
    """How close the queue is to back pressure, from the inventory's thresholds."""

    if connection is None:
        return ""

    limits = {"queued_count": (connection.get("object_threshold"), _number),
              "queued_bytes": (connection.get("size_threshold"), _bytes)}
    limit, shown = limits.get(row["metric"], (None, None))
    size = parse_size(limit) if row["metric"] == "queued_bytes" else limit

    if not isinstance(size, (int, float)) or size <= 0:
        return ""

    return f", back pressure at {shown(size)} ({100 * row['latest'] / size:.0f}% full)"


def _line(row: dict, connection: dict | None = None) -> str:
    where = {"connection": _connection(connection) if connection else f"connection {row['component']}",
             "system": "the deployment's nodes"}.get(
        row["kind"], row["component"] + (" (a source)" if row["source"] else ""))

    if row["latest"] is None:
        return f"- {where}: no data in the last {WINDOW_SECONDS // 60} min{_expiry(connection)}"

    shown, word = _SHOWN.get(row["metric"], (_number, row["metric"]))
    text = f"- {where}: {shown(row['latest'])} {word}" + _fill(row, connection)

    if row["peak"] is not None and row["peak"] > 1.1 * row["latest"]:
        text += f", up to {shown(row['peak'])} in the last {PEAK_SECONDS // 3600} h"

    if row["above"] is not None:
        text += f" (alert above {shown(row['above'])})"

    elif row["below"] is not None:
        text += f" (alert below {shown(row['below'])})"

    found = [metrics.STATE_TEXT[row["state"]]] if row["state"] is not None else []

    return text + _expiry(connection) + ": " + ", ".join(found + [row["trend"]])


def _unmeasured(connection: dict) -> str:
    objects, size = connection.get("object_threshold"), connection.get("size_threshold")
    limits = " / ".join(v for v in (f"{objects} objects" if objects else "", size or "") if v)

    return (f"- {_connection(connection)}: not measured" + (f", back pressure at {limits}" if limits else "")
            + _expiry(connection))


def flow_order(inv: dict) -> tuple:
    """
    ({component: its distance from the group's sources}, the connections
    in flow order): a connection sorts by how far its source is, a tie in
    the definition's order. A loop back upstream doesn't move anything.
    """

    connections = inv.get("connections") or []
    names = [p["name"] for p in inv.get("processors") or []]
    names += [n for c in connections for n in (c["source"], c["destination"]) if n not in names]
    fed = {c["destination"] for c in connections if c["source"] != c["destination"]}
    depth = {n: 0 for n in names if n not in fed}
    frontier = list(depth)

    while frontier:
        reached = []

        for c in connections:
            if c["source"] in frontier and c["destination"] not in depth:
                depth[c["destination"]] = depth[c["source"]] + 1
                reached.append(c["destination"])

        frontier = reached

    return depth, sorted(connections, key=lambda c: depth.get(c["source"], len(names)))


def render(rows: list, inv: dict | None = None) -> str:
    """
    One line per row; with the group's inventory, every connection first,
    in flow order, measured or not, then the other rows in flow order.
    """

    if not inv or "error" in inv:
        return "\n".join(_line(r) for r in rows)

    depth, connections = flow_order(inv)
    measured = {}

    for row in rows:
        if row["kind"] == "connection":
            measured.setdefault(row["component"], []).append(row)

    lines = []

    for connection in connections:
        lines += [_line(r, connection) for r in measured.pop(connection["name"], [])] or [_unmeasured(connection)]

    rest = [r for r in rows if r["kind"] != "connection"] + [r for found in measured.values() for r in found]
    rest.sort(key=lambda r: (r["kind"] == "system", depth.get(r["component"], len(depth))))

    return "\n".join(lines + [_line(r) for r in rest])
