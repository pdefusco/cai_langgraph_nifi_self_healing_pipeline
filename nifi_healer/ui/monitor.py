"""
The Monitor tab: one deployment's KPIs as a continuous timeline, cut
into stretches by every change made to it, whoever made it.

    m = monitor.model(store, pack, entry, now, span="12 h", selected=None)
    html = monitor.render(m)
    monitor.change_choices(m)      # [(label, index)], newest first

A stretch (A, B, C, ...) runs from one change to the next: a new flow
version, a parameter update, a resize. Changes come from the deployment's
CDF event log (kpi/changes.py); the agent's are told apart by its own
applied actions and carry the action's verdict. An operator's change is
only a marker. KPI (alert rule) updates are a tick on the rail; they do
not start a stretch.

Everything is drawn server-side into one HTML string: the charts are
SVG stretched to the width (no text inside, so nothing distorts), labels
and markers are HTML on top. JS only moves the crosshair and reports a
clicked change; it is attached once (gr.HTML js_on_load) and survives
re-renders because it listens on the component's element.

Letters are given over the deployment's whole history, so a stretch keeps
its letter when the range changes. Times are UTC.
"""

import html
import math
import re
from datetime import datetime, timezone

import config
from nifi_healer import fleet
from nifi_healer.kpi import changes as change_log
from nifi_healer.kpi import outcomes, rollup
from nifi_healer.kpi.detectors import group_name
from nifi_healer.store.db import OPEN_STATUSES
from nifi_healer.ui import views

RANGES = {"3 h": 10800, "6 h": 21600, "12 h": 43200, "All": None}
DEFAULT_RANGE = "12 h"
STEPS = {"3 h": 75, "6 h": 150, "12 h": 300}

# "All": the finest of these giving at most _MAX_BUCKETS buckets.
_ALL_STEPS = (75, 150, 300, 600, 900, 1800, 3600)
_MAX_BUCKETS = 240

_TICK_STEPS = (900, 1800, 3600, 7200, 10800, 21600, 43200, 86400)
_MAX_TICKS = 8

# A gap in the stored history longer than this (or two buckets) breaks the line.
_BRIDGE_SECONDS = 3600

# How far after an operator's flow version change its version is first seen by a health poll.
_VERSION_SEEN_BEFORE, _VERSION_SEEN_AFTER = 120, 3600

_W = 1000  # every chart's viewBox width

# A count chart's scale never goes below this, so a near-idle KPI's noise stays flat.
_COUNT_FLOOR = 5

KIND_LABEL = {"created": "Provisioned", "flow_version": "Flow version", "parameters": "Parameters",
              "sizing": "Sizing", "other": "Other", "kpis": "Alert rules"}

METRIC_LABEL = {"queued_count": "queued FlowFiles", "flowfiles_in": "FlowFiles in / bucket",
                "flowfiles_out": "FlowFiles out / bucket"}
METRIC_SHORT = {"flowfiles_in": "in", "flowfiles_out": "out"}

SYSTEM = (("system:cpu", "CPU", "%", "utilization"), ("system:memory", "Memory", "%", "utilization"),
          ("system:cores", "Cores", "cores", "allocated cores"))

_STEP_FIELDS = ("dlq", "object_threshold", "size_threshold", "cluster_size", "nodes", "backoff",
                "max_backoff", "retry_count", "penalty", "yield_duration")


# ---------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------


def _e(text) -> str:
    """Escaped for HTML, and inert to Gradio's template syntax."""

    return (html.escape("" if text is None else str(text), quote=True)
            .replace("`", "&#96;").replace("$", "&#36;").replace("{", "&#123;").replace("}", "&#125;"))


def fmt(value, unit: str = "") -> str:
    if value is None:
        return "—"

    if unit == "%":
        return f"{value:.1f}%"

    if unit == "cores":
        return f"{value:.0f}"

    if unit == "ratio":
        return f"{value:.0f}%"

    if abs(value) < 10 and not float(value).is_integer():
        return f"{value:.1f}"

    return f"{value:,.0f}"


def short(value: float) -> str:
    if value >= 1000:
        text = f"{value / 1000:.0f}" if value >= 10000 else f"{value / 1000:.1f}"

        return text.removesuffix(".0") + "k"

    return f"{value:.0f}"


def _hhmm(t: float) -> str:
    return datetime.fromtimestamp(t, tz=timezone.utc).strftime("%H:%M")


def _when(t: float, with_day: bool) -> str:
    return datetime.fromtimestamp(t, tz=timezone.utc).strftime("%b %d %H:%M" if with_day else "%H:%M")


def _clip(text: str, length: int = 30) -> str:
    text = str(text or "")

    return text if len(text) <= length else text[:length - 1] + "…"


def delta(before, after, good: str, unit: str = "") -> tuple:
    """(text, "good" | "bad" | "") for a ledger cell's change from the previous stretch."""

    if before is None or after is None:
        return "", ""

    diff = after - before

    if unit in ("%", "ratio"):
        if abs(diff) < (max(3, 0.1 * abs(before)) if unit == "ratio" else 0.5):
            return "=", ""

        tone = "" if unit == "ratio" else ("good" if (diff < 0) == (good == "down") else "bad")

        return f"{'+' if diff > 0 else '−'}{abs(diff):.{0 if unit == 'ratio' else 1}f} pt", tone

    if abs(diff) <= max(0.05 * abs(before), 2):
        return "=", ""

    tone = "good" if (diff < 0) == (good == "down") else "bad"
    text = f"from {fmt(before, unit)}" if abs(before) < 1 else f"{'+' if diff > 0 else '−'}{abs(diff / before):.0%}"

    return f"{text} {'better' if tone == 'good' else 'worse'}", tone


# ---------------------------------------------------------------------
# Choices
# ---------------------------------------------------------------------


def deployment_choices() -> list:
    """[(label, pack)] for every deployed pack."""

    return [(entry.get("deployment_name") or name, name) for name, entry in sorted(fleet.deployed_packs().items())]


def change_choices(m: dict) -> list:
    """[(label, segment index)], newest first: every change that started a stretch."""

    return [(f"{s['letter']} · {_when(s['start'], True)} · {s['who']} · {s['short']}", str(s["index"]))
            for s in reversed(m.get("segments") or []) if s.get("source")]


# ---------------------------------------------------------------------
# The model
# ---------------------------------------------------------------------


def _letter(index: int) -> str:
    return chr(65 + index % 26) + (str(index // 26 + 1) if index >= 26 else "")


def _window(span: str, origin: float | None, now: float) -> tuple:
    """(since, step, n): n buckets of step seconds from since, the last one holding now."""

    seconds = RANGES.get(span, RANGES[DEFAULT_RANGE]) if span in RANGES else RANGES[DEFAULT_RANGE]

    if seconds is None:
        start = origin if origin is not None else now - 3600
        step = next((s for s in _ALL_STEPS if s >= (now - start) / _MAX_BUCKETS), _ALL_STEPS[-1])

    else:
        start = max(now - seconds, origin) if origin is not None else now - seconds
        step = STEPS.get(span, STEPS[DEFAULT_RANGE])

    start = min(start, now - 2 * step)
    since = math.floor(start / step) * step

    return since, step, max(1, math.ceil((now - since) / step))


def _step_text(step: dict) -> str:
    parts = [step.get("action") or ""]
    parts += [str(step[k]) for k in ("target", "property", "relationship", "parameter") if step.get(k)]

    if step.get("value") is not None:
        parts.append(f"→ {step['value']}")

    parts += [f"{k} {step[k]}" for k in _STEP_FIELDS if step.get(k) is not None]

    return "  ".join(p for p in parts if p)


def _step_short(step: dict) -> str:
    name = step.get("parameter") or step.get("property") or step.get("target") or step.get("action") or ""
    value = step.get("value")

    if value is None:
        value = next((step[k] for k in _STEP_FIELDS if step.get(k) is not None), None)

    return f"{name} → {value}" if value is not None else str(name)


def _version_of(crn: str, by_crn: dict):
    if not crn:
        return None

    if crn in by_crn:
        return by_crn[crn]

    found = re.search(r"/v\.(\d+)$", crn)

    return int(found.group(1)) if found else None


def _outcome_line(details: dict) -> str:
    shown = views._deciding(details or {})

    if not shown:
        return ""

    before, after = shown.get("before") or {}, shown.get("after") or {}

    if before.get("latest") is None or after.get("latest") is None:
        return ""

    return (f"{views._component(shown.get('kpi'))}: "
            f"{views.change(before.get('latest'), after.get('latest'), shown.get('good', 'down'))}")


def _agent_segment(store, change: dict, by_crn: dict, flow, used: set, cache: dict) -> dict:
    action = change["action"]
    incident_id = action.get("incident_id") or ""

    if incident_id not in cache:
        cache[incident_id] = {"proposals": {p["id"]: p for p in store.proposals(incident_id)},
                              "outcomes": _by_action(store.outcomes(incident_id)),
                              "labels": store.labels(incident_id) if incident_id else {},
                              "incident": store.incident(incident_id) or {}}

    found = cache[incident_id]
    proposal = found["proposals"].get(action.get("proposal_id")) or {}
    steps = [s for s in proposal.get("action") or [] if isinstance(s, dict)]
    short_text = _step_short(steps[0]) + (f" +{len(steps) - 1}" if len(steps) > 1 else "") if steps \
        else action.get("action_type") or "change"

    if action.get("family") == "FLOW_EDIT":
        version = _version_of(action.get("to_flow_version_crn"), by_crn)
        used.add(action.get("to_flow_version_crn"))

        if version is not None:
            flow = version
            short_text = f"v{version} · {short_text}"

    judged = found["outcomes"].get(action.get("id")) or {}
    windows = _windows(action, judged)
    outcome = views.shown_outcome(list(judged.values()))
    details = (outcome or {}).get("details") or {}
    diagnosis = proposal.get("diagnosis") or ""

    return {
        "source": "agent", "who": "Agent", "flow": flow, "short": _clip(short_text),
        "what": "; ".join(_step_text(s) for s in steps) or action.get("action_type") or "",
        "note": _clip(diagnosis, 280), "result": _outcome_line(details),
        "incident": incident_id.split("-", 3)[-1] if incident_id else "",
        "verdict": (outcome or {}).get("verdict") or "pending",
        "window": views.window_label(outcome["window_seconds"]) if outcome else "",
        "windows": windows, "regression": any(w["regression"] for w in windows),
        "label": (found["labels"].get(action.get("id")) or {}).get("label") or "",
    }


def _by_action(found: list) -> dict:
    """{action id: {window seconds: outcome}}."""

    out = {}

    for o in found:
        out.setdefault(o["action_id"], {})[o["window_seconds"]] = o

    return out


def _windows(action: dict, judged: dict) -> list:
    """Every verify window of an applied action: its verdict, or when it is due."""

    out = []

    for window in sorted(set(config.VERIFY_WINDOWS) | set(judged)):
        o = judged.get(window)
        details = (o or {}).get("details") or {}
        due = outcomes.due_at(action["finished_at"], window, action.get("started_at")) \
            if action.get("finished_at") else None
        out.append({"label": views.window_label(window), "verdict": o["verdict"] if o else "pending",
                    "due": due, "regression": bool(details.get("regression")),
                    "reason": details.get("reason") or "" if details.get("cut") or details.get("missed") else ""})

    return out


def _operator_segment(change: dict, versions: list, flow, used: set) -> dict:
    kind, t = change["kind"], change["t"]
    out = {"source": "operator", "who": "Operator", "flow": flow, "note": "", "result": "",
           "incident": "", "verdict": "", "window": "", "windows": [], "regression": False,
           "label": ""}

    if kind == "flow_version":
        seen = [r for r in versions if r["crn"] not in used and r.get("first_seen_at") is not None
                and t - _VERSION_SEEN_BEFORE <= r["first_seen_at"] <= t + _VERSION_SEEN_AFTER]
        row = min(seen, key=lambda r: r["first_seen_at"]) if seen else None

        if row:
            used.add(row["crn"])
            out["flow"] = row.get("version")

        version = row.get("version") if row else None
        out["short"] = f"v{version}" if version is not None else "flow version"
        out["what"] = (f"Flow version → v{version}" + (f"  ({row['comments']})" if row.get("comments") else "")
                       if version is not None else "Flow version changed")

    elif kind == "parameters":
        out["short"], out["what"] = "parameters", "Parameters updated"

    else:
        out["short"], out["what"] = _clip(change.get("name") or KIND_LABEL.get(kind, kind), 24), change.get("name") or ""

    return out


def _segments(store, pack: str, found: list, origin: float) -> tuple:
    """(segments, minor changes): every stretch since the first record, oldest first."""

    versions = store.flow_versions(pack)
    by_crn = {r["crn"]: r.get("version") for r in versions}
    flow = min((r["version"] for r in versions if r.get("version") is not None), default=None)
    used, cache, minor = set(), {}, []
    segments = [{"start": origin, "kind": "created", "source": "", "who": "", "flow": flow,
                 "short": "as provisioned", "what": "", "note": "", "result": "", "incident": "",
                 "verdict": "", "window": "", "windows": [], "regression": False, "label": ""}]

    for change in found:
        if change["kind"] == "created":
            continue

        if change["kind"] not in change_log.SEGMENTING:
            minor.append(change)
            continue

        if change.get("action"):
            segment = _agent_segment(store, change, by_crn, flow, used, cache)

        else:
            segment = _operator_segment(change, versions, flow, used)

        flow = segment["flow"]
        segments.append(dict(segment, start=change["t"], kind=change["kind"]))

    for i, segment in enumerate(segments):
        segment.update(index=i, letter=_letter(i),
                       end=segments[i + 1]["start"] if i + 1 < len(segments) else None)

    return segments, minor


def _series(grid: dict, key: str, n: int) -> list:
    found = grid.get(key) or {}

    return [found.get(i) for i in range(n)]


def _incidents(store, pack: str, crn: str, since: float, end: float, now: float) -> dict:
    """{scenario: [incident bar]} overlapping the window."""

    lanes = {}

    for incident in store.incidents(pack=pack, limit=500):
        if incident.get("deployment_crn") != crn:
            continue

        opened, closed = incident["opened_at"], incident.get("closed_at")

        if (closed or now) <= since or opened >= end:
            continue

        status = incident["status"]
        state = ("open" if status in OPEN_STATUSES else "rejected" if status in ("REJECTED", "NO_ACTION")
                 else "improved" if status == "RESOLVED"
                 else "worsened" if status == "UNRESOLVED" and incident.get("verdict") == "WORSENED"
                 else "nochange")
        lanes.setdefault(incident.get("scenario") or "", []).append({
            "id": incident["id"].split("-", 3)[-1], "start": opened, "end": closed,
            "state": state, "status": status, "summary": incident.get("summary") or ""})

    return lanes


def model(store, pack: str, entry: dict, now: float, span: str = DEFAULT_RANGE, selected=None) -> dict:
    crn = entry.get("deployment_crn") or ""
    kpis = entry.get("kpis") or []
    found = change_log.attribute(store.changes(crn), store.actions(deployment_crn=crn))
    first = store.first_sample_at(crn)
    times = [t for t in [first] + [c["t"] for c in found] if t is not None]
    origin = min(times) if times else None
    since, step, n = _window(span, origin, now)
    end = since + n * step
    segments, minor = _segments(store, pack, found, origin if origin is not None else since)

    try:
        selected = int(selected)

    except (TypeError, ValueError):
        selected = None

    if selected is None or not 0 < selected < len(segments):
        selected = next((s["index"] for s in reversed(segments) if s["source"]), None)

    grid = store.history(crn, since, step)
    groups = []

    for scenario in sorted({k["scenario"] for k in kpis}):
        members = [k for k in kpis if k["scenario"] == scenario]
        sides = rollup.edges(members)
        flow = None

        if sides["in"] and sides["out"]:
            flow = {}

            for side in ("in", "out"):
                columns = [_series(grid, k["key"], n) for k in sides[side]]
                flow[side] = [sum(c[i] for c in columns) if all(c[i] is not None for c in columns) else None
                              for i in range(n)]

            flow["labels"] = (" + ".join(sorted({k["component"] for k in sides["in"]})),
                              " + ".join(sorted({k["component"] for k in sides["out"]})))

        groups.append({
            "scenario": scenario, "title": group_name(scenario) or scenario, "flow": flow,
            "kpis": [dict(k, values=_series(grid, k["key"], n)) for k in members if not k.get("rollup")]})

    system = [{"key": key, "label": label, "unit": unit, "meta": meta, "good": "down",
               "values": _series(grid, key, n)} for key, label, unit, meta in SYSTEM]

    return {
        "pack": pack, "crn": crn, "entry": entry, "deployment": store.deployment(crn) or {},
        "now": now, "span": span, "since": since, "step": step, "n": n, "end": end,
        "segments": segments, "minor": minor, "selected": selected, "groups": groups, "system": system,
        "incidents": _incidents(store, pack, crn, since, end, now),
    }


# ---------------------------------------------------------------------
# The fleet
# ---------------------------------------------------------------------

FLEET_SORTS = ("Needs attention", "Recent change", "Name")

# A KPI within this fraction of its alert threshold (on the bad side) is "near".
_NEAR = 0.8

STATE_TEXT = {None: "no data", 0: "fine", 1: "near", 2: "over"}
HEALTH_TEXT = {"CONCERNING_HEALTH": "concerning", "BAD_HEALTH": "bad health"}


def kpi_state(kpi: dict, value):
    """0 fine, 1 near its alert threshold, 2 over it; None without a value or a threshold."""

    if value is None:
        return None

    if kpi.get("above") is not None:
        return 2 if value > kpi["above"] else 1 if value >= _NEAR * kpi["above"] else 0

    if kpi.get("below") is not None:
        return 2 if value < kpi["below"] else 1 if value * _NEAR <= kpi["below"] else 0

    return None


def _worst(states) -> int | None:
    return max((s for s in states if s is not None), default=None)


def _carry(m: dict, values: list) -> list:
    """
    values with each gap a line would be drawn across filled by the bucket
    before it, and the newest buckets (not yet polled) by the last value.
    """

    bridge = max(1, round(2 * max(m["step"], _BRIDGE_SECONDS) / m["step"]))
    out = list(values)
    present = [i for i, v in enumerate(values) if v is not None]

    for a, b in zip(present, present[1:]):
        if b - a <= bridge:
            out[a + 1:b] = [values[a]] * (b - a - 1)

    if present:
        last = present[-1]
        out[last + 1:last + 3] = [values[last]] * len(out[last + 1:last + 3])

    return out


def _merged(bars: list) -> list:
    """Incident bars merged where they overlap: one lane per deployment."""

    out = []

    for bar in sorted(bars, key=lambda b: b["start"]):
        end = bar["end"] if bar["end"] is not None else math.inf

        if out and bar["start"] < out[-1]["stop"]:
            last = out[-1]
            last["bars"].append(bar)
            last["stop"] = max(last["stop"], end)

        else:
            out.append({"start": bar["start"], "stop": end, "bars": [bar]})

    for group in out:
        states = {b["state"] for b in group["bars"]}
        group["end"] = None if group["stop"] == math.inf else group["stop"]
        group["state"] = "open" if "open" in states else "improved" if states == {"improved"} else "nochange"
        group["label"] = group["bars"][0]["id"] + (f" +{len(group['bars']) - 1}" if len(group["bars"]) > 1 else "")

    return out


def _origin(store, crn: str, found: list):
    times = [t for t in [store.first_sample_at(crn)] + [c["t"] for c in found] if t is not None]

    return min(times) if times else None


def _fleet_row(store, pack: str, entry: dict, since: float, step: float, n: int, now: float) -> dict:
    crn = entry.get("deployment_crn") or ""
    found = change_log.attribute(store.changes(crn), store.actions(deployment_crn=crn))
    origin = _origin(store, crn, found)
    segments, _ = _segments(store, pack, found, origin if origin is not None else since)
    grid = store.history(crn, since, step)
    watched = [k for k in entry.get("kpis") or [] if not k.get("rollup")
               and (k.get("above") is not None or k.get("below") is not None)]
    columns = [(k, _carry({"step": step}, _series(grid, k["key"], n))) for k in watched]
    states = [_worst(kpi_state(k, values[i]) for k, values in columns) for i in range(n)]
    texts = []

    for i in range(n):
        at = [kpi_state(k, values[i]) for k, values in columns]
        over, near = at.count(2), at.count(1)
        texts.append("no data" if states[i] is None else f"{over} over" if over else f"{near} near" if near else "fine")

    # Now: each KPI at its newest bucket in the window.
    latest = [kpi_state(k, next((v for v in reversed(values) if v is not None), None)) for k, values in columns]
    incidents = [bar for bars in _incidents(store, pack, crn, since, since + n * step, now).values() for bar in bars]
    changed = [s for s in segments if s["source"]]
    verdicts = [s for s in changed if s["source"] == "agent"]
    d = store.deployment(crn) or {}

    return {
        "pack": pack, "name": d.get("name") or entry.get("deployment_name") or pack, "deployment": d,
        "segments": segments, "states": states, "texts": texts, "incidents": _merged(incidents),
        "over": latest.count(2), "near": latest.count(1), "watched": len(watched),
        "now_state": _worst(latest), "bad_buckets": states.count(2),
        "open": sum(1 for bar in incidents if bar["state"] == "open"),
        "last_change": changed[-1] if changed else None,
        "verdict": verdicts[-1]["verdict"] if verdicts else "",
        "changes_in_range": sum(1 for s in changed if since <= s["start"] <= since + n * step),
    }


def needs_attention(row: dict) -> bool:
    state = row["deployment"].get("state")

    return bool(row["open"] or row["over"] or (state and state != "GOOD_HEALTH"))


def _sort_key(sort: str):
    if sort == "Recent change":
        return lambda r: (-(r["last_change"]["start"] if r["last_change"] else -1e18), r["name"])

    if sort == "Name":
        return lambda r: r["name"]

    return lambda r: (-needs_attention(r), -r["open"], -r["over"], -r["near"], -r["bad_buckets"], r["name"])


def fleet_model(store, packs: dict, now: float, span: str = DEFAULT_RANGE, query: str = "",
                sort: str = FLEET_SORTS[0], attention_only: bool = False, selected: str = "") -> dict:
    """
    Every deployment on one time axis: per bucket, the worst state of its
    alerting KPIs, with its changes and incidents. One history read per
    deployment, from the same store the deployment view reads.
    """

    firsts = [store.first_sample_at(e.get("deployment_crn") or "") for e in packs.values()]
    firsts = [t for t in firsts if t is not None]
    since, step, n = _window(span, min(firsts) if firsts else None, now)
    rows = [_fleet_row(store, pack, entry, since, step, n, now) for pack, entry in packs.items()]
    words = (query or "").strip().lower()
    shown = [r for r in rows if (not words or words in r["pack"].lower() or words in r["name"].lower())
             and (not attention_only or needs_attention(r))]

    return {
        "now": now, "span": span, "since": since, "step": step, "n": n, "end": since + n * step,
        "rows": sorted(shown, key=_sort_key(sort)), "total": len(rows), "selected": selected or "",
        "attention": sum(1 for r in rows if needs_attention(r)),
        "open": sum(r["open"] for r in rows), "changes": sum(r["changes_in_range"] for r in rows),
        "filtered": bool(words or attention_only),
    }


# ---------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------


def _visible(m: dict, segment: dict) -> bool:
    stop = segment["end"] if segment["end"] is not None else m["now"]

    return stop > max(segment["start"], m["since"]) and segment["start"] < m["end"]


def _pct(m: dict, t: float) -> float:
    return min(100.0, max(0.0, (t - m["since"]) / (m["end"] - m["since"]) * 100))


def _x(m: dict, t: float) -> float:
    return _pct(m, t) * _W / 100


def _bucket_x(m: dict, i: int) -> float:
    return (i + 0.5) / m["n"] * _W


def _runs(m: dict, values: list) -> list:
    """[[(i, v)]]: the present buckets, split at gaps too long to draw across."""

    bridge = max(1, round(2 * max(m["step"], _BRIDGE_SECONDS) / m["step"]))
    runs, last = [], None

    for i, v in enumerate(values):
        if v is None:
            continue

        if last is None or i - last > bridge:
            runs.append([])

        runs[-1].append((i, v))
        last = i

    return runs


def _paths(m: dict, values: list, y, height: int, stepped: bool = False) -> tuple:
    """(line d, area d) for a series."""

    line, area = [], []
    half = _W / m["n"] / 2

    for run in _runs(m, values):
        points = [(_bucket_x(m, i), y(v)) for i, v in run]

        if len(points) == 1:   # a lone bucket: a short flat line, so it shows
            (x, py), = points
            points = [(x - half, py), (x + half, py)]

        if stepped:
            d = f"M{points[0][0]:.1f},{points[0][1]:.1f}" + "".join(
                f"H{x:.1f}V{py:.1f}" for x, py in points[1:])

        else:
            d = "M" + "L".join(f"{x:.1f},{py:.1f}" for x, py in points)

        line.append(d)
        area.append(f"{d}L{points[-1][0]:.1f},{height}L{points[0][0]:.1f},{height}Z")

    return "".join(line), "".join(area)


def _ticks(m: dict) -> list:
    span = m["end"] - m["since"]
    step = next((s for s in _TICK_STEPS if span / s <= _MAX_TICKS), _TICK_STEPS[-1])

    return list(range(math.ceil(m["since"] / step) * step, int(m["end"]) + 1, step))


def _backdrop(m: dict, height: int) -> str:
    """The grid and the change lines of one chart."""

    out = [f'<line class="mx-grid" x1="{_x(m, t):.1f}" x2="{_x(m, t):.1f}" y1="0" y2="{height}"/>'
           for t in _ticks(m)]

    for s in m["segments"]:
        if s["source"] and m["since"] <= s["start"] <= m["end"]:
            sel = " mx-sel" if s["index"] == m["selected"] else ""
            out.append(f'<line class="mx-chg mx-{s["source"]}{sel}" x1="{_x(m, s["start"]):.1f}" '
                       f'x2="{_x(m, s["start"]):.1f}" y1="0" y2="{height}"/>')

    return "".join(out)


def _svg(height: int, body: str) -> str:
    return (f'<svg class="mx-svg" viewBox="0 0 {_W} {height}" preserveAspectRatio="none" '
            f'style="height:{height}px" aria-hidden="true">{body}</svg>')


def _row(gutter: str, plot: str, height: int, cls: str = "") -> str:
    return (f'<div class="mx-row{(" " + cls) if cls else ""}"><div class="mx-gut">{gutter}</div>'
            f'<div class="mx-plot" style="height:{height}px">{plot}</div></div>')


def _val(values: list, texts: list) -> str:
    last = next((t for v, t in zip(reversed(values), reversed(texts)) if v is not None), "—")

    return (f'<span class="mx-val" data-v="{_e("|".join(texts))}" data-last="{_e(last)}">'
            f'{_e(last)}</span>')


def _chart(m: dict, values: list, height: int, unit: str = "", above=None, below=None,
           stepped: bool = False, uid: str = "") -> str:
    present = [v for v in values if v is not None]
    peak = max(present, default=0)
    threshold = above if above is not None else below
    top = max([1 if unit else _COUNT_FLOOR, peak] + ([threshold] if threshold is not None and threshold < 3 * max(peak, 1)
                                          else [])) * 1.12

    def y(v):
        return height - 4 - v / top * (height - 12)

    line, area = _paths(m, values, y, height, stepped)
    body = _backdrop(m, height)

    if not stepped:
        body += f'<path class="mx-area" d="{area}"/>'

    body += f'<path class="mx-ln" d="{line}"/>'

    if threshold is not None and threshold <= top:
        ty = y(threshold)
        clip = (f'<rect x="0" y="0" width="{_W}" height="{ty:.1f}"/>' if above is not None
                else f'<rect x="0" y="{ty:.1f}" width="{_W}" height="{height - ty:.1f}"/>')
        body += (f'<clipPath id="{uid}">{clip}</clipPath><path class="mx-ln-bad" clip-path="url(#{uid})" '
                 f'd="{line}"/><line class="mx-thr" x1="0" x2="{_W}" y1="{ty:.1f}" y2="{ty:.1f}"/>')

    label = f"{top:.0f}%" if unit == "%" else short(top)

    return _svg(height, body) + f'<span class="mx-max">{_e(label)}</span>'


def _header(m: dict) -> str:
    d, entry = m["deployment"], m["entry"]
    shown = sum(1 for s in m["segments"] if s["source"] and m["since"] <= s["start"] <= m["end"])
    facts = []

    if d.get("flow_version") is not None:
        facts.append(f"<b>flow v{_e(d['flow_version'])}</b>")

    if d.get("cluster_size"):
        facts.append(_e(f"{d['cluster_size']} × {d.get('node_count') or '?'}"))

    if d.get("state"):
        facts.append(_e(d["state"]))

    facts.append(f"{shown} change{'' if shown == 1 else 's'} in range")
    facts.append("times UTC")
    name = d.get("name") or entry.get("deployment_name") or m["pack"]

    return (f'<div class="mx-head"><span class="mx-name">{_e(name)}</span>'
            f'<span class="mx-facts">{"".join(f"<span>{f}</span>" for f in facts)}</span></div>')


LEGEND = (
    '<div class="mx-legend">'
    '<span><i class="mx-key-agent"></i>Agent change</span>'
    '<span><i class="mx-key-operator"></i>Operator change</span>'
    '<span><i class="mx-key-minor"></i>Alert rules updated</span>'
    '<span><i class="mx-key-thr"></i>Alert threshold</span>'
    '<span><i class="mx-key-bad"></i>Over threshold</span>'
    '<span><i class="mx-key-in"></i>FlowFiles into the group</span>'
    '<span><i class="mx-inc mx-improved"></i>Incident resolved</span>'
    '<span><i class="mx-inc mx-nochange"></i>no change</span>'
    '<span><i class="mx-inc mx-worsened"></i>worsened</span>'
    '<span><i class="mx-inc mx-rejected"></i>rejected</span>'
    '<span><i class="mx-inc mx-open"></i>open</span>'
    '</div>')


def _axis(m: dict) -> str:
    with_day = m["end"] - m["since"] > 86400
    spans = []

    for t in _ticks(m):
        midnight = t % 86400 == 0
        text = datetime.fromtimestamp(t, tz=timezone.utc).strftime("%b %d") if midnight and not with_day \
            else _when(t, with_day and midnight)
        pct = _pct(m, t)
        edge = " mx-start" if pct < 3 else " mx-end" if pct > 97 else ""
        spans.append(f'<span class="mx-tick{edge}" style="left:{pct:.2f}%">{_e(text)}</span>')

    return _row("Time · UTC", _svg(26, _backdrop(m, 26)) + "".join(spans), 26, "mx-axis")


def _rail(m: dict) -> str:
    parts = [_svg(40, _backdrop(m, 40).replace('y1="0"', 'y1="14"'))]

    for s in m["segments"]:
        if not _visible(m, s):
            continue

        left = _pct(m, max(s["start"], m["since"]))
        right = _pct(m, s["end"] if s["end"] is not None else m["now"])
        cls = " mx-sel" if s["index"] == m["selected"] else " mx-alt" if s["index"] % 2 else ""
        parts.append(f'<div class="mx-band{cls}" style="left:{left:.2f}%;width:{max(0.0, right - left):.2f}%" '
                     f'title="{_e(s["letter"] + " · " + s["short"])}"><b>{_e(s["letter"])}</b> '
                     f'<span>{_e(s["short"])}</span></div>')

    for s in m["segments"]:
        if s["source"] and m["since"] <= s["start"] <= m["end"]:
            sel = " mx-sel" if s["index"] == m["selected"] else ""
            label = f"{_hhmm(s['start'])} · {s['who']} · {KIND_LABEL.get(s['kind'], s['kind'])} · {s['short']}"
            parts.append(f'<button class="mx-mk mx-{s["source"]}{sel}" data-pick="{s["index"]}" '
                         f'style="left:{_pct(m, s["start"]):.2f}%" title="{_e(label)}" aria-label="{_e(label)}">'
                         f'</button>')

    for c in m["minor"]:
        if m["since"] <= c["t"] <= m["end"]:
            parts.append(f'<span class="mx-minor" style="left:{_pct(m, c["t"]):.2f}%" '
                         f'title="{_e(_hhmm(c["t"]) + " · " + (c.get("name") or "Alert rules updated"))}"></span>')

    return _row("Changes", "".join(parts), 40, "mx-rail")


def _detail(m: dict) -> str:
    if m["selected"] is None:
        return ""

    s = m["segments"][m["selected"]]
    kind = KIND_LABEL.get(s["kind"], s["kind"])
    gutter = (f'<span class="mx-lbl">Change {_e(s["letter"])} · {_e(_when(s["start"], True))}</span>'
              f'<span class="mx-meta"><span class="mx-chip mx-{s["source"]}">{_e(s["who"])}</span>{_e(kind)}</span>')
    top = f'<span class="mx-what">{_e(s["what"] or s["short"])}</span>'

    if s["incident"]:
        top += f'<span class="mx-chip">{_e(s["incident"])}</span>'

    body = [f'<div class="mx-detail-top">{top}</div>']

    if s["note"]:
        body.append(f'<p class="mx-note">{_e(s["note"])}</p>')

    if s["result"]:
        body.append(f'<p class="mx-note mx-result">{_e(s["result"])}</p>')

    if s["source"] == "agent":
        body.append(f'<div class="mx-windows">Verdicts {"".join(_window_chip(w) for w in s["windows"])}'
                    f'{_label_chip(s["label"])}</div>')

    else:
        body.append('<div class="mx-windows">No verdict: made outside the agent.</div>')

    return (f'<div class="mx-row mx-detail"><div class="mx-gut">{gutter}</div>'
            f'<div class="mx-detail-body">{"".join(body)}</div></div>')


# A reviewer's call on a fix, in the colour of the verdict it agrees with.
_LABEL_CLASS = {"RIGHT_FIX": "IMPROVED", "WRONG_FIX": "WORSENED", "UNCLEAR": "INCONCLUSIVE"}


def _label_chip(label: str) -> str:
    if not label:
        return ""

    return (f'<span class="mx-chip mx-v-{_LABEL_CLASS.get(label, "pending")}">'
            f'reviewer: {_e(views.LABELS.get(label, label))}</span>')


def _window_chip(w: dict) -> str:
    """One verify window in a change's detail: `1 h improved`, or when it is due."""

    if w["verdict"] == "pending":
        due = f' · due {_e(_when(w["due"], False))}' if w["due"] else ""
        chip = f'<span class="mx-chip mx-v-pending">pending{due}</span>'

    else:
        chip = f'<span class="mx-chip mx-v-{_e(w["verdict"])}">{_e(w["verdict"].replace("_", " ").lower())}</span>'

    if w["regression"]:
        chip += '<span class="mx-chip mx-v-WORSENED">regressed</span>'

    reason = f'<span class="mx-planned">{_e(w["reason"])}</span>' if w["reason"] else ""

    return f'<span class="mx-win">{_e(w["label"])} {chip}{reason}</span>'


def _lane(m: dict, scenario: str) -> str:
    out = []

    for bar in m["incidents"].get(scenario, []):
        left = _pct(m, max(bar["start"], m["since"]))
        right = _pct(m, bar["end"] if bar["end"] is not None else m["now"])
        title = (f"{bar['id']} · {_hhmm(bar['start'])}–{_hhmm(bar['end']) if bar['end'] else 'open'} · "
                 f"{bar['status'].lower()} · {bar['summary']}")
        out.append(f'<span class="mx-inc mx-{bar["state"]}" style="left:{left:.2f}%;width:{max(0.4, right - left):.2f}%" '
                   f'title="{_e(title)}">{_e(bar["id"])}</span>')

    return "".join(out)


def _kpi_gutter(kpi: dict, values: list) -> str:
    metric = kpi.get("metric") or ""
    label = _e(kpi.get("component") or kpi["key"])

    if metric in METRIC_SHORT:
        label += f" <small>{METRIC_SHORT[metric]}</small>"

    meta = []

    if kpi.get("primary"):
        meta.append('<span class="mx-chip mx-decides">decides</span>')

    meta.append(_e(METRIC_LABEL.get(metric, metric)))

    if kpi.get("above") is not None:
        meta.append(f"· alert &gt; {_e(fmt(kpi['above']))}")

    elif kpi.get("below") is not None:
        meta.append(f"· alert &lt; {_e(fmt(kpi['below']))}")

    return (f'<span class="mx-lbl" title="{_e(kpi["key"])}">{label}</span>'
            + _val(values, [fmt(v) if v is not None else "" for v in values])
            + f'<span class="mx-meta">{" ".join(meta)}</span>')


def _flow_row(m: dict, flow: dict) -> str:
    values = [None if a is None or b is None else (a, b) for a, b in zip(flow["in"], flow["out"])]
    texts = ["" if v is None else f"{fmt(v[0])} → {fmt(v[1])} · {fmt(v[1] / v[0] * 100, 'ratio') if v[0] else '—'}"
             for v in values]
    present = [x for v in values if v for x in v]
    top = max([_COUNT_FLOOR] + present) * 1.12
    height = 50

    def y(v):
        return height - 4 - v / top * (height - 12)

    line_in, _ = _paths(m, flow["in"], y, height)
    line_out, _ = _paths(m, flow["out"], y, height)
    body = _backdrop(m, height) + f'<path class="mx-ln-in" d="{line_in}"/><path class="mx-ln" d="{line_out}"/>'
    gutter = ('<span class="mx-lbl">group in → out</span>' + _val(values, texts)
              + f'<span class="mx-meta">{_e(flow["labels"][0])} → {_e(flow["labels"][1])}</span>')

    return _row(gutter, _svg(height, body) + f'<span class="mx-max">{_e(short(top))}</span>', height)


def _level(m: dict, values: list, s: dict):
    """The mean of the stretch's last three buckets inside it and the window."""

    since, step, n = m["since"], m["step"], m["n"]
    lo = max(0, math.ceil((s["start"] - since) / step))
    hi = n if s["end"] is None else min(n, math.floor((s["end"] - since) / step))
    found = [values[i] for i in range(lo, hi) if values[i] is not None][-3:]

    if not found:  # a stretch shorter than a bucket: any bucket it touches
        lo = max(0, math.floor((s["start"] - since) / step))
        hi = n if s["end"] is None else min(n, math.ceil((s["end"] - since) / step))
        found = [values[i] for i in range(lo, hi) if values[i] is not None][-3:]

    return sum(found) / len(found) if found else None


def _ledger(m: dict) -> str:
    shown = [s for s in m["segments"] if _visible(m, s)]

    if not shown:
        return ""

    with_day = m["end"] - m["since"] > 86400
    sel = lambda s: ' class="mx-sel"' if s["index"] == m["selected"] else ""  # noqa: E731
    head = ['<th scope="col"><span class="mx-eyebrow">KPI</span></th>']

    for s in shown:
        until = "now" if s["end"] is None else _when(s["end"], with_day)
        chips = (f'<span class="mx-chip mx-{s["source"]}">{_e(s["who"])}</span>' if s["source"]
                 else f'<span class="mx-chip">v{_e(s["flow"])}</span>' if s["flow"] is not None else "")

        if s["source"] == "agent":
            chips += f'<span class="mx-chip mx-v-{_e(s["verdict"])}">{_e(s["verdict"].replace("_", " ").lower())}</span>'

            if s["regression"]:
                chips += '<span class="mx-chip mx-v-WORSENED">regressed</span>'

            chips += _label_chip(s["label"])

        inner = (f'<span class="mx-letter">{_e(s["letter"])}</span>'
                 f'<span class="mx-when">{_e(_when(s["start"], with_day))}–{_e(until)}</span>'
                 f'<span class="mx-short">{_e(s["short"])}</span><span class="mx-chips">{chips}</span>')
        cell = (f'<button class="mx-colhead" data-pick="{s["index"]}">{inner}</button>' if s["source"]
                else f'<span class="mx-colhead">{inner}</span>')
        head.append(f'<th scope="col"{sel(s)}>{cell}</th>')

    rows = []

    def line(label: str, meta: str, levels: list, good: str, unit: str = "") -> None:
        cells, before = [], None

        for i, (s, level) in enumerate(zip(shown, levels)):
            text, tone = delta(before, level, good, unit) if i else ("", "")
            cells.append(f'<td{sel(s)}><span class="mx-v">{_e(fmt(level, unit))}</span>'
                         f'<span class="mx-d{" mx-" + tone if tone else ""}">{_e(text) or "&nbsp;"}</span></td>')
            before = level

        rows.append(f'<tr><td class="mx-k"><span class="mx-lbl">{label}</span>'
                    f'<span class="mx-meta">{meta}</span></td>{"".join(cells)}</tr>')

    def group(title: str) -> None:
        rows.append(f'<tr class="mx-grp"><td>{_e(title)}</td>' + "".join(f"<td{sel(s)}></td>" for s in shown) + "</tr>")

    for g in m["groups"]:
        group(g["title"])

        for kpi in g["kpis"]:
            metric = kpi.get("metric") or ""
            label = _e(kpi.get("component") or kpi["key"]) + (f" · {METRIC_SHORT[metric]}" if metric in METRIC_SHORT else "")
            meta = (("decides · " if kpi.get("primary") else "") + ("queued" if metric == "queued_count" else "FlowFiles / bucket")
                    + f" · {'lower' if kpi.get('good', 'down') == 'down' else 'higher'} is better")
            line(label, _e(meta), [_level(m, kpi["values"], s) for s in shown], kpi.get("good", "down"))

        if g["flow"]:
            levels = []

            for s in shown:
                a, b = _level(m, g["flow"]["in"], s), _level(m, g["flow"]["out"], s)
                levels.append(b / a * 100 if a and b is not None else None)

            line("group out / in", _e(f"{g['flow']['labels'][0]} → {g['flow']['labels'][1]}"), levels, "up", "ratio")

    group("Deployment · system metrics")

    for k in m["system"]:
        line(_e(k["label"]), _e(k["meta"]), [_level(m, k["values"], s) for s in shown], k["good"], k["unit"])

    return ('<div class="mx-ledger-head"><span class="mx-eyebrow">Change ledger</span>'
            '<p>One column per stretch between changes. A cell is the KPI\'s level at the end of the stretch '
            '(the mean of its last three buckets); below it, the change from the previous column, coloured by '
            'whether that direction is good for the KPI.</p></div>'
            f'<div class="mx-scroll"><table class="mx-ledger"><thead><tr>{"".join(head)}</tr></thead>'
            f'<tbody>{"".join(rows)}</tbody></table></div>')


def render(m: dict) -> str:
    if not m or not m.get("crn"):
        return '<div class="mx"><p class="mx-empty">Select a deployment.</p></div>'

    uid = 0
    rows = [_axis(m), _rail(m), _detail(m)]

    for g in m["groups"]:
        rows.append(_row(f'<span class="mx-lbl mx-title">{_e(g["title"])}</span>',
                         _svg(30, _backdrop(m, 30)) + _lane(m, g["scenario"]), 30, "mx-group"))

        for kpi in g["kpis"]:
            uid += 1
            rows.append(_row(_kpi_gutter(kpi, kpi["values"]),
                             _chart(m, kpi["values"], 50, above=kpi.get("above"), below=kpi.get("below"),
                                    uid=f"mx-clip-{uid}"), 50))

        if g["flow"]:
            rows.append(_flow_row(m, g["flow"]))

    rows.append(_row('<span class="mx-lbl mx-title">Deployment · system metrics</span>',
                     _svg(30, _backdrop(m, 30)), 30, "mx-group"))

    for k in m["system"]:
        texts = [fmt(v, k["unit"]) if v is not None else "" for v in k["values"]]
        gutter = (f'<span class="mx-lbl">{_e(k["label"])}</span>' + _val(k["values"], texts)
                  + f'<span class="mx-meta">{_e(k["meta"])}</span>')
        rows.append(_row(gutter, _chart(m, k["values"], 40, unit=k["unit"], stepped=k["unit"] == "cores"), 40))

    timeline = (f'<div class="mx-tl" data-since="{m["since"]:.0f}" data-step="{m["step"]}" data-n="{m["n"]}">'
                + "".join(rows) + '<div class="mx-cross" hidden></div><div class="mx-crosslbl" hidden></div></div>')

    return (f'<div class="mx">{_header(m)}{LEGEND}<div class="mx-scroll">{timeline}</div>'
            f'{_ledger(m)}</div>')


FLEET_LEGEND = (
    '<div class="mx-legend">'
    '<span><i class="mx-key-s mx-s0"></i>KPIs fine</span>'
    f'<span><i class="mx-key-s mx-s1"></i>near an alert threshold (≥ {_NEAR:.0%})</span>'
    '<span><i class="mx-key-s mx-s2"></i>over one</span>'
    '<span><i class="mx-key-s"></i>no data</span>'
    '<span><i class="mx-key-agent"></i>Agent change</span>'
    '<span><i class="mx-key-operator"></i>Operator change</span>'
    '<span><i class="mx-inc mx-open"></i>open incident</span>'
    '<span><i class="mx-inc mx-improved"></i>resolved</span>'
    '<span><i class="mx-inc mx-nochange"></i>other closed</span>'
    '</div>')

_FLEET_HEIGHT = 42


def _frow(gutter: str, plot: str, stats: str, cls: str = "", attrs: str = "", height: int = _FLEET_HEIGHT) -> str:
    return (f'<div class="mx-row{(" " + cls) if cls else ""}"{attrs}><div class="mx-gut">{gutter}</div>'
            f'<div class="mx-plot" style="height:{height}px">{plot}</div><div class="mx-stats">{stats}</div></div>')


def _strip(f: dict, row: dict) -> str:
    """The health strip, the change lines and the markers of one deployment."""

    w = _W / f["n"]
    m = dict(f, segments=row["segments"], selected=None)
    body = _backdrop(m, _FLEET_HEIGHT).replace('y1="0"', 'y1="14"')
    runs, start = [], 0

    for i, s in enumerate(row["states"] + [object()]):
        if i and s != row["states"][start]:
            runs.append((start, i, row["states"][start]))
            start = i

    body += "".join(f'<rect class="mx-s{s}" x="{a * w:.2f}" y="15" width="{(b - a) * w:.2f}" height="11"/>'
                    for a, b, s in runs if s is not None)
    parts = [_svg(_FLEET_HEIGHT, body)]

    for s in row["segments"]:
        if s["source"] and f["since"] <= s["start"] <= f["end"]:
            label = f"{s['letter']} · {_when(s['start'], True)} · {s['who']} · {KIND_LABEL.get(s['kind'], s['kind'])} · {s['short']}"
            parts.append(f'<button class="mx-mk mx-{s["source"]}" data-pick="{s["index"]}" '
                         f'style="left:{_pct(f, s["start"]):.2f}%" title="{_e(label)}" aria-label="{_e(label)}"></button>')

    for group in row["incidents"]:
        left = _pct(f, max(group["start"], f["since"]))
        right = _pct(f, group["end"] if group["end"] is not None else f["now"])
        title = "\n".join(f"{b['id']} · {_hhmm(b['start'])}–{_hhmm(b['end']) if b['end'] else 'open'} · "
                          f"{b['status'].lower()} · {b['summary']}" for b in group["bars"])
        parts.append(f'<span class="mx-inc mx-finc mx-{group["state"]}" '
                     f'style="left:{left:.2f}%;width:{max(0.4, right - left):.2f}%" title="{_e(title)}">'
                     f'{_e(group["label"])}</span>')

    return "".join(parts)


def _ago(seconds: float) -> str:
    if seconds < 3600:
        return f"{max(1, seconds / 60):.0f} min ago"

    if seconds < 172800:
        return f"{seconds / 3600:.0f} h ago"

    return f"{seconds / 86400:.0f} d ago"


def _fleet_stats(f: dict, row: dict) -> str:
    state = row["now_state"]
    now_text = (f"{row['over']} over" if row["over"] else f"{row['near']} near" if row["near"]
                else STATE_TEXT[state] if row["watched"] else "no alerts")
    # The crosshair swaps the text for the bucket's; at rest it is each KPI's newest value.
    cells = [f'<span class="mx-now mx-n{state if state is not None else "x"}">'
             f'<span class="mx-val" data-v="{_e("|".join(row["texts"]))}" data-last="{_e(now_text)}">'
             f'{_e(now_text)}</span></span>']
    cells.append(f'<span class="mx-open{" mx-has" if row["open"] else ""}">{row["open"] or "—"}</span>')
    last = row["last_change"]

    if last:
        cells.append(f'<span class="mx-last" title="{_e(last["who"] + " · " + last["short"])}">'
                     f'<i class="mx-key-{last["source"]}"></i><b>{_e(last["letter"])}</b> '
                     f'{_e(_ago(f["now"] - last["start"]))}</span>')

    else:
        cells.append('<span class="mx-last mx-none">none</span>')

    verdict = row["verdict"]
    cells.append(f'<span class="mx-chip mx-v-{_e(verdict)}">{_e(verdict.replace("_", " ").lower())}</span>'
                 if verdict else '<span class="mx-none">—</span>')

    return "".join(cells)


def render_fleet(f: dict) -> str:
    if not f or not f.get("total"):
        return '<div class="mx"><p class="mx-empty">No deployments registered yet.</p></div>'

    facts = [f"<b>{f['total']}</b> deployment{'' if f['total'] == 1 else 's'}",
             f"<b>{f['attention']}</b> need{'s' if f['attention'] == 1 else ''} attention",
             f"<b>{f['open']}</b> open incident{'' if f['open'] == 1 else 's'}",
             f"<b>{f['changes']}</b> change{'' if f['changes'] == 1 else 's'} in range", "times UTC"]

    if f["filtered"]:
        facts.insert(1, f"{len(f['rows'])} shown")

    head = (f'<div class="mx-head"><span class="mx-name">Fleet</span>'
            f'<span class="mx-facts">{"".join(f"<span>{x}</span>" for x in facts)}</span></div>')
    axis = _axis(dict(f, segments=[], selected=None))
    axis = axis[:-len("</div>")] + ('<div class="mx-stats mx-stats-head"><span>KPIs now</span><span>Open</span>'
                                    '<span>Last change</span><span>Verdict</span></div></div>')
    rows = [axis]

    for row in f["rows"]:
        d = row["deployment"]
        meta = [f"v{d['flow_version']}" if d.get("flow_version") is not None else "",
                _e(f"{d['cluster_size']} × {d.get('node_count') or '?'}") if d.get("cluster_size") else "",
                f'<span class="mx-state">{_e(HEALTH_TEXT.get(d["state"], d["state"].replace("_", " ").lower()))}</span>'
                if d.get("state") and d["state"] != "GOOD_HEALTH" else ""]
        gutter = (f'<span class="mx-lbl" title="{_e(row["name"])}">{_e(row["name"])}</span>'
                  f'<span class="mx-meta">{" · ".join(x for x in meta if x)}</span>')
        cls = "mx-frow" + (" mx-sel" if row["pack"] == f["selected"] else "")
        attrs = f' data-pack="{_e(row["pack"])}" tabindex="0" role="button" aria-label="Open {_e(row["name"])}"'
        rows.append(_frow(gutter, _strip(f, row), _fleet_stats(f, row), cls, attrs))

    if not f["rows"]:
        rows.append('<p class="mx-empty">No deployment matches the filter.</p>')

    timeline = (f'<div class="mx-tl" data-since="{f["since"]:.0f}" data-step="{f["step"]}" data-n="{f["n"]}">'
                + "".join(rows) + '<div class="mx-cross" hidden></div><div class="mx-crosslbl" hidden></div></div>')

    return (f'<div class="mx mx-fleet">{head}{FLEET_LEGEND}<div class="mx-scroll">{timeline}</div>'
            '<p class="mx-hint">Click a deployment to open its timeline below; click a marker to open it at that change.</p></div>')


# ---------------------------------------------------------------------
# Browser side
# ---------------------------------------------------------------------

# Runs once per page (gr.HTML js_on_load); listeners sit on the
# component's element, so a re-render keeps them. No template literals:
# Gradio's own templating reads "${".
JS = """
function parts(){
  var tl = element.querySelector('.mx-tl');
  if (!tl) return null;
  return {tl: tl, plot: tl.querySelector('.mx-axis .mx-plot'),
          cross: tl.querySelector('.mx-cross'), lbl: tl.querySelector('.mx-crosslbl')};
}
function rest(p){
  p.cross.hidden = true; p.lbl.hidden = true;
  p.tl.querySelectorAll('.mx-val').forEach(function(el){ el.textContent = el.dataset.last; });
}
element.addEventListener('mousemove', function(e){
  var p = parts();
  if (!p || !p.plot) return;
  var r = p.plot.getBoundingClientRect();
  if (!p.tl.contains(e.target) || e.clientX < r.left || e.clientX > r.right){ rest(p); return; }
  var n = +p.tl.dataset.n, since = +p.tl.dataset.since, step = +p.tl.dataset.step;
  var i = Math.min(n - 1, Math.max(0, Math.floor((e.clientX - r.left) / r.width * n)));
  var left = p.plot.offsetLeft + (i + 0.5) / n * p.plot.offsetWidth;
  p.cross.style.left = left + 'px'; p.lbl.style.left = left + 'px';
  p.lbl.textContent = new Date((since + i * step) * 1000).toISOString().slice(11, 16);
  p.cross.hidden = false; p.lbl.hidden = false;
  p.tl.querySelectorAll('.mx-val').forEach(function(el){
    var v = (el.dataset.v || '').split('|')[i];
    el.textContent = v ? v : '—';
  });
});
element.addEventListener('mouseleave', function(){ var p = parts(); if (p) rest(p); });
element.addEventListener('click', function(e){
  var b = e.target.closest('[data-pick]'), row = e.target.closest('[data-pack]');
  if (!b && !row) return;
  var data = {};
  if (b) data.pick = b.dataset.pick;
  if (row) data.pack = row.dataset.pack;
  trigger('click', data);
});
element.addEventListener('keydown', function(e){
  if (e.key !== 'Enter' && e.key !== ' ') return;
  var row = e.target.closest('[data-pack]');
  if (row && e.target === row){ e.preventDefault(); trigger('click', {pack: row.dataset.pack}); }
});
"""

CSS = """
.mx{--mx-gut:250px;--mx-ink:var(--body-text-color);--mx-muted:var(--body-text-color-subdued);
  --mx-surface:var(--block-background-fill);--mx-surface-2:var(--background-fill-secondary);
  --mx-line:var(--border-color-primary);--mx-grid:color-mix(in srgb,var(--border-color-primary) 55%,transparent);
  --mx-accent:var(--color-accent);--mx-accent-tint:color-mix(in srgb,var(--color-accent) 14%,transparent);
  --mx-operator:#c98419;--mx-operator-tint:color-mix(in srgb,#c98419 16%,transparent);
  --mx-good:#2f9e6a;--mx-good-tint:color-mix(in srgb,#2f9e6a 16%,transparent);
  --mx-bad:#d9493e;--mx-bad-tint:color-mix(in srgb,#d9493e 14%,transparent);
  --mx-neutral-tint:color-mix(in srgb,var(--body-text-color-subdued) 18%,transparent);
  --mx-fill:color-mix(in srgb,var(--color-accent) 10%,transparent);
  --mx-mono:var(--font-mono),ui-monospace,SFMono-Regular,Menlo,monospace;
  color:var(--mx-ink);font-size:13px;line-height:1.4;background:var(--mx-surface);
  border:1px solid var(--mx-line);border-radius:8px;overflow:hidden}
.mx *{box-sizing:border-box}
.mx-empty{padding:16px;color:var(--mx-muted);margin:0}
.mx-head{display:flex;flex-wrap:wrap;align-items:center;gap:8px 18px;padding:12px 16px;border-bottom:1px solid var(--mx-line)}
.mx-name{font:600 15px var(--mx-mono)}
.mx-facts{display:flex;flex-wrap:wrap;gap:14px;color:var(--mx-muted)}
.mx-facts b{color:var(--mx-ink);font-weight:500}
.mx-legend{display:flex;flex-wrap:wrap;gap:6px 16px;padding:8px 16px;border-bottom:1px solid var(--mx-line);font-size:12px;color:var(--mx-muted)}
.mx-legend span{display:inline-flex;align-items:center;gap:6px}
.mx-legend i{display:inline-block;flex:none}
.mx-key-agent{width:10px;height:10px;border-radius:50%;background:var(--mx-accent)}
.mx-key-operator{width:10px;height:10px;background:var(--mx-operator)}
.mx-key-minor{width:2px;height:10px;background:var(--mx-muted)}
.mx-key-thr{width:18px;border-top:1px dashed var(--mx-bad)}
.mx-key-bad{width:18px;border-top:2px solid var(--mx-bad)}
.mx-key-in{width:18px;border-top:1.5px dashed var(--mx-muted)}
.mx-legend .mx-inc{position:static;width:18px;height:10px}
.mx-scroll{overflow-x:auto}
.mx-tl{position:relative;min-width:760px}
.mx-row{display:grid;grid-template-columns:var(--mx-gut) 1fr;border-bottom:1px solid var(--mx-grid)}
.mx-gut{padding:5px 10px 5px 16px;display:grid;grid-template-columns:1fr auto;align-items:center;gap:1px 8px;min-width:0;border-right:1px solid var(--mx-line)}
.mx-lbl{font:500 12px var(--mx-mono);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.mx-lbl small{color:var(--mx-muted);font-weight:400}
.mx-val{font:500 12px var(--mx-mono);font-variant-numeric:tabular-nums;text-align:right;white-space:nowrap}
.mx-meta{grid-column:1/-1;display:flex;gap:6px;align-items:center;font-size:11px;color:var(--mx-muted);white-space:nowrap;overflow:hidden}
.mx-plot{position:relative;min-width:0;overflow:hidden}
.mx-svg{display:block;width:100%;position:absolute;inset:0}
.mx-svg *{vector-effect:non-scaling-stroke}
.mx-group{background:var(--mx-surface-2);border-bottom-color:var(--mx-line)}
.mx-group .mx-gut{grid-template-columns:1fr}
.mx-title{font:600 12px var(--font)}
.mx-axis .mx-gut,.mx-rail .mx-gut{color:var(--mx-muted);font-size:11px;letter-spacing:.06em;text-transform:uppercase}
.mx-tick{position:absolute;top:6px;transform:translateX(-50%);font:10px var(--mx-mono);color:var(--mx-muted);white-space:nowrap}
.mx-tick.mx-start{transform:none}.mx-tick.mx-end{transform:translateX(-100%)}
.mx-max{position:absolute;top:1px;left:4px;font:10px var(--mx-mono);color:var(--mx-muted);pointer-events:none}
.mx-grid{stroke:var(--mx-grid);stroke-width:1}
.mx-ln{fill:none;stroke:var(--mx-ink);stroke-width:1.4;stroke-linejoin:round}
.mx-ln-bad{fill:none;stroke:var(--mx-bad);stroke-width:1.8;stroke-linejoin:round}
.mx-ln-in{fill:none;stroke:var(--mx-muted);stroke-width:1.3;stroke-dasharray:3 2}
.mx-area{fill:var(--mx-fill);stroke:none}
.mx-thr{stroke:var(--mx-bad);stroke-width:1;stroke-dasharray:2 3;opacity:.7}
.mx-chg{stroke-width:1;stroke-dasharray:3 3;opacity:.55}
.mx-chg.mx-agent{stroke:var(--mx-accent)}.mx-chg.mx-operator{stroke:var(--mx-operator)}
.mx-chg.mx-sel{stroke-dasharray:none;opacity:1;stroke-width:1.5}
.mx-band{position:absolute;top:0;bottom:0;padding:16px 6px 0;font-size:11px;color:var(--mx-muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.mx-band b{color:var(--mx-ink);font-weight:600}
.mx-band.mx-alt{background:var(--mx-surface-2)}
.mx-band.mx-sel{background:var(--mx-accent-tint)}
.mx .mx-mk{all:unset;position:absolute;top:2px;width:11px;height:11px;transform:translateX(-50%);cursor:pointer;z-index:2;box-shadow:0 0 0 2px var(--mx-surface)}
.mx .mx-mk.mx-agent{border-radius:50%;background:var(--mx-accent)}
.mx .mx-mk.mx-operator{background:var(--mx-operator)}
.mx .mx-mk.mx-sel{box-shadow:0 0 0 2px var(--mx-surface),0 0 0 3.5px var(--mx-ink)}
.mx .mx-mk:focus-visible{outline:2px solid var(--mx-accent);outline-offset:3px}
.mx-minor{position:absolute;top:3px;width:2px;height:9px;transform:translateX(-50%);background:var(--mx-muted);opacity:.7}
.mx-inc{position:absolute;top:8px;height:14px;border:1px solid;border-radius:3px;font:500 10px/12px var(--mx-mono);padding:0 5px;color:var(--mx-ink);white-space:nowrap;overflow:hidden;text-overflow:clip}
.mx-inc.mx-improved{background:var(--mx-good-tint);border-color:var(--mx-good)}
.mx-inc.mx-nochange{background:var(--mx-neutral-tint);border-color:var(--mx-muted)}
.mx-inc.mx-worsened{background:var(--mx-bad-tint);border-color:var(--mx-bad)}
.mx-inc.mx-rejected{background:transparent;border-color:var(--mx-muted);border-style:dashed}
.mx-inc.mx-open{border-color:var(--mx-bad);background:repeating-linear-gradient(45deg,var(--mx-bad-tint) 0 3px,transparent 3px 6px)}
.mx-cross{position:absolute;top:0;bottom:0;width:0;border-left:1px solid var(--mx-ink);opacity:.35;pointer-events:none}
.mx-crosslbl{position:absolute;top:4px;transform:translateX(-50%);font:500 11px var(--mx-mono);background:var(--mx-ink);color:var(--mx-surface);padding:2px 6px;border-radius:3px;pointer-events:none;white-space:nowrap;z-index:3}
.mx-chip{font:500 10px/1 var(--font);letter-spacing:.04em;padding:3px 6px;border-radius:3px;background:var(--mx-surface-2);color:var(--mx-ink);white-space:nowrap}
.mx-chip.mx-decides,.mx-chip.mx-agent{background:var(--mx-accent-tint);color:var(--mx-accent)}
.mx-chip.mx-operator{background:var(--mx-operator-tint);color:var(--mx-operator)}
.mx-chip.mx-v-IMPROVED{background:var(--mx-good-tint);color:var(--mx-good)}
.mx-chip.mx-v-WORSENED{background:var(--mx-bad-tint);color:var(--mx-bad)}
.mx-chip.mx-v-NO_CHANGE,.mx-chip.mx-v-INCONCLUSIVE,.mx-chip.mx-v-pending{background:var(--mx-neutral-tint)}
.mx-detail{background:var(--mx-surface);border-bottom:1px solid var(--mx-line)}
.mx-detail .mx-gut{align-content:start}
.mx-detail-body{padding:8px 16px 10px;display:grid;gap:5px;min-width:0}
.mx-detail-top{display:flex;flex-wrap:wrap;align-items:center;gap:6px 10px}
.mx-what{font:500 12.5px var(--mx-mono);overflow-wrap:anywhere}
.mx-note{margin:0;max-width:90ch;color:var(--mx-ink);opacity:.85}
.mx-windows{display:flex;flex-wrap:wrap;gap:6px 12px;align-items:center;font-size:12px;color:var(--mx-muted)}
.mx-planned{font-size:11px;font-style:italic}
.mx-win{display:inline-flex;gap:4px;align-items:center}
.mx-ledger-head{padding:14px 16px 8px;border-top:1px solid var(--mx-line)}
.mx-ledger-head p{margin:4px 0 0;color:var(--mx-muted);max-width:90ch}
.mx-eyebrow{font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:var(--mx-muted)}
.mx table.mx-ledger{border-collapse:separate;border-spacing:0;width:100%;font-size:12px;margin:0}
.mx-ledger th,.mx-ledger td{padding:6px 12px;text-align:right;border:0;border-bottom:1px solid var(--mx-grid);white-space:nowrap;vertical-align:top;background:transparent}
.mx-ledger th:first-child,.mx-ledger td:first-child{text-align:left;position:sticky;left:0;background:var(--mx-surface);z-index:1;border-right:1px solid var(--mx-line);min-width:220px}
.mx-ledger thead th{vertical-align:bottom;font-weight:400;border-bottom:1px solid var(--mx-line)}
.mx .mx-colhead{all:unset;display:grid;gap:3px;justify-items:end}
.mx button.mx-colhead{cursor:pointer}
.mx button.mx-colhead:focus-visible{outline:2px solid var(--mx-accent);outline-offset:2px}
.mx-letter{font:600 15px var(--font)}
.mx-when{font:11px var(--mx-mono);color:var(--mx-muted)}
.mx-short{font-size:12px}
.mx-chips{display:flex;gap:4px}
.mx-ledger .mx-sel{background:var(--mx-accent-tint)}
.mx-ledger thead th.mx-sel{box-shadow:inset 0 2px 0 var(--mx-accent)}
.mx-ledger tr.mx-grp td{background:var(--mx-surface-2);font-weight:600;text-align:left}
.mx-v{font:500 12px var(--mx-mono);font-variant-numeric:tabular-nums}
.mx-d{display:block;font:11px var(--mx-mono);font-variant-numeric:tabular-nums;color:var(--mx-muted)}
.mx-d.mx-good{color:var(--mx-good)}.mx-d.mx-bad{color:var(--mx-bad)}
.mx-k .mx-lbl{display:block}.mx-k .mx-meta{display:block}
.mx-fleet{--mx-stats:350px}
.mx-fleet .mx-row{grid-template-columns:var(--mx-gut) 1fr var(--mx-stats)}
.mx-fleet .mx-tl{min-width:1040px}
.mx-stats{display:grid;grid-template-columns:70px 40px 1fr 96px;align-items:center;gap:10px;padding:0 12px;border-left:1px solid var(--mx-line);font-size:12px;min-width:0}
.mx-stats-head span{font-size:11px;letter-spacing:.06em;text-transform:uppercase;color:var(--mx-muted);white-space:nowrap}
.mx-frow{cursor:pointer}
.mx-frow:hover{background:var(--mx-surface-2)}
.mx-frow.mx-sel{background:var(--mx-accent-tint);box-shadow:inset 3px 0 0 var(--mx-accent)}
.mx-frow:focus-visible{outline:2px solid var(--mx-accent);outline-offset:-2px}
.mx-s0{fill:color-mix(in srgb,var(--mx-good) 30%,transparent)}
.mx-s1{fill:color-mix(in srgb,var(--mx-bad) 40%,transparent)}
.mx-s2{fill:var(--mx-bad)}
.mx-key-s{width:14px;height:10px;border:1px solid var(--mx-line)}
.mx-key-s.mx-s0{background:color-mix(in srgb,var(--mx-good) 30%,transparent)}
.mx-key-s.mx-s1{background:color-mix(in srgb,var(--mx-bad) 40%,transparent)}
.mx-key-s.mx-s2{background:var(--mx-bad)}
.mx .mx-finc{top:29px;height:11px;font:500 9px/9px var(--mx-mono);padding:0 4px}
.mx-now{font:500 12px var(--mx-mono);white-space:nowrap}
.mx-now .mx-val{font-weight:inherit}
.mx-n2{color:var(--mx-bad);font-weight:600}.mx-n1{color:var(--mx-bad)}.mx-n0{color:var(--mx-good)}.mx-nx{color:var(--mx-muted)}
.mx-open{font:500 12px var(--mx-mono);color:var(--mx-muted)}
.mx-open.mx-has{color:var(--mx-bad);font-weight:600}
.mx-last{display:flex;gap:6px;align-items:center;white-space:nowrap;overflow:hidden}
.mx-last i{display:inline-block;flex:none}
.mx-stats > .mx-chip{justify-self:start}
.mx-none{color:var(--mx-muted)}
.mx-state{color:var(--mx-bad)}
.mx-hint{margin:0;padding:8px 16px;font-size:12px;color:var(--mx-muted);border-top:1px solid var(--mx-line)}
@media (max-width:720px){.mx{--mx-gut:160px}}
"""
