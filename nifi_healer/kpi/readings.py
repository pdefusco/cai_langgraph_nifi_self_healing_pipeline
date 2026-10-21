"""
What every KPI of an incident's process group reads now, for the
diagnosis prompt. The signals name only what breached; in a cascade the
cause is a stage away from the alert, so the agent also needs the queues
and rates that have not breached.

    rows = for_incident(incident, store, now)
    text = render(rows)

From the samples the poller stored, over the last WINDOW_SECONDS (no CDF
call). Each row has the latest value, its alert threshold, a state (over,
near or fine, as the Monitor tab colours it) and a trend. Processor
counts are shown per second (config.COUNT_WINDOW_SECONDS). Back pressure
throttles what a source sends, which hides the intake, so a source that
sent more in the last PEAK_SECONDS shows that peak too.

Only the KPIs the pack defines; NFH-65 makes the coverage uniform.
"""

import config
from flows import packs
from nifi_healer import fleet
from nifi_healer.kpi import detectors, metrics

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
    sources = set(scenario.flow_edges()["in"]) if scenario else set()
    crn = incident.get("deployment_crn") or ""
    rows = []

    for kpi in kpis:
        raw = store.samples(crn, kpi["key"], now - PEAK_SECONDS)
        recent = [(t, v) for t, v in raw if t >= now - WINDOW_SECONDS]
        latest = _per_second(kpi, recent)[-1][1] if recent else None
        source = kpi.get("metric") == "flowfiles_out" and kpi.get("component") in sources

        rows.append({
            "key": kpi["key"], "kind": kpi.get("kind"), "component": kpi.get("component") or "",
            "metric": kpi.get("metric"), "latest": latest,
            "above": kpi.get("above"), "below": kpi.get("below"),
            "state": metrics.kpi_state(kpi, latest),
            # Fitted on the stored values: a live total is the larger number, so its noise floor is lower.
            "trend": _trend(recent) if recent else "",
            "source": source,
            "peak": max(v for _, v in _per_second(kpi, raw)) if source and raw else None,
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


def _line(row: dict) -> str:
    where = {"connection": f"connection {row['component']}", "system": "the deployment's nodes"}.get(
        row["kind"], row["component"] + (" (a source)" if row["source"] else ""))

    if row["latest"] is None:
        return f"- {where}: no data in the last {WINDOW_SECONDS // 60} min"

    shown, word = _SHOWN.get(row["metric"], (_number, row["metric"]))
    text = f"- {where}: {shown(row['latest'])} {word}"

    if row["peak"] is not None and row["peak"] > 1.1 * row["latest"]:
        text += f", up to {shown(row['peak'])} in the last {PEAK_SECONDS // 3600} h"

    if row["above"] is not None:
        text += f" (alert above {shown(row['above'])})"

    elif row["below"] is not None:
        text += f" (alert below {shown(row['below'])})"

    found = [metrics.STATE_TEXT[row["state"]]] if row["state"] is not None else []

    return text + ": " + ", ".join(found + [row["trend"]])


def render(rows: list) -> str:
    return "\n".join(_line(r) for r in rows)
