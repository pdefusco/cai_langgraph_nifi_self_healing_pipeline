"""
KPI metrics: what to ask DataFlow for, and how to read what comes back.

Scenarios name a *logical* metric ("queued_count"). This module maps it
to the DataFlow metricId, the KPI scope and the alert unit, so an id that
turns out wrong on a live tenant is corrected in one file,
provision/out/metric_overrides.json, instead of in 21 scenarios:

    {"flowfiles_out": {"metric_id": "processorAmountFlowFilesSent", "verified": true}}

provision/03_discover_kpi_metadata.py dumps every metric the tenant
offers (`kpiMetaData.kpiScopes[].metricTypes[]`), which is where the
corrections come from.

Reading charts. A MetricChart has no id: (componentType, componentName,
name) is its only identity, and `name` may be a label rather than the
metricId. A chart is looked up by the tuple provisioning recorded for the
KPI when there is one, else by component name plus metricId or label.

Every chart response holds ~25 buckets anchored to the moment of the
request, so two responses never share bucket boundaries. Anything that
compares before and after must do it inside one response (`split_at`).

Ported in part from SIB self_healing/df_api.py (latest_sample, slope).
"""

import json
import math
import os
from dataclasses import dataclass, replace

import config


@dataclass(frozen=True)
class Metric:
    logical: str
    metric_id: str
    component_type: str      # KPI metricComponentType
    unit_id: str             # ConfiguredAlertThreshold.unitId
    label: str               # chart name, where known
    verified: bool = False   # confirmed on a live tenant


_BUILTIN = {
    m.logical: m
    for m in (
        # ids, labels and unit ids as kpiMetaData offers them on CDF with
        # NiFi 2.6.0 (unitTypes: count, byte, percentage, ...). The
        # flow-level scopes come from get-flow-configuration-metadata-in-
        # deployment, SYSTEM from get-deployment-configuration-metadata.
        Metric("queued_count", "connectionAmountQueued", "NIFI_CONNECTION", "count",
               "Flow Files Queued", verified=True),
        Metric("queued_bytes", "connectionBytesQueued", "NIFI_CONNECTION", "byte",
               "Bytes Queued", verified=True),
        Metric("percent_full", "connectionPercentFull", "NIFI_CONNECTION", "percentage",
               "Percent Full", verified=True),
        Metric("bytes_out", "processorAmountBytesSent", "NIFI_PROCESSOR", "byte",
               "Bytes Sent", verified=True),
        # "received from a source within the flow"; processorAmountFlowFiles-
        # Received counts only data from outside the flow.
        Metric("flowfiles_in", "processorAmountItemsInput", "NIFI_PROCESSOR", "count",
               "Flow Files Input to processor", verified=True),
        Metric("flowfiles_out", "processorAmountFlowFilesOut", "NIFI_PROCESSOR", "count",
               "Flow Files Out from processor", verified=True),
        Metric("cpu", "cpuUtilization", "SYSTEM", "percentage", "CPU Utilization", verified=True),
    )
}

# Which KPI kinds may use which scope.
KIND_SCOPES = {
    "connection": "NIFI_CONNECTION",
    "processor": "NIFI_PROCESSOR",
    "system": "SYSTEM",
}

_cache = {"mtime": None, "table": dict(_BUILTIN)}


def table() -> dict:
    """The metric table, with provision/out/metric_overrides.json applied."""

    path = config.METRIC_OVERRIDES_FILE

    try:
        mtime = os.path.getmtime(path)

    except OSError:
        mtime = None

    if mtime != _cache["mtime"]:
        merged = dict(_BUILTIN)

        if mtime is not None:
            with open(path) as handle:
                overrides = json.load(handle)

            for logical, fields in overrides.items():
                if logical in merged:
                    merged[logical] = replace(merged[logical], **fields)

                else:
                    merged[logical] = Metric(logical=logical, **fields)

        _cache.update(mtime=mtime, table=merged)

    return _cache["table"]


def metric(logical: str) -> Metric:
    try:
        return table()[logical]

    except KeyError:
        raise KeyError(f"unknown metric {logical!r}; known: {sorted(table())}") from None


def logical_for(metric_id: str) -> str:
    """The logical name for a metricId, or "" if it is not in the table."""

    for logical, entry in table().items():
        if entry.metric_id == metric_id:
            return logical

    return ""


# ---------------------------------------------------------------------
# KPI payloads
# ---------------------------------------------------------------------


def kpi_payload(kpi, component_id: str = "") -> dict:
    """
    The ConfiguredKpi for a scenario Kpi, as update-flow-in-deployment
    --kpis takes it.

    `component_id` is the semicolon-joined ancestry from
    get-flow-configuration-metadata-in-deployment (unused for SYSTEM).
    The CLI fails client-side if frequencyTolerance.unit.id is missing,
    so the unit is always sent.
    """

    entry = metric(kpi.metric)
    scope = KIND_SCOPES[kpi.kind]

    if entry.component_type != scope:
        raise ValueError(f"{kpi.key}: {kpi.metric} is a {entry.component_type} metric, not {scope}")

    payload = {"metricId": entry.metric_id, "metricComponentType": scope}

    if scope != "SYSTEM":
        if not component_id:
            raise ValueError(f"{kpi.key}: a {scope} KPI needs a componentId")

        payload["componentId"] = component_id

    if kpi.alert:
        alert = {
            "frequencyTolerance": {
                "value": max(1, math.ceil(kpi.tolerance_seconds / 60)),
                "unit": {"id": "MINUTES"},
            },
        }

        if kpi.above is not None:
            alert["thresholdMoreThan"] = {"unitId": entry.unit_id, "value": kpi.above}

        if kpi.below is not None:
            alert["thresholdLessThan"] = {"unitId": entry.unit_id, "value": kpi.below}

        payload["alert"] = alert

    return payload


def same_kpi(a: dict, b: dict) -> bool:
    """Two ConfiguredKpis measure the same thing (ignoring the alert)."""

    return (a.get("metricId"), a.get("metricComponentType"), a.get("componentId") or "") == \
           (b.get("metricId"), b.get("metricComponentType"), b.get("componentId") or "")


# ---------------------------------------------------------------------
# Reading charts
# ---------------------------------------------------------------------


def _norm(text) -> str:
    return "".join(ch for ch in str(text or "").lower() if ch.isalnum())


def _norm_type(text) -> str:
    text = str(text or "").upper()

    return text[5:] if text.startswith("NIFI_") else text


def chart_key(chart: dict) -> tuple:
    return (chart.get("componentType"), chart.get("componentName"), chart.get("name"))


def find_chart(charts: list, kind: str, component: str, logical: str,
               recorded: tuple | list | None = None) -> dict | None:
    """
    The chart for one KPI, or None.

    `recorded` is the (componentType, componentName, name) provisioning
    saw for this KPI on the live tenant; it wins when present.
    """

    if recorded:
        recorded = tuple(recorded)

        for chart in charts:
            if chart_key(chart) == recorded:
                return chart

    entry = metric(logical)
    names = {_norm(entry.metric_id), _norm(entry.label)}
    scope = _norm_type(KIND_SCOPES[kind])

    for chart in charts:
        if _norm(chart.get("name")) not in names:
            continue

        if kind == "system":
            if _norm_type(chart.get("componentType")) in (scope, "", "DEPLOYMENT", "FLOW"):
                return chart

            continue

        if chart.get("componentName") == component and _norm_type(chart.get("componentType")) == scope:
            return chart

    # Some tenants report the scope differently; a unique name match is
    # still unambiguous because component names are unique per pack.
    if kind != "system":
        loose = [c for c in charts
                 if c.get("componentName") == component and _norm(c.get("name")) in names]

        if len(loose) == 1:
            return loose[0]

    return None


def points(chart: dict | None) -> list:
    """[(epoch seconds, value)], oldest first, buckets with no value dropped."""

    if not chart:
        return []

    data = (chart.get("metrics") or {}).get("datas") or chart.get("datas") or []
    out = []

    for item in data:
        if not isinstance(item, dict):
            continue

        timestamp, value = item.get("timestamp"), item.get("value")

        if timestamp is None or value is None:
            continue

        try:
            out.append((float(timestamp) / 1000.0, float(value)))

        except (TypeError, ValueError):
            continue

    out.sort()

    return out


def latest_sample(chart: dict | None):
    """The newest (epoch seconds, value), or None."""

    series = points(chart)

    return series[-1] if series else None


def trend(series: list) -> float:
    """Least-squares slope, in units per minute; 0.0 with under 2 points."""

    if len(series) < 2:
        return 0.0

    n = len(series)
    mean_t = sum(t for t, _ in series) / n
    mean_v = sum(v for _, v in series) / n
    var = sum((t - mean_t) ** 2 for t, _ in series)

    if var == 0:
        return 0.0

    cov = sum((t - mean_t) * (v - mean_v) for t, v in series)

    return cov / var * 60.0


def drain_rate(series: list) -> float:
    """
    How fast a queue empties when it does, in units per minute (>= 0).

    The median drop over the bucket pairs that fell and stayed above zero
    (a pair that hits zero only shows how much was left, not how fast it
    went). SIB's `slope()`; used to tell a draining backlog from a stuck one.
    """

    rates = []

    for (t0, v0), (t1, v1) in zip(series, series[1:]):
        if v1 < v0 and v1 > 0 and t1 > t0:
            rates.append((v0 - v1) / (t1 - t0) * 60.0)

    if not rates:
        return 0.0

    rates.sort()
    middle = len(rates) // 2

    return rates[middle] if len(rates) % 2 else (rates[middle - 1] + rates[middle]) / 2


def breaches(value: float, above=None, below=None) -> bool:
    if above is not None and value > above:
        return True

    if below is not None and value < below:
        return True

    return False


def evaluate_threshold(series: list, above=None, below=None, debounce: int = 1) -> dict:
    """
    Is the newest end of `series` breaching, and for how long?

    `consecutive` counts the newest buckets that breach in a row; the
    signal is `breaching` only once that reaches `debounce`.
    """

    consecutive = 0
    since = None

    for t, v in reversed(series):
        if not breaches(v, above, below):
            break

        consecutive += 1
        since = t

    latest = series[-1][1] if series else None

    return {
        "breaching": bool(series) and consecutive >= max(1, debounce),
        "consecutive": consecutive,
        "since": since,
        "latest": latest,
        "threshold": {"above": above, "below": below},
    }


def summarize(series: list) -> dict:
    if not series:
        return {"n": 0, "latest": None, "min": None, "max": None, "mean": None,
                "trend_per_min": 0.0, "drain_per_min": 0.0}

    values = [v for _, v in series]

    return {
        "n": len(series),
        "first_at": series[0][0],
        "latest_at": series[-1][0],
        "latest": values[-1],
        "min": min(values),
        "max": max(values),
        "mean": sum(values) / len(values),
        "trend_per_min": trend(series),
        "drain_per_min": drain_rate(series),
    }


def summarize_chart(chart: dict | None) -> dict:
    summary = summarize(points(chart))

    if chart:
        summary["chart"] = list(chart_key(chart))
        alert = chart.get("alert") or {}
        summary["alert"] = {"above": alert.get("thresholdMoreThan"),
                            "below": alert.get("thresholdLessThan")}

    return summary


def split_at(series: list, moment: float, settle: float = 0.0) -> tuple:
    """
    (before, after) around `moment`, dropping `settle` seconds after it.

    Both halves come from one response, so they share bucket boundaries.
    A bucket is assigned by its timestamp; the one straddling the change
    falls inside the settle gap when settle is at least one bucket wide.
    """

    before = [(t, v) for t, v in series if t < moment]
    after = [(t, v) for t, v in series if t >= moment + settle]

    return before, after
