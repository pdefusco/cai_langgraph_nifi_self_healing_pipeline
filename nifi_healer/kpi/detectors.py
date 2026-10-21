"""
Deterministic signal detection: what in one poll of a pack is worth an
incident. No LLM here; the agent only ever sees what these found.

    found = detect(pack_name, fleet_entry, charts, alerts, now, kinds)
    found["signals"]   # [{type, severity, scenario, process_group, ...}]
    found["kpis"]      # {kpi key: summary + status}, for the UI

Signal types:

  kpi_breach       the newest buckets of a KPI breach its threshold, for
                   BREACH_DEBOUNCE buckets and the KPI's tolerance
  platform_alert   DataFlow itself reports the KPI alert as active
  flow_imbalance   conservation check: FlowFiles leave a stage slower
                   than they arrive and no queue is growing to hold the
                   difference (S13 expiration, S14 auto-terminated
                   failures). Neither raises any alert. Skipped across
                   a processor that bundles or splits FlowFiles by
                   design (MergeContent sends one for every thousand it
                   takes in), given the deployed flow's processor kinds.
  trend_to_breach  a queue below its threshold but rising fast enough to
                   cross it within TREND_HORIZON_SECONDS

`critical` signals open incidents; `warning` ones only ride along as
context. Every signal names its scenario, so incidents are opened per
process group: one fault, one incident, however many KPIs show it.
"""

import config
from flows import packs
from nifi_healer.kpi import metrics

CRITICAL = "critical"
WARNING = "warning"

# Logical metrics where a rising value is the problem, for trend_to_breach.
_LEVELS = ("queued_count", "queued_bytes", "percent_full")

# Buckets averaged for the conservation check, smoothing out bursts.
_RATE_BUCKETS = 5

# Buckets fitted for trend_to_breach.
_TREND_BUCKETS = 10

# Processor kinds whose FlowFile count out differs from in by design.
REGROUPING = frozenset((
    "MergeContent", "MergeRecord", "JoinEnrichment", "SplitText", "SplitJson", "SplitXml",
    "SplitRecord", "SplitContent", "SplitAvro", "SplitExcel", "ForkRecord", "PartitionRecord",
    "DuplicateFlowFile", "UnpackContent",
))


def group_name(scenario_id: str) -> str:
    scenario = packs.SCENARIOS.get(scenario_id)

    return scenario.group_name if scenario else ""


def bucket_seconds(series: list) -> float:
    """The chart's bucket width: the median gap between points."""

    gaps = sorted(b[0] - a[0] for a, b in zip(series, series[1:]) if b[0] > a[0])

    return gaps[len(gaps) // 2] if gaps else 0.0


def _signal(kind: str, severity: str, kpi: dict, summary: str, now: float, **evidence) -> dict:
    return {
        "type": kind,
        "severity": severity,
        "scenario": kpi.get("scenario", ""),
        "process_group": group_name(kpi.get("scenario", "")),
        "kpi": kpi.get("key", ""),
        "component": kpi.get("component", ""),
        "metric": kpi.get("metric", ""),
        "summary": summary,
        "evidence": evidence,
        "at": now,
    }


def _where(kpi: dict) -> str:
    return kpi.get("component") or "the deployment"


def _label(kpi: dict) -> str:
    try:
        return metrics.metric(kpi["metric"]).label

    except KeyError:
        return kpi.get("metric", "")


def _number(value) -> str:
    if value is None:
        return "-"

    return f"{value:.0f}" if abs(value) >= 100 else f"{value:.3g}"


# ---------------------------------------------------------------------
# Per-KPI checks
# ---------------------------------------------------------------------


def breach(kpi: dict, series: list, now: float, debounce: int | None = None) -> dict | None:
    above, below = kpi.get("above"), kpi.get("below")

    if above is None and below is None:
        return None

    result = metrics.evaluate_threshold(series, above, below,
                                        debounce or config.BREACH_DEBOUNCE)

    if not result["breaching"]:
        return None

    # The KPI's tolerance, as DataFlow applies it, capped at what the
    # response can show: a 30-minute tolerance on a 30-minute chart is
    # satisfied by every bucket breaching.
    width = bucket_seconds(series)
    held = result["consecutive"] * width
    needed = min(kpi.get("tolerance_seconds") or 0, len(series) * width)

    if held < needed:
        return None

    side = f"above {_number(above)}" if above is not None and result["latest"] > above \
        else f"below {_number(below)}"
    summary = (f"{_label(kpi)} on {_where(kpi)} is {_number(result['latest'])}, {side}, "
               f"for the last {result['consecutive']} bucket(s) (~{held / 60:.0f} min).")

    return _signal("kpi_breach", CRITICAL, kpi, summary, now,
                   latest=result["latest"], since=result["since"],
                   consecutive=result["consecutive"], threshold=result["threshold"],
                   trend_per_min=metrics.trend(series[-_TREND_BUCKETS:]))


def trend_to_breach(kpi: dict, series: list, now: float) -> dict | None:
    above = kpi.get("above")

    if above is None or kpi.get("metric") not in _LEVELS or len(series) < 3:
        return None

    latest = series[-1][1]

    if latest > above:
        return None

    slope = metrics.trend(series[-_TREND_BUCKETS:])

    if slope <= 0:
        return None

    minutes = (above - latest) / slope

    if minutes * 60 > config.TREND_HORIZON_SECONDS:
        return None

    summary = (f"{_label(kpi)} on {_where(kpi)} is {_number(latest)} and rising "
               f"{_number(slope)}/min; it crosses {_number(above)} in ~{minutes:.0f} min.")

    return _signal("trend_to_breach", WARNING, kpi, summary, now,
                   latest=latest, trend_per_min=slope, minutes_to_breach=minutes,
                   threshold={"above": above, "below": None})


# ---------------------------------------------------------------------
# Conservation (S13, S14)
# ---------------------------------------------------------------------


def _dead_letter(kpi: dict) -> bool:
    # The name rule ROUTE_RELATIONSHIP_TO_DLQ checks its target by.
    return kpi["metric"] == "flowfiles_in" and "DLQ" in kpi["component"].upper()


def _pairs(kpis: list) -> list:
    """
    (upstream kpi, downstream kpi, accounted) inside one scenario: a
    stage's own in and out, or one stage's out and a later stage's in.

    A DLQ's FlowFiles In is never a pair's downstream side: what it takes
    is accounted for, not lost. Between two stages, `accounted` is the
    scenario's DLQ in-KPIs, whose rates add to the downstream side.
    """

    by_scenario = {}

    for kpi in kpis:
        if kpi.get("rollup"):
            continue    # charted for the group's in/out, not paired

        if kpi.get("kind") == "processor" and kpi.get("metric") in ("flowfiles_in", "flowfiles_out"):
            by_scenario.setdefault(kpi["scenario"], []).append(kpi)

    pairs = []

    for group in by_scenario.values():
        dead = [k for k in group if _dead_letter(k)]
        ins = {k["component"]: k for k in group if k["metric"] == "flowfiles_in" and k not in dead}
        outs = {k["component"]: k for k in group if k["metric"] == "flowfiles_out"}

        for component in ins.keys() & outs.keys():
            pairs.append((ins[component], outs[component], []))

        for source, out in outs.items():
            for sink, into in ins.items():
                if source != sink and source not in ins and sink not in outs:
                    pairs.append((out, into, dead))

    return pairs


def rising(series: list) -> bool:
    if len(series) < 3:
        return False

    tail = series[-_TREND_BUCKETS:]
    first, last = tail[0][1], tail[-1][1]

    return metrics.trend(tail) > 0 and last > first * 1.1 and last - first >= 1


def falling(series: list) -> bool:
    if len(series) < 3:
        return False

    tail = series[-_TREND_BUCKETS:]
    first, last = tail[0][1], tail[-1][1]

    return metrics.trend(tail) < 0 and last < first * 0.9 and first - last >= 1


def _rate(points: list) -> float:
    tail = points[-_RATE_BUCKETS:]

    return sum(v for _, v in tail) / len(tail) if tail else 0.0


def imbalance(upstream: dict, downstream: dict, series: dict, queues: list, now: float,
              accounted: list = ()) -> dict | None:
    """accounted: DLQ in-KPIs whose FlowFiles count as delivered downstream."""

    up = series.get(upstream["key"]) or []
    down = series.get(downstream["key"]) or []

    if len(up) < 2 or len(down) < 2:
        return None

    rate_in = _rate(up)
    rate_out = _rate(down) + sum(_rate(series.get(k["key"]) or []) for k in accounted)

    if rate_in <= 1e-6:
        return None

    ratio = rate_out / rate_in

    if ratio >= config.IMBALANCE_RATIO:
        return None

    growing = [q["component"] for q in queues if rising(series.get(q["key"]) or [])]

    if growing:
        # A backlog is holding the difference: a throughput problem, which
        # the queue's own KPI reports.
        return None

    if upstream["component"] == downstream["component"]:
        span = f"{upstream['component']} sends out"

    else:
        span = f"{downstream['component']} receives"

    summary = (f"{span} {ratio:.0%} of the FlowFiles {upstream['component']} "
               f"{'takes in' if upstream['metric'] == 'flowfiles_in' else 'sends'} "
               f"({_number(rate_out)} vs {_number(rate_in)}), and no queue in the group is "
               "growing to hold the difference: data is being dropped or held inside "
               "the component.")

    return _signal("flow_imbalance", CRITICAL, downstream, summary, now,
                   upstream=upstream["key"], downstream=downstream["key"],
                   rate_in=rate_in, rate_out=rate_out, ratio=ratio,
                   queues_checked=[q["key"] for q in queues])


# ---------------------------------------------------------------------
# Platform alerts
# ---------------------------------------------------------------------


def match_alert(event: dict, kpis: list) -> dict | None:
    """
    The KPI an active-alert event is about, or None.

    Events carry no KPI id, only text, so the match is by component name
    (unique per pack) and chart label in the event's name and message.
    """

    text = f"{event.get('name') or ''} {event.get('message') or ''}"
    folded = text.lower()
    best, best_score = None, 0

    for kpi in kpis:
        if kpi.get("above") is None and kpi.get("below") is None:
            continue    # no threshold, so no alert

        score = 0

        if kpi.get("component") and kpi["component"] in text:
            score += 2

        if _label(kpi).lower() in folded or metrics.metric(kpi["metric"]).metric_id.lower() in folded:
            score += 1

        if kpi.get("kind") == "system" and score == 1:
            score += 1

        if score > best_score:
            best, best_score = kpi, score

    return best if best_score >= 2 else None


def alert_signals(alerts: list, kpis: list, now: float) -> tuple:
    """(signals, unmatched events)."""

    signals, unmatched = [], []

    for event in alerts:
        kpi = match_alert(event, kpis)

        if kpi is None:
            unmatched.append(event)
            continue

        first = event.get("firstOccurrence")
        summary = (f"DataFlow alert active: {event.get('name') or _label(kpi)}"
                   + (f" ({event.get('severity')})" if event.get("severity") else "") + ".")
        signals.append(_signal("platform_alert", CRITICAL, kpi, summary, now,
                               event=event.get("name"), message=event.get("message"),
                               alert_severity=event.get("severity"),
                               first_occurrence=first / 1000.0 if first else None))

    return signals, unmatched


# ---------------------------------------------------------------------
# One poll
# ---------------------------------------------------------------------


def detect(pack_name: str, entry: dict, charts: list, alerts: list, now: float,
           kinds: dict | None = None) -> dict:
    """kinds: {component name: processor kind} of the deployed flow version, if read."""

    kpis = entry.get("kpis") or []
    kinds = kinds or {}
    series, status = {}, {}
    signals = []

    for kpi in kpis:
        chart = metrics.find_chart(charts, kpi["kind"], kpi["component"], kpi["metric"],
                                   kpi.get("chart"))
        points = metrics.points(chart)
        series[kpi["key"]] = points
        summary = metrics.summarize(points)
        summary.update(scenario=kpi["scenario"], component=kpi["component"],
                       metric=kpi["metric"], above=kpi.get("above"), below=kpi.get("below"),
                       primary=kpi.get("primary", True), good=kpi.get("good", "down"),
                       rollup=kpi.get("rollup", False),
                       status="missing" if chart is None else "ok")
        status[kpi["key"]] = summary

        if not points:
            continue

        found = breach(kpi, points, now) or trend_to_breach(kpi, points, now)

        if found:
            signals.append(found)
            summary["status"] = "breaching" if found["severity"] == CRITICAL else "warning"

    queues = {}

    for kpi in kpis:
        if kpi.get("metric") == "queued_count":
            queues.setdefault(kpi["scenario"], []).append(kpi)

    for upstream, downstream, accounted in _pairs(kpis):
        if {kinds.get(upstream["component"]), kinds.get(downstream["component"])} & REGROUPING:
            continue

        found = imbalance(upstream, downstream, series, queues.get(upstream["scenario"], []), now,
                          accounted)

        if found:
            signals.append(found)
            status[downstream["key"]]["status"] = "imbalance"

    from_alerts, unmatched = alert_signals(alerts, kpis, now)

    for signal in from_alerts:
        if status.get(signal["kpi"], {}).get("status") == "ok":
            status[signal["kpi"]]["status"] = "alert"

    signals.extend(from_alerts)

    return {"pack": pack_name, "signals": signals, "kpis": status,
            "unmatched_alerts": unmatched, "at": now}


def by_group(signals: list) -> dict:
    """{process group: [signals]}, for groups with at least one critical signal."""

    groups = {}

    for signal in signals:
        if signal.get("process_group"):
            groups.setdefault(signal["process_group"], []).append(signal)

    return {g: s for g, s in groups.items() if any(x["severity"] == CRITICAL for x in s)}


def lead(signals: list) -> dict | None:
    """An incident's most telling signal."""

    order = {"kpi_breach": 0, "flow_imbalance": 1, "platform_alert": 2, "trend_to_breach": 3}
    ranked = sorted(signals, key=lambda s: (s.get("severity") != CRITICAL, order.get(s.get("type"), 9)))

    return ranked[0] if ranked else None


def headline(signals: list) -> str:
    """One line for an incident: its most telling signal."""

    return (lead(signals) or {}).get("summary", "")
