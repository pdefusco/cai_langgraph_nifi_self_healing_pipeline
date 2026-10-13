"""
Did a remediation work? The ground truth Phases 2 and 3 learn from.

    span = plan(window, started_at, now)
    verdict = evaluate(kpis, series_by_key, started_at, finished_at, signalled, span)

Each verify window (config.VERIFY_WINDOWS) is judged from *one* chart
response, split at the change: buckets are anchored to each request, so
two responses never line up. The 15 min window reads the 1 h chart, the
1 h window the 12 h chart and the 12 h window the 1 day chart, each with
at least two buckets after the change. Buckets from the start of the
change to the settle gap after it finished (VERIFY_SETTLE_SECONDS, or a
bucket if that is longer) are dropped; they straddle the restart of the
affected components. So is a before bucket that runs into the change.

Each KPI is judged in its own good direction:

  good=down (queues)  IMPROVED when it fell under its threshold, is now
                      draining where it was not, or its level dropped by
                      a fifth or more without rising again. The slope
                      matters more than the alert clearing (an emptied
                      queue clears the alert without fixing anything).
  good=up (rates)     IMPROVED when the mean rose by a tenth or more, or
                      it crossed back over its `below` threshold.

The incident's verdict comes from its primary KPIs and the KPIs whose
signals opened it (S17's queue never fills live, the bad endpoint fails
fast, so only its stub's rate shows the fault): any WORSENED wins, then
any IMPROVED, then NO_CHANGE. Too few buckets on either side of every
deciding KPI is INCONCLUSIVE. Other KPIs that got worse are reported as
side effects, and the group's in and out (kpi/rollup.py) alongside.
"""

import config
from nifi_healer.kpi import metrics, rollup

IMPROVED = "IMPROVED"
NO_CHANGE = "NO_CHANGE"
WORSENED = "WORSENED"
INCONCLUSIVE = "INCONCLUSIVE"

# A change over the window smaller than this fraction of the KPI's
# scale is noise.
_EPSILON = 0.05

# Relative level changes that count, for level and rate KPIs.
_LEVEL_DROP = 0.2
_RATE_RISE = 0.1

PERIODS = (("LAST_THIRTY_MINUTES", 1800), ("LAST_ONE_HOUR", 3600),
           ("LAST_TWELVE_HOURS", 12 * 3600), ("LAST_ONE_DAY", 24 * 3600))

# Buckets per chart: ~25, so a bucket is at most a 24th of the period.
_BUCKETS = 24


def _plan(name: str, seconds: int, window: float) -> dict:
    bucket = seconds / _BUCKETS

    return {"period": name, "seconds": seconds, "bucket": bucket, "window": window,
            "settle": max(config.VERIFY_SETTLE_SECONDS, bucket),
            "before": max(config.VERIFY_WINDOW_SECONDS, 3 * bucket)}


def plan(window: float, started_at: float, now: float) -> dict | None:
    """
    The chart to judge `window` from, read at `now`: the shortest period
    with at least two buckets in the window that still reaches back over
    the before window. Its buckets set the settle gap (the bucket
    straddling the change is dropped) and widen the before window to
    three. None when no chart reaches back that far any more.
    """

    for name, seconds in PERIODS:
        found = _plan(name, seconds, window)

        if window >= 2 * found["bucket"] and seconds >= now - (started_at - found["before"]) + found["bucket"]:
            return found

    return None


def due_at(finished_at: float, window: float | None = None, started_at: float | None = None) -> float:
    """When `window` closes: read on time, its chart is the shortest that holds it whole."""

    window = window or config.VERIFY_WINDOW_SECONDS
    started_at = finished_at if started_at is None else started_at

    for name, seconds in PERIODS:
        found = _plan(name, seconds, window)
        span = found["before"] + (finished_at - started_at) + found["settle"] + window + found["bucket"]

        if window >= 2 * found["bucket"] and seconds >= span:
            return finished_at + found["settle"] + window

    return finished_at + config.VERIFY_SETTLE_SECONDS + window


def windows(series: list, started_at: float, finished_at: float, span: dict | None = None) -> tuple:
    """
    (before, after): the before window up to the change, the after window
    from the end of the settle gap. `span` is a plan(); the 15 min window
    at a fine chart's buckets when absent.
    """

    span = span or {"bucket": 0.0, "settle": config.VERIFY_SETTLE_SECONDS,
                    "before": config.VERIFY_WINDOW_SECONDS, "window": config.VERIFY_WINDOW_SECONDS}
    start, end = finished_at + span["settle"], finished_at + span["settle"] + span["window"]
    before = [(t, v) for t, v in series
              if started_at - span["before"] <= t and t + span["bucket"] < started_at]
    after = [(t, v) for t, v in series if start <= t <= end]

    return before, after


def judge(kpi: dict, before: list, after: list) -> dict:
    """The verdict for one KPI, with the numbers behind it."""

    pre, post = metrics.summarize(before), metrics.summarize(after)
    out = {"kpi": kpi.get("key"), "good": kpi.get("good", "down"), "primary": kpi.get("primary", True),
           "before": pre, "after": post}

    if pre["n"] < 2 or post["n"] < 2:
        return dict(out, verdict=INCONCLUSIVE,
                    reason=f"{pre['n']} bucket(s) before and {post['n']} after; need 2 each")

    above, below = kpi.get("above"), kpi.get("below")

    if out["good"] == "down":
        scale = max(abs(pre["mean"]), abs(above or 0.0), 1.0)
        minutes = max((post["latest_at"] - post["first_at"]) / 60.0, 1.0)
        pre_delta = pre["trend_per_min"] * minutes / scale
        post_delta = post["trend_per_min"] * minutes / scale

        if above is not None and pre["mean"] > above and post["max"] <= above:
            return dict(out, verdict=IMPROVED, reason=f"back under {above} for the whole window")

        if post_delta < -_EPSILON and post_delta < pre_delta - _EPSILON:
            return dict(out, verdict=IMPROVED,
                        reason=f"draining at {post['trend_per_min']:.3g}/min "
                               f"(was {pre['trend_per_min']:+.3g}/min)")

        if post["mean"] < pre["mean"] * (1 - _LEVEL_DROP) and post_delta <= _EPSILON:
            return dict(out, verdict=IMPROVED,
                        reason=f"mean {pre['mean']:.3g} -> {post['mean']:.3g}, not rising")

        if post_delta > _EPSILON and post_delta > pre_delta + _EPSILON:
            return dict(out, verdict=WORSENED,
                        reason=f"rising faster: {pre['trend_per_min']:+.3g}/min -> "
                               f"{post['trend_per_min']:+.3g}/min")

        # A backlog that was already growing keeps raising its mean with no
        # help from the change, so the level is compared with where the old
        # trend was heading, not with where it was.
        gap = ((post["first_at"] + post["latest_at"]) - (pre["first_at"] + pre["latest_at"])) / 120.0
        projected = pre["mean"] + max(pre["trend_per_min"], 0.0) * gap

        if post["mean"] > projected * (1 + _LEVEL_DROP) and post_delta >= -_EPSILON \
                and post["mean"] - projected > _EPSILON * scale:
            return dict(out, verdict=WORSENED,
                        reason=f"mean {pre['mean']:.3g} -> {post['mean']:.3g} "
                               f"(the old trend projected {projected:.3g}), not draining")

        return dict(out, verdict=NO_CHANGE,
                    reason=f"mean {pre['mean']:.3g} -> {post['mean']:.3g}, "
                           f"trend {pre['trend_per_min']:+.3g} -> {post['trend_per_min']:+.3g}/min")

    if below is not None and pre["mean"] < below and post["min"] >= below:
        return dict(out, verdict=IMPROVED, reason=f"back over {below} for the whole window")

    floor = max(abs(pre["mean"]), 1e-6)

    if post["mean"] > pre["mean"] + _RATE_RISE * floor:
        return dict(out, verdict=IMPROVED, reason=f"mean {pre['mean']:.3g} -> {post['mean']:.3g}")

    if post["mean"] < pre["mean"] - _RATE_RISE * floor:
        return dict(out, verdict=WORSENED, reason=f"mean {pre['mean']:.3g} -> {post['mean']:.3g}")

    return dict(out, verdict=NO_CHANGE, reason=f"mean {pre['mean']:.3g} -> {post['mean']:.3g}")


def combine(verdicts: list) -> str:
    found = [v for v in verdicts if v != INCONCLUSIVE]

    if not found:
        return INCONCLUSIVE

    if WORSENED in found:
        return WORSENED

    if IMPROVED in found:
        return IMPROVED

    return NO_CHANGE


def evaluate(kpis: list, series_by_key: dict, started_at: float, finished_at: float,
             signalled=(), span: dict | None = None) -> dict:
    """
    kpis: the fleet KPI entries of the incident's scenario.
    series_by_key: {kpi key: [(t, value)]} from one chart response.
    signalled: keys of the KPIs whose signals opened the incident.
    span: the plan() of the window judged; the 15 min window when absent.
    """

    judged = []

    # Roll-up KPIs are read only as the group's in and out, below.
    for kpi in (k for k in kpis if not k.get("rollup")):
        before, after = windows(series_by_key.get(kpi["key"]) or [], started_at, finished_at, span)
        judged.append(dict(judge(kpi, before, after), signalled=kpi.get("key") in signalled))

    deciding = [j for j in judged if j["primary"] or j["signalled"]] or judged
    verdict = combine([j["verdict"] for j in deciding])
    side_effects = [j["kpi"] for j in judged if j not in deciding and j["verdict"] == WORSENED]

    return {"verdict": verdict, "kpis": judged, "side_effects": side_effects,
            "group": group_change(kpis, series_by_key, started_at, finished_at, span),
            "started_at": started_at, "finished_at": finished_at,
            "settle_seconds": span["settle"] if span else config.VERIFY_SETTLE_SECONDS,
            "window_seconds": span["window"] if span else config.VERIFY_WINDOW_SECONDS,
            "before_seconds": span["before"] if span else config.VERIFY_WINDOW_SECONDS}


def group_change(kpis: list, series_by_key: dict, started_at: float, finished_at: float,
                 span: dict | None = None) -> dict:
    """
    The process group's in and out (kpi/rollup.py) either side of the
    change, and out over in. Shown with the verdict, never deciding it.
    {} when an edge of the group has no chart.
    """

    flow = rollup.group_flow(kpis, series_by_key)

    if not (flow["in"] and flow["out"]):
        return {}

    change = {"edges": flow["edges"]}

    for side in ("in", "out"):
        before, after = windows(flow[side], started_at, finished_at, span)
        change[side] = {"before": metrics.summarize(before), "after": metrics.summarize(after)}

    for when in ("before", "after"):
        into, out = change["in"][when]["mean"], change["out"][when]["mean"]
        change[f"ratio_{when}"] = out / into if into and out is not None else None

    return change
