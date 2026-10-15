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

A rate that fell while the group's input fell by half or more (a burst
or a nightly batch ended) is INCONCLUSIVE, not WORSENED: the window
after the change missed the load the fix was for, and a later window,
which holds the next burst, decides.

The verdict's number is its lift (outcome_lift): how far the deciding
KPIs moved their good way, from -100% to +100%, so outcomes compare
without reading each KPI's before and after.
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

# The group's input falling by this much (a burst or a batch ended)
# lowers its rates with no help from the change.
_LOAD_DROP = 0.5

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


def levels(judged: dict) -> tuple:
    """
    (before, after) for a judged KPI's lift: a queue where it stood when
    the change went in and where it ended the window, a rate each side's
    mean (one bucket of a rate is noise).
    """

    pre, post = judged.get("before") or {}, judged.get("after") or {}
    field = "mean" if judged.get("good") == "up" else "latest"

    return pre.get(field), post.get(field)


def lift(judged: dict) -> float | None:
    """
    How far a judged KPI moved its good way, from -1 to +1: the change
    over the larger of its two levels. +1 is a backlog cleared or a
    stopped rate back, 0 no change, -1 the reverse; a halving or a
    doubling is half of that. None when the KPI was not measured.
    """

    before, after = levels(judged)

    if judged.get("verdict") in (None, INCONCLUSIVE) or before is None or after is None:
        return None

    scale = max(abs(before), abs(after))

    if scale < 1e-9:
        return 0.0

    return (before - after) / scale * (-1 if judged.get("good") == "up" else 1)


def outcome_lift(details: dict) -> dict | None:
    """
    An outcome's lift, one number: of the deciding KPIs, those whose
    signals opened the incident (else the primary ones) and any that got
    worse, the one that moved least its good way, so one queue cleared
    does not hide another still full. {lift, kpi, good, before, after,
    verdict}; None when no deciding KPI was measured.
    """

    measured = [k for k in (details or {}).get("kpis") or [] if lift(k) is not None]
    deciding = [k for k in measured if k.get("primary", True) or k.get("signalled")] or measured
    pool = [k for k in deciding if k.get("signalled")] or deciding
    pool += [k for k in deciding if k["verdict"] == WORSENED and k not in pool]

    if not pool:
        return None

    worst = min(pool, key=lift)
    before, after = levels(worst)

    return {"lift": lift(worst), "kpi": worst.get("kpi"), "good": worst.get("good", "down"),
            "before": before, "after": after, "verdict": worst["verdict"]}


def evaluate(kpis: list, series_by_key: dict, started_at: float, finished_at: float,
             signalled=(), span: dict | None = None) -> dict:
    """
    kpis: the fleet KPI entries of the incident's scenario.
    series_by_key: {kpi key: [(t, value)]} from one chart response.
    signalled: keys of the KPIs whose signals opened the incident.
    span: the plan() of the window judged; the 15 min window when absent.
    """

    judged = []
    group = group_change(kpis, series_by_key, started_at, finished_at, span)
    fell = load_fell(group)

    # Roll-up KPIs are read only as the group's in and out, below.
    for kpi in (k for k in kpis if not k.get("rollup")):
        before, after = windows(series_by_key.get(kpi["key"]) or [], started_at, finished_at, span)
        found = dict(judge(kpi, before, after), signalled=kpi.get("key") in signalled)

        # An intermittent load: the window after the change can miss the
        # next burst, and a later window decides.
        if fell and found["good"] == "up" and found["verdict"] == WORSENED:
            found.update(verdict=INCONCLUSIVE, reason=f"{found['reason']}, but {fell}")

        judged.append(found)

    deciding = [j for j in judged if j["primary"] or j["signalled"]] or judged
    verdict = combine([j["verdict"] for j in deciding])
    side_effects = [j["kpi"] for j in judged if j not in deciding and j["verdict"] == WORSENED]

    return {"verdict": verdict, "kpis": judged, "side_effects": side_effects, "group": group,
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


def load_fell(group: dict) -> str:
    """Why the group's input fell by _LOAD_DROP or more across the change, or "" when it did not."""

    into = (group or {}).get("in") or {}
    before, after = (into.get("before") or {}).get("mean"), (into.get("after") or {}).get("mean")

    if before is None or after is None or before <= 0 or after > before * (1 - _LOAD_DROP):
        return ""

    return f"the load into the group fell from {before:.3g} to {after:.3g} with it"
