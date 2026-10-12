"""
Did a remediation work? The ground truth Phases 2 and 3 learn from.

    verdict = evaluate(kpis, series_by_key, started_at, finished_at)

Both halves of the comparison come from *one* chart response, split at
the change (`metrics.split_at`): buckets are anchored to each request, so
two responses never line up. Buckets from the start of the change to
VERIFY_SETTLE_SECONDS after it finished are dropped; they straddle the
restart of the affected components.

Each KPI is judged in its own good direction:

  good=down (queues)  IMPROVED when it fell under its threshold, is now
                      draining where it was not, or its level dropped by
                      a fifth or more without rising again. The slope
                      matters more than the alert clearing (an emptied
                      queue clears the alert without fixing anything).
  good=up (rates)     IMPROVED when the mean rose by a tenth or more, or
                      it crossed back over its `below` threshold.

The incident's verdict comes from its primary KPIs: any WORSENED wins,
then any IMPROVED, then NO_CHANGE. Too few buckets on either side of
every primary KPI is INCONCLUSIVE. Secondary KPIs that got worse are
reported as side effects.
"""

import config
from nifi_healer.kpi import metrics

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


def period_for(started_at: float, now: float) -> str:
    """The shortest chart period reaching a verify window back before the change."""

    needed = now - (started_at - config.VERIFY_WINDOW_SECONDS)

    for name, seconds in PERIODS:
        if seconds >= needed:
            return name

    return PERIODS[-1][0]


def due_at(finished_at: float) -> float:
    return finished_at + config.VERIFY_SETTLE_SECONDS + config.VERIFY_WINDOW_SECONDS


def windows(series: list, started_at: float, finished_at: float) -> tuple:
    """(before, after): one verify window either side, the settle gap dropped."""

    settle = (finished_at - started_at) + config.VERIFY_SETTLE_SECONDS
    before, after = metrics.split_at(series, started_at, settle)
    before = [(t, v) for t, v in before if t >= started_at - config.VERIFY_WINDOW_SECONDS]
    after = [(t, v) for t, v in after
             if t <= finished_at + config.VERIFY_SETTLE_SECONDS + config.VERIFY_WINDOW_SECONDS]

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


def evaluate(kpis: list, series_by_key: dict, started_at: float, finished_at: float) -> dict:
    """
    kpis: the fleet KPI entries of the incident's scenario.
    series_by_key: {kpi key: [(t, value)]} from one chart response.
    """

    judged = []

    for kpi in kpis:
        before, after = windows(series_by_key.get(kpi["key"]) or [], started_at, finished_at)
        judged.append(judge(kpi, before, after))

    primary = [j for j in judged if j["primary"]] or judged
    verdict = combine([j["verdict"] for j in primary])
    side_effects = [j["kpi"] for j in judged if not j["primary"] and j["verdict"] == WORSENED]

    return {"verdict": verdict, "kpis": judged, "side_effects": side_effects,
            "started_at": started_at, "finished_at": finished_at,
            "settle_seconds": config.VERIFY_SETTLE_SECONDS,
            "window_seconds": config.VERIFY_WINDOW_SECONDS}
