"""
A process group's in and out: what its entries send on (FlowFiles Out)
against what its exits receive (FlowFiles In).

    flow = group_flow(kpis, series_by_key)       # one scenario's KPIs
    flow["in"], flow["out"]                       # [(t, summed value)]
    rollup.ratio(flow)                            # out/in over the last buckets

There is no process-group scope for KPIs on CDF, so the roll-up sums the
processor charts at the group's edges (flows/scenarios/base.py
Scenario.flow_edges). A side is summed only over the buckets every one
of its charts has, so a chart that lags does not read as a drop.
"""

from flows import packs


def edges(kpis: list) -> dict:
    """{"in": [kpi], "out": [kpi]}: the KPIs at the group's edges, by its scenario."""

    sides = {"in": [], "out": []}
    scenario = packs.SCENARIOS.get(kpis[0]["scenario"]) if kpis else None

    if scenario is None:
        return sides

    wanted = scenario.flow_edges()

    for kpi in kpis:
        if kpi.get("kind") != "processor":
            continue

        if kpi.get("metric") == "flowfiles_out" and kpi.get("component") in wanted["in"]:
            sides["in"].append(kpi)

        elif kpi.get("metric") == "flowfiles_in" and kpi.get("component") in wanted["out"]:
            sides["out"].append(kpi)

    # Every edge has to be charted, or the sum is of part of the group.
    for side in sides:
        if len({k["component"] for k in sides[side]}) < len(wanted[side]):
            sides[side] = []

    return sides


def summed(series: list) -> list:
    """[(t, sum)] over the timestamps every series has."""

    if not series or not all(series):
        return []

    common = set.intersection(*({t for t, _ in s} for s in series))
    totals = {}

    for s in series:
        for t, v in s:
            if t in common:
                totals[t] = totals.get(t, 0.0) + v

    return sorted(totals.items())


def group_flow(kpis: list, series_by_key: dict) -> dict:
    """{"in": [(t, v)], "out": [(t, v)], "edges": {"in": [names], "out": [names]}}."""

    sides = edges(kpis)

    return {
        "in": summed([series_by_key.get(k["key"]) or [] for k in sides["in"]]),
        "out": summed([series_by_key.get(k["key"]) or [] for k in sides["out"]]),
        "edges": {side: [k["component"] for k in found] for side, found in sides.items()},
    }


def ratio(flow: dict, buckets: int = 4) -> float | None:
    """Out over in across the last few buckets both sides have, or None."""

    common = sorted({t for t, _ in flow["in"]} & {t for t, _ in flow["out"]})[-buckets:]

    if not common:
        return None

    into = sum(v for t, v in flow["in"] if t in common)
    out = sum(v for t, v in flow["out"] if t in common)

    return out / into if into > 1e-6 else None

