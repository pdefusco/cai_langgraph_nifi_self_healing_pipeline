"""
Fix success-rate priors: how often each fix has worked, on the incident's
own process group and, across the fleet, on incidents led by the same
symptom.

    rows = for_incident(incident)
    rows[0]       # {scope, fix, worked, no_help, worse, rejected, tries, ...}
    lines(rows)   # what the agent and the reviewer are shown

Counted from the same cases as the remediation memory (rag/memory.py):
applied fixes the KPIs or a reviewer judged, and rejected proposals.
Memory shows a few similar cases in full; the priors count all of them,
so one lucky or unlucky case does not stand for the record.

On the group, a fix is its steps without their values: 4 or 6 concurrent
tasks on the same processor is one fix. On other groups it is the
actions alone, since their components have other names, counted over
incidents led by the same symptom (the leading signal's metric and how
it fired). An applied fix counts as what was applied, so a reviewer's
edit is credited to the edit, not to the agent's proposal.
"""

import config
from nifi_healer.eval import scorecard as sc
from nifi_healer.kpi import detectors, metrics
from nifi_healer.rag import memory
from nifi_healer.store.db import get_store

GROUP, FLEET = "group", "fleet"

# A fix that never worked on the group in this many tries is flagged to the reviewer.
WARN_AFTER = 2

_HOW = {"kpi_breach": "over its threshold", "trend_to_breach": "trending to its threshold",
        "flow_imbalance": "out of balance (in vs out)", "platform_alert": "raising an alert"}
_FIELD = {sc.WORKED: "worked", sc.NO_HELP: "no_help", sc.WORSE: "worse", "rejected": "rejected"}


def symptom(incident: dict) -> str:
    """ "Queued (count) over its threshold": what led the incident, without where."""

    signal = detectors.lead(incident.get("signals") or [])

    if not signal:
        return "no signal"

    try:
        label = metrics.metric(signal.get("metric") or "").label

    except KeyError:
        label = signal.get("metric") or "the deployment"

    return f"{label} {_HOW.get(signal.get('type'), signal.get('type') or '')}".strip()


def group_fix(steps: list) -> str:
    named = sorted({memory.step_text(s, values=False) for s in steps if isinstance(s, dict)})

    return " + ".join(named) or "NO_ACTION"


def fleet_fix(steps: list) -> str:
    return "+".join(sorted({str(s.get("action") or "?") for s in steps if isinstance(s, dict)})) or "NO_ACTION"


def tries(record: dict, skip: str = "") -> list:
    """Every judged or rejected fix in a store.record(): [{pack, process_group, symptom, group_fix, fleet_fix, outcome}]."""

    incidents = {i["id"]: i for i in record.get("incidents") or []}
    out = []

    for case in sc.cases(record):
        if case["incident"] == skip or "/model:oracle" in case["agent_version"] or not case["fix_steps"]:
            continue

        if case["review"] == sc.REJECTED:
            outcome = "rejected"

        elif case["applied"] and case["result"] in _FIELD:
            outcome = case["result"]

        else:
            continue

        incident = incidents[case["incident"]]
        out.append({"pack": incident["pack"], "process_group": incident["process_group"],
                    "symptom": symptom(incident), "group_fix": group_fix(case["fix_steps"]),
                    "fleet_fix": fleet_fix(case["fix_steps"]), "outcome": outcome})

    return out


def _tally(found: list, scope: str, fix: str, **extra) -> list:
    rows = {}

    for one in found:
        row = rows.setdefault(one[fix], dict(scope=scope, fix=one[fix], worked=0, no_help=0, worse=0,
                                             rejected=0, tries=0, **extra))
        row[_FIELD[one["outcome"]]] += 1
        row["tries"] += 1

    return sorted(rows.values(), key=lambda r: (-r["tries"], -r["worked"], r["fix"]))


def for_incident(incident: dict, store=None, k: int = 0) -> list:
    """
    The record of every fix tried on the incident's group, then of the k
    tried most on other groups' incidents led by the same symptom; none
    when priors are off.
    """

    if not config.PRIORS:
        return []

    found = tries((store or get_store()).record(), skip=incident.get("id", ""))
    lead = symptom(incident)
    here = (incident.get("pack"), incident.get("process_group"))
    mine = [t for t in found if (t["pack"], t["process_group"]) == here]
    elsewhere = [t for t in found if (t["pack"], t["process_group"]) != here and t["symptom"] == lead]

    return _tally(mine, GROUP, "group_fix") + \
        _tally(elsewhere, FLEET, "fleet_fix", symptom=lead)[:k or config.PRIORS_TOP_K]


def _counts(row: dict) -> str:
    rest = [f"{row[f]} {name}" for f, name in (("no_help", sc.NO_HELP), ("worse", sc.WORSE),
                                                ("rejected", "rejected")) if row[f]]

    return f"{row['worked']} of {row['tries']} worked" + (f" ({', '.join(rest)})" if rest else "")


def line(row: dict) -> str:
    if row["scope"] == GROUP:
        return f"On this process group, {row['fix']}: {_counts(row)}"

    return f"On other process groups, with {row['symptom']}, {row['fix']}: {_counts(row)}"


def lines(rows: list) -> list:
    return [line(r) for r in rows or []]


def warning(rows: list, steps: list) -> str:
    """Why the reviewer should look twice: this fix has kept failing on this group."""

    fix = group_fix(steps)
    row = next((r for r in rows or [] if r["scope"] == GROUP and r["fix"] == fix), None)

    if not row or row["worked"] or row["tries"] < WARN_AFTER:
        return ""

    return f"this fix has not worked on this process group before: {_counts(row)}"
