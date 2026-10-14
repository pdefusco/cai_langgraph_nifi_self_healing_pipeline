"""
How well the agent does: one case per incident, from the store's record,
and the rates over any grouping of them.

Each case answers, for one incident:

  proposed   was the agent's first proposal valid (it may repair one)?
  match      did its proposal pick the scenario's known-correct fix?
             (`expected`, else `acceptable`; the design label the agent
             never sees). Targets and kinds count, values do not: 4 or 6
             concurrent tasks is tuning, not a different diagnosis.
  review     what the reviewer did: approved it as proposed, approved an
             edit of it, rejected it, or has yet to decide.
  result     what the applied fix did: the reviewer's label when there
             is one (RIGHT_FIX, WRONG_FIX, UNCLEAR), else the deciding
             verify windows' verdict. A dry run has none.
  regressed  a later window measured a resolved incident worse.
  rolled back  a rollback of its fix was applied.
  cited      the past cases (rag/memory.py) its proposal cites.

A rollback incident is not a case: the agent proposed nothing in it. It
shows as its fix's `rolled_back`.

The agent is judged by its own last proposal; an edited fix's result is
the reviewer's as much as the agent's, so `origin` tells them apart.
"""

import statistics

from flows import packs
from nifi_healer.actions import catalog
from nifi_healer.kpi import outcomes
from nifi_healer.runtime.poller import settled

# Results, best first.
WORKED, NO_HELP, WORSE, UNCLEAR, PENDING = "worked", "didn't help", "made worse", "unclear", "pending"
RESULTS = (WORKED, NO_HELP, WORSE, UNCLEAR, PENDING)
_FROM_LABEL = {"RIGHT_FIX": WORKED, "WRONG_FIX": NO_HELP, "UNCLEAR": UNCLEAR}
_FROM_VERDICT = {outcomes.IMPROVED: WORKED, outcomes.NO_CHANGE: NO_HELP, outcomes.WORSENED: WORSE,
                 outcomes.INCONCLUSIVE: UNCLEAR}

# Reviews, and how the agent's pick compares with the design label.
APPROVED, EDITED, REJECTED, UNDECIDED, NO_REVIEW = "approved", "edited", "rejected", "undecided", "no review"
REVIEWS = (APPROVED, EDITED, REJECTED, UNDECIDED, NO_REVIEW)
EXPECTED, ACCEPTABLE, OTHER, UNKNOWN = "expected", "acceptable", "other", "no label"
MATCHES = (EXPECTED, ACCEPTABLE, OTHER, UNKNOWN)

# The fields that make a step a different fix; the rest are its values.
_IDENTITY = ("action", "target", "property", "parameter", "relationship")

GROUPINGS = {
    "Agent version": lambda c: c["agent_version"] or "(none)",
    "Scenario": lambda c: c["scenario_title"],
    "Fix chosen": lambda c: c["action"] or "(no proposal)",
    "Who chose the fix": lambda c: c["origin"],
    "Replay run": lambda c: c.get("run") or "(not a replay)",
    "Replay round": lambda c: f"round {c['round']}" if c.get("round") else "(not a replay)",
    "Cited a past case": lambda c: "cited a past case" if c.get("cited") else
                                   "cited none" if c["proposed"] else "(no proposal)",
}


def step_key(step: dict) -> tuple:
    """A step's identity; a relationship may be one or a list of them, in any order."""

    def part(field):
        if field == "relationship":
            return tuple(catalog.relationships_of(step))

        value = step.get(field)

        return "" if value in (None, "") else str(value)

    return tuple(part(f) for f in _IDENTITY)


def match(steps: list, scenario) -> str:
    """How the steps compare with the scenario's design label."""

    if scenario is None or not scenario.expected:
        return UNKNOWN

    mine = {step_key(s) for s in steps if isinstance(s, dict)} or {step_key({"action": "NO_ACTION"})}

    def covers(fixes) -> bool:
        wanted = {step_key(f) for f in fixes}

        return bool(wanted) and wanted <= mine

    if covers(scenario.expected):
        return EXPECTED

    if any(covers((f,)) for f in scenario.acceptable):
        return ACCEPTABLE

    return OTHER


def _agents(proposals: list) -> list:
    """The agent's own proposals; a reviewer's edit is validated as a proposal too (+review)."""

    return [p for p in proposals if not (p.get("agent_version") or "").endswith("+review")]


def _valid(proposal: dict) -> bool:
    # Its status is SUPERSEDED once a newer one comes; the validation stays.
    return bool((proposal.get("validation") or {}).get("ok"))


def _result(action: dict | None, found: list, label: dict | None) -> tuple:
    """(result, from): from the reviewer's label, else what was measured."""

    if action is None or action["dry_run"]:
        return None, ""

    if action["status"] != "APPLIED":
        return (PENDING, "") if action["status"] in ("PLANNED", "APPLYING") else (None, "")

    if label:
        return _FROM_LABEL.get(label["label"], UNCLEAR), "reviewer"

    status, verdict = settled({o["window_seconds"]: o["verdict"] for o in found})

    if status is None:
        return PENDING, ""

    return _FROM_VERDICT.get(verdict, UNCLEAR), "measured"


def cases(record: dict) -> list:
    """One case per incident in a store.record(), oldest first."""

    by = {name: {} for name in ("proposals", "reviews", "actions", "outcomes", "action_labels")}

    for name, rows in by.items():
        for row in record.get(name) or []:
            rows.setdefault(row["incident_id"], []).append(row)

    out = []
    applied_in = {iid for iid, rows in by["actions"].items()
                  if any(a["status"] == "APPLIED" and not a["dry_run"] for a in rows)}
    rolled_back = {i["rollback_of_action_id"] for i in record.get("incidents") or []
                   if i.get("kind") == "rollback" and i["id"] in applied_in}

    for incident in record.get("incidents") or []:
        if incident.get("kind") == "rollback":
            continue

        iid = incident["id"]
        scenario = packs.SCENARIOS.get(incident.get("scenario") or "")
        mine = _agents(by["proposals"].get(iid, []))
        reviews = by["reviews"].get(iid, [])
        decided = [r for r in reviews if r["decision"] in ("approve", "reject")]
        actions = [a for a in by["actions"].get(iid, []) if a["status"] != "REFUSED"]
        applied = [a for a in actions if a["status"] == "APPLIED" and not a["dry_run"]]
        last = (applied or actions or [None])[-1]
        proposals = {p["id"]: p for p in by["proposals"].get(iid, [])}
        chosen = proposals.get((last or {}).get("proposal_id")) or {}
        pick = next((p for p in reversed(mine) if _valid(p)), mine[-1] if mine else None)
        steps = [s for s in (pick or {}).get("action") or [] if isinstance(s, dict)]
        edited = (chosen.get("agent_version") or "").endswith("+review") \
            or any(r["decision"] == "edit" for r in reviews)
        labels = {r["action_id"]: r for r in by["action_labels"].get(iid, [])}
        result, source = _result(last, [o for o in by["outcomes"].get(iid, [])
                                        if last and o["action_id"] == last["id"]],
                                 labels.get((last or {}).get("id")))

        if decided and decided[-1]["decision"] == "reject":
            review = REJECTED

        elif decided:
            review = EDITED if edited else APPROVED

        elif incident["status"] == "PENDING_REVIEW":
            review = UNDECIDED

        else:
            review = NO_REVIEW

        out.append({
            "incident": iid, "scenario": incident.get("scenario") or "",
            "scenario_title": scenario.group_name if scenario else incident.get("process_group") or "",
            "status": incident["status"], "opened_at": incident["opened_at"],
            "closed_at": incident.get("closed_at"),
            "agent_version": (pick or {}).get("agent_version") or "",
            "proposed": bool(mine), "valid_first": _valid(mine[0]) if mine else None,
            "valid": bool(pick and _valid(pick)),
            "repairs": max(len(mine) - 1, 0),
            "steps": steps, "action": "+".join(dict.fromkeys(str(s.get("action") or "") for s in steps)) or
                                    ("NO_ACTION" if pick and _valid(pick) else ""),
            "match": match(steps, scenario) if pick else UNKNOWN,
            "review": review, "origin": "reviewer's edit" if edited else "agent as proposed",
            "dry_run": bool(last and last["dry_run"] and last["status"] == "DRY_RUN"),
            "applied": bool(applied), "apply_failed": bool(last and last["status"] == "FAILED"),
            "result": result, "result_from": source,
            "regressed": any((o.get("details") or {}).get("regression") for o in by["outcomes"].get(iid, [])),
            "rolled_back": any(a["id"] in rolled_back for a in applied),
            "cited": [str(c) for c in (pick or {}).get("citations") or [] if str(c).startswith("case:")],
            "control": bool(scenario and scenario.family == "NONE"),
        })

    return out


def _rate(hits: int, total: int) -> float | None:
    return hits / total if total else None


def summarize(group: list) -> dict:
    """The rates over a list of cases; None where nothing was there to count."""

    proposed = [c for c in group if c["proposed"]]
    labelled = [c for c in proposed if c["match"] != UNKNOWN]
    decided = [c for c in group if c["review"] in (APPROVED, EDITED, REJECTED)]
    judged = [c for c in group if c["result"] in (WORKED, NO_HELP, WORSE)]
    fixed = [c["closed_at"] - c["opened_at"] for c in group
             if c["status"] == "RESOLVED" and c.get("closed_at")]

    return {
        "incidents": len(group),
        "controls": sum(c["control"] for c in group),
        "proposed": len(proposed),
        "first_valid": sum(bool(c["valid_first"]) for c in proposed),
        "valid_first": _rate(sum(bool(c["valid_first"]) for c in proposed), len(proposed)),
        "repairs": sum(c["repairs"] for c in proposed),
        "cited": _rate(sum(bool(c.get("cited")) for c in proposed), len(proposed)),
        "match": {m: sum(c["match"] == m for c in labelled) for m in MATCHES[:3]},
        "matched": _rate(sum(c["match"] in (EXPECTED, ACCEPTABLE) for c in labelled), len(labelled)),
        "review": {r: sum(c["review"] == r for c in group) for r in REVIEWS},
        "approved": _rate(sum(c["review"] == APPROVED for c in decided), len(decided)),
        "applied": sum(c["applied"] for c in group),
        "dry_runs": sum(c["dry_run"] for c in group),
        "apply_failed": sum(c["apply_failed"] for c in group),
        "results": {r: sum(c["result"] == r for c in group) for r in RESULTS},
        "labelled": sum(c["result_from"] == "reviewer" for c in group),
        "worked": _rate(sum(c["result"] == WORKED for c in judged), len(judged)),
        "judged": len(judged),
        "regressed": sum(c["regressed"] for c in group),
        "rolled_back": sum(c.get("rolled_back", False) for c in group),
        "to_resolve": statistics.median(fixed) if fixed else None,
    }


def table(found: list, grouping: str) -> list:
    """[(group, summary)], the biggest group first."""

    key = GROUPINGS.get(grouping) or GROUPINGS["Agent version"]
    groups = {}

    for case in found:
        groups.setdefault(key(case), []).append(case)

    return sorted(((name, summarize(group)) for name, group in groups.items()),
                  key=lambda row: (-row[1]["incidents"], row[0]))
