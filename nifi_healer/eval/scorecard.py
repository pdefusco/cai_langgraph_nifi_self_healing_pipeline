"""
How well the agent does: one case per incident, from the store's record,
and the rates over any grouping of them.

Each case answers, for one incident:

  proposed   was the agent's first proposal valid (it may repair one)?
  match      did its proposal pick the scenario's known-correct fix?
             (`expected`, else `acceptable`; the design label the agent
             never sees). The value counts where it decides whether the
             fix can work: concurrent tasks, a run schedule and a resize
             must go at least as far from what is deployed as the
             label's (more tasks, a shorter schedule, a larger
             deployment, or the other way where the label goes that
             way). Further is tuning; less far is `short`, the right
             lever at a value that can't clear the problem.
  review     what the reviewer did: approved it as proposed, approved an
             edit of it, rejected it, or has yet to decide; or no person
             did and a trusted fix approved itself (auto-approved).
  result     what the applied fix did: the reviewer's label when there
             is one (RIGHT_FIX, WRONG_FIX, UNCLEAR), else the deciding
             verify windows' verdict. A dry run has none.
  lift       how far the deciding KPIs moved their good way in the window
             the verdict settled on, -100%..+100% (kpi/outcomes.py);
             lift_kpi the KPI it was measured on, and its two levels.
  regressed  a later window measured a resolved incident worse.
  rolled back  a rollback of its fix was applied.
  cited      the past cases (rag/memory.py) its proposal cites.
  fix_steps  what ran (an edit, if the reviewer made one), else what the
             agent proposed: what rag/priors.py credits the result to.

A rollback incident is not a case: the agent proposed nothing in it. It
shows as its fix's `rolled_back`.

The agent is judged by its own last proposal; an edited fix's result is
the reviewer's as much as the agent's, so `origin` tells them apart.
"""

import functools
import statistics

import config
from flows import packs
from flows.builder.lib import ProcessGroup
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
# A trusted fix that approved itself (agent/autonomy.py): no person decided, so it is not in the approval rate.
AUTO = "auto-approved"
REVIEWS = (APPROVED, EDITED, REJECTED, AUTO, UNDECIDED, NO_REVIEW)
AUTO_REVIEWER = "autonomy (trusted fix)"
EXPECTED, ACCEPTABLE, SHORT, OTHER, UNKNOWN = "expected", "acceptable", "value short", "other", "no label"
MATCHES = (EXPECTED, ACCEPTABLE, SHORT, OTHER, UNKNOWN)
# The right value (the fix can work), and the right lever whatever its value.
RIGHT_VALUE = (EXPECTED, ACCEPTABLE)
RIGHT_LEVER = (EXPECTED, ACCEPTABLE, SHORT)

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


# The deployed setting a valued step changes; the label's value says which way is further.
_DEPLOYED = {"SET_CONCURRENT_TASKS": "concurrentlySchedulableTaskCount", "SET_RUN_SCHEDULE": "schedulingPeriod"}
# The usual way, when the deployed value isn't known: more tasks, a shorter schedule.
_USUAL = {"SET_CONCURRENT_TASKS": 1, "SET_RUN_SCHEDULE": -1, "SCALE_DEPLOYMENT": 1}


@functools.lru_cache(maxsize=None)
def _processors(scenario_id: str) -> dict:
    """The scenario's processors as provisioning builds them, by name."""

    scenario = packs.SCENARIOS.get(scenario_id)

    if scenario is None:
        return {}

    group = ProcessGroup(scenario.id, scenario.group_name)
    scenario.build(group)

    return {p["name"]: p for p in group.processors.values()}


def _size(step: dict, now: tuple) -> tuple:
    rank = {s: i for i, s in enumerate(catalog.CLUSTER_SIZES)}

    return rank.get(step.get("cluster_size"), now[0]), catalog.magnitude(step.get("nodes")) or now[1]


def _pack_size(scenario) -> tuple:
    pack = next((p for p in packs.PACKS.values() if scenario in p.scenarios), None)

    return _size({"cluster_size": pack.cluster_size, "nodes": pack.nodes}, (0, 1)) if pack else (0, 1)


def direction(ref: dict, scenario=None) -> int:
    """
    Which way the label's value moves from what is deployed: +1 more tasks,
    a longer schedule or a larger deployment; -1 the other way. S08 slows a
    runaway source, S21 shrinks an oversized deployment.
    """

    ref = catalog.normalize(ref)
    kind = ref["action"]

    if kind == "SCALE_DEPLOYMENT":
        now, wanted = _pack_size(scenario), _size(ref, _pack_size(scenario))

        return -1 if wanted[0] < now[0] or wanted[1] < now[1] else 1

    processor = _processors(getattr(scenario, "id", "")).get(ref.get("target"), {})
    now, wanted = catalog.magnitude(processor.get(_DEPLOYED.get(kind))), catalog.magnitude(ref.get("value"))

    if now is None or wanted is None or now == wanted:
        return _USUAL.get(kind, 1)

    return 1 if wanted > now else -1


def reaches(step: dict, ref: dict, scenario=None) -> bool:
    """
    Whether a step is the reference's fix at a value that goes at least as
    far from what is deployed (direction): as many tasks or more, a run
    schedule as short or shorter, a resize as far or further. A route to a
    DLQ must reach the reference's DLQ. Other actions are judged by their
    identity alone.
    """

    if step_key(step) != step_key(ref):
        return False

    step, ref = catalog.normalize(step), catalog.normalize(ref)
    kind = ref["action"]
    way = direction(ref, scenario) if kind in _USUAL else 0

    if kind == "SCALE_DEPLOYMENT":
        now = _pack_size(scenario)

        return all((m - w) * way >= 0 for m, w in zip(_size(step, now), _size(ref, now)))

    if kind in _DEPLOYED:
        mine, wanted = catalog.magnitude(step.get("value")), catalog.magnitude(ref.get("value"))

        return wanted is None or (mine is not None and (mine - wanted) * way >= 0)

    if kind == "ROUTE_RELATIONSHIP_TO_DLQ":
        # The right relationships to another stage's DLQ save the data where nobody looks for it.
        return step.get("dlq") == ref.get("dlq")

    return True


def match(steps: list, scenario) -> str:
    """How the steps compare with the scenario's design label."""

    if scenario is None or not scenario.expected:
        return UNKNOWN

    mine = catalog.merge_routes([catalog.normalize(s) for s in steps if isinstance(s, dict)])
    mine = mine or [{"action": "NO_ACTION"}]
    keys = {step_key(s) for s in mine}

    def reached(fixes) -> bool:
        return bool(fixes) and all(any(reaches(s, f, scenario) for s in mine) for f in fixes)

    def picked(fixes) -> bool:
        return bool(fixes) and {step_key(f) for f in fixes} <= keys

    if reached(scenario.expected):
        return EXPECTED

    if any(reached((f,)) for f in scenario.acceptable):
        return ACCEPTABLE

    if picked(scenario.expected) or any(picked((f,)) for f in scenario.acceptable):
        return SHORT

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


def _measured(found: list) -> dict | None:
    """Of an action's outcomes, the window the verdict settles on (poller.settled), else the first so far."""

    by = {o["window_seconds"]: o for o in found}
    first, *later = config.VERIFY_WINDOWS
    row = by.get(first)

    if row and row["verdict"] not in (outcomes.IMPROVED, outcomes.WORSENED) and later and later[0] in by:
        row = by[later[0]]

    return row


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
        found = [o for o in by["outcomes"].get(iid, []) if last and o["action_id"] == last["id"]]
        result, source = _result(last, found, labels.get((last or {}).get("id")))
        measured = _measured(found) if result else None
        moved = outcomes.outcome_lift(measured.get("details")) if measured else None

        if decided and decided[-1]["decision"] == "reject":
            review = REJECTED

        elif decided and decided[-1].get("reviewer") == AUTO_REVIEWER:
            review = AUTO

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
            "fix_steps": [s for s in chosen.get("action") or [] if isinstance(s, dict)] if applied else steps,
            "review": review, "auto": review == AUTO,
            "origin": "reviewer's edit" if edited else "agent as proposed",
            "dry_run": bool(last and last["dry_run"] and last["status"] == "DRY_RUN"),
            "applied": bool(applied), "apply_failed": bool(last and last["status"] == "FAILED"),
            "result": result, "result_from": source,
            "lift": moved["lift"] if moved else None,
            "lift_kpi": dict(moved, window=measured["window_seconds"]) if moved else None,
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
    lifts = sorted(c["lift"] for c in judged if c.get("lift") is not None)
    lifted_on = {}

    for c in judged:
        if c.get("lift") is not None:
            lifted_on[c["lift_kpi"]["kpi"]] = lifted_on.get(c["lift_kpi"]["kpi"], 0) + 1

    return {
        "incidents": len(group),
        "controls": sum(c["control"] for c in group),
        "proposed": len(proposed),
        "first_valid": sum(bool(c["valid_first"]) for c in proposed),
        "valid_first": _rate(sum(bool(c["valid_first"]) for c in proposed), len(proposed)),
        "repairs": sum(c["repairs"] for c in proposed),
        "cited": _rate(sum(bool(c.get("cited")) for c in proposed), len(proposed)),
        "match": {m: sum(c["match"] == m for c in labelled) for m in MATCHES[:-1]},
        "matched": _rate(sum(c["match"] in RIGHT_VALUE for c in labelled), len(labelled)),
        "right_lever": _rate(sum(c["match"] in RIGHT_LEVER for c in labelled), len(labelled)),
        "review": {r: sum(c["review"] == r for c in group) for r in REVIEWS},
        "approved": _rate(sum(c["review"] == APPROVED for c in decided), len(decided)),
        "applied": sum(c["applied"] for c in group),
        "dry_runs": sum(c["dry_run"] for c in group),
        "apply_failed": sum(c["apply_failed"] for c in group),
        "results": {r: sum(c["result"] == r for c in group) for r in RESULTS},
        "labelled": sum(c["result_from"] == "reviewer" for c in group),
        "worked": _rate(sum(c["result"] == WORKED for c in judged), len(judged)),
        "judged": len(judged),
        "lift": statistics.median(lifts) if lifts else None,
        "lift_range": tuple(statistics.quantiles(lifts, n=4, method="inclusive")[::2]) if len(lifts) > 1 else None,
        "lifted": len(lifts),
        "lifted_on": lifted_on,
        "first_at": min((c["opened_at"] for c in group), default=None),
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
