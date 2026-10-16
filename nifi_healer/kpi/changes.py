"""
What changed on a deployment, and who changed it.

CDF's event log (list-deployment-events) is the record of every change,
whoever made it. One change shows up as several events: "Updating
Parameters", "Parameters Updated", "Deployed Flow Update Successful", ...
Only the event that marks a change as done is kept:

    Deployment Successful            created
    Change Flow Version Completed    flow_version
    Parameters Updated               parameters
    Alert Rules Updated              kpis (a marker; no new stretch)
    ... a size or node count         sizing
    any other "... Updated/Completed" other

An event says what changed, not the values or (usefully) who: the agent's
own actions say that. A change within an applied action's time window,
of the action's family, is the agent's; one within a chaos injection's
(chaos.py), of a kind it changed, is the injector's; any other is an
operator's.
"""

DONE = {
    "Deployment Successful": "created",
    "Change Flow Version Completed": "flow_version",
    "Parameters Updated": "parameters",
    "Alert Rules Updated": "kpis",
}

# The steps of a change, and the platform's own bookkeeping.
NOISE = {
    "Deployed Flow Update Initiated", "Deployed Flow Update Successful", "Updating Parameters",
    "Updating Alert Rules", "Change Flow Version Initiated", "KPI Alert Rules Activated",
    "Default Alert Rules Activated", "NiFi Flow Started", "NiFi Flow Imported", "Importing NiFi Flow",
    "Starting NiFi Flow", "New Deployed Flow Initiated", "New Deployed Flow Successful",
    "NiFi Cluster Provisioned", "Provisioning NiFi Cluster", "Deployment Initiated",
}
_NOISE_PREFIXES = ("updating", "activating", "preparing", "provisioning", "importing", "starting")

_SIZING_WORDS = ("scal", "resiz", "node count", "nodes", "cluster size")

FAMILY_KINDS = {"FLOW_EDIT": "flow_version", "PARAMETER": "parameters", "SIZING": "sizing"}

# A chaos injection's step kinds (provisioning.run_steps) as changes; an
# import alone changes nothing deployed.
STEP_KINDS = {"version": "flow_version", "parameters": "parameters", "sizing": "sizing"}

# How far around an action an event of its family is counted as the
# action's: before its start (clock skew) and after its finish (a change
# is reported done once the platform settles).
MATCH_BEFORE, MATCH_AFTER = 120, 900

# A change that starts a new stretch of the timeline; a KPI change only marks it.
SEGMENTING = ("created", "flow_version", "parameters", "sizing", "other")


def classify(event: dict) -> str | None:
    """The change an event marks as done, or None for a step, an alert or noise."""

    if (event.get("eventType") or "STATUS_UPDATE") != "STATUS_UPDATE":
        return None

    name = (event.get("name") or "").strip()

    if name in DONE:
        return DONE[name]

    low = name.lower()

    if not name or name in NOISE or low.startswith(_NOISE_PREFIXES) or "initiated" in low:
        return None

    if any(word in low for word in _SIZING_WORDS):
        return "sizing"

    if low.endswith(("updated", "completed", "successful", "changed")):
        return "other"

    return None


def from_events(events: list) -> list:
    """list-deployment-events summaries -> [{event_crn, t, kind, name}], the changes only."""

    out = []

    for event in events:
        kind = classify(event)

        if not kind or not event.get("crn") or event.get("firstOccurrence") is None:
            continue

        out.append({"event_crn": event["crn"], "t": float(event["firstOccurrence"]) / 1000.0,
                    "kind": kind, "name": event.get("name")})

    return out


def attribute(changes: list, actions: list, injections: list = ()) -> list:
    """
    Each change with its source: [{t, kind, name, source: agent | chaos |
    operator | platform, action, injection}], oldest first. An applied
    action or a planted injection with no event to match (an event log
    that has not caught up, or a change kind CDF names differently) still
    appears, at its finish. `injections` are chaos_injections rows.
    """

    out = [dict(c, source="platform" if c["kind"] in ("created", "kpis") else "operator",
                action=None, injection=None) for c in sorted(changes, key=lambda c: c["t"])]
    applied = [a for a in actions if a.get("status") == "APPLIED" and not a.get("dry_run")
               and a.get("started_at")]

    for action in sorted(applied, key=lambda a: a["started_at"]):
        kind = FAMILY_KINDS.get(action.get("family") or "")
        finished = action.get("finished_at") or action["started_at"]
        candidates = [c for c in out if c["action"] is None and c["kind"] == kind
                      and action["started_at"] - MATCH_BEFORE <= c["t"] <= finished + MATCH_AFTER]

        if candidates:
            match = min(candidates, key=lambda c: abs(c["t"] - finished))
            match.update(source="agent", action=action)

        else:
            out.append({"event_crn": None, "t": finished, "kind": kind or "other",
                        "name": action.get("action_type"), "source": "agent", "action": action,
                        "injection": None})

    # Every step an injection performed, though a later one failed.
    for row in sorted(injections, key=lambda r: r["at"]):
        finished = row.get("finished_at") or row["at"]
        kinds = list(dict.fromkeys(STEP_KINDS[s.get("kind")] for s in row.get("steps") or []
                                   if s.get("performed") and s.get("kind") in STEP_KINDS))

        for kind in kinds:
            candidates = [c for c in out if c["action"] is None and c["injection"] is None
                          and c["kind"] == kind and row["at"] - MATCH_BEFORE <= c["t"] <= finished + MATCH_AFTER]

            if candidates:
                min(candidates, key=lambda c: abs(c["t"] - finished)).update(source="chaos", injection=row)

            else:
                out.append({"event_crn": None, "t": finished, "kind": kind, "name": f"chaos {row['scenario']}",
                            "source": "chaos", "action": None, "injection": row})

    return sorted(out, key=lambda c: c["t"])
