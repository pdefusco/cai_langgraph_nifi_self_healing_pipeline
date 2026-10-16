"""
Fault episodes: what became of each fault the chaos injector (chaos.py)
planted, read from the store. Computed on every read, never stored.

An episode starts at a PLANTED injection and follows its scenario's
process group on that deployment:

  detected   the first incident opened on the group after the plant
             (a rollback proposal is not one)
  proposed   the agent's first proposal on its incidents
  decided    the first approval or rejection: by a person, or by autonomy
             (agent/autonomy.py); review_wait is how long it waited
  fixes      the fixes applied (not dry runs), over every incident until
             it cleared: a compound fault can take more than one
  cleared    the start of the first stretch, after the last fix settled,
             in which every alerting KPI of the scenario stayed healthy
             for its clear_within (as the stored samples have them)
  status     cleared; missed (no incident within twice its
             detect_within); gave up (neither within a day, or twice
             detect_within if longer); else open

An episode ends (ended_at) once it has cleared for clear_within, or at
its missed or gave-up deadline; chaos rests the scenario from then.

A false alarm is an incident on a control's group, or on a planted group
while no episode of it is open. Only what happened since chaos first
ticked on (chaos.since) counts.
"""

import statistics

import config
from flows import packs
from nifi_healer import chaos
from nifi_healer.eval import scorecard as sc

CLEARED, OPEN, MISSED, GAVE_UP = "cleared", "open", "missed", "gave up"
STATUSES = (CLEARED, OPEN, MISSED, GAVE_UP)

GIVE_UP_AFTER = 86400


def _healthy(kpi, value: float) -> bool:
    return not ((kpi.above is not None and value > kpi.above) or (kpi.below is not None and value < kpi.below))


def cleared_at(store, deployment_crn: str, scenario, start: float) -> float | None:
    """
    When, from `start`, the scenario's alerting KPIs began their first
    stretch of all being healthy that lasted clear_within (each KPI as
    its latest sample has it); None if none has yet.
    """

    kpis = {k.key: k for k in scenario.kpis if k.alert}
    points = sorted((t, key, v) for key in kpis for t, v in store.samples(deployment_crn, key, start)
                    if v is not None)
    latest, run, i = {}, None, 0

    while i < len(points):
        t = points[i][0]

        while i < len(points) and points[i][0] == t:
            _, key, value = points[i]
            latest[key] = _healthy(kpis[key], value)
            i += 1

        if not all(latest.values()):
            run = None
            continue

        run = t if run is None else run

        if t - run >= scenario.clear_within:
            return run

    return None


def _by_incident(rows: list) -> dict:
    out = {}

    for row in rows or []:
        out.setdefault(row["incident_id"], []).append(row)

    return out


def episode(store, row: dict, record: dict, cases: dict, now: float, until: float | None = None) -> dict:
    """
    One injection's episode. `record` is store.record(since), `cases`
    scorecard.cases of it by incident; `until` is when the same scenario
    was planted next, if it was.
    """

    scenario = packs.SCENARIOS[row["scenario"]]
    crn, group, at = row["deployment_crn"], scenario.group_name, row["at"]
    missed_at = at + 2 * scenario.detect_within
    give_up_at = at + max(GIVE_UP_AFTER, 2 * scenario.detect_within)
    end = until if until is not None else float("inf")
    found = sorted((i for i in record["incidents"] if i["deployment_crn"] == crn and i["process_group"] == group
                    and i.get("kind") != "rollback" and at <= i["opened_at"] < end),
                   key=lambda i: i["opened_at"])

    # Detected late is missed: what opened after the deadline was not this plant's.
    if found and found[0]["opened_at"] >= missed_at:
        found = []

    actions = _by_incident(record.get("actions"))
    taken, fixes, cleared = [], [], None

    for incident in found:
        if cleared is not None and incident["opened_at"] >= cleared:
            break

        taken.append(incident)
        applied = [a for a in actions.get(incident["id"], []) if a["status"] == "APPLIED" and not a["dry_run"]
                   and a.get("finished_at")]

        if applied:
            fixes += applied
            last = max(a["finished_at"] for a in fixes)
            cleared = cleared_at(store, crn, scenario, last + config.VERIFY_SETTLE_SECONDS)

    ids = {i["id"] for i in taken}
    proposals = sorted((p for p in sc._agents([p for p in record.get("proposals") or []
                                               if p["incident_id"] in ids])), key=lambda p: p["created_at"])
    decisions = sorted((r for r in record.get("reviews") or [] if r["incident_id"] in ids
                        and r["decision"] in ("approve", "reject")), key=lambda r: r["created_at"])
    mine = [cases[i] for i in ids if i in cases]
    detected = taken[0]["opened_at"] if taken else None
    proposed = proposals[0]["created_at"] if proposals else None
    decided = decisions[0] if decisions else None

    if cleared is not None:
        status, ended = CLEARED, cleared + scenario.clear_within

    elif not taken and now >= missed_at:
        status, ended = MISSED, missed_at

    elif now >= give_up_at:
        status, ended = GAVE_UP, give_up_at

    else:
        status, ended = OPEN, None

    return {
        "injection": row["id"], "pack": row["pack"], "scenario": scenario.id, "group": group,
        "kind": scenario.kind, "family": scenario.family, "deployment_crn": crn, "at": at,
        "incidents": [i["id"] for i in taken],
        "detected_at": detected, "mttd": detected - at if detected is not None else None,
        "proposed_at": proposed,
        "decided_at": decided["created_at"] if decided else None,
        "decision": decided["decision"] if decided else None,
        # Who decided, as a role: never a name.
        "decided_by": None if not decided else "autonomy" if decided.get("reviewer") == sc.AUTO_REVIEWER
        else "person",
        "review_wait": decided["created_at"] - proposed if decided and proposed is not None else None,
        "fixes": len(fixes),
        "match": next((c["match"] for c in sorted(mine, key=lambda c: c["opened_at"]) if c["proposed"]),
                      sc.UNKNOWN),
        "regressed": any(c["regressed"] for c in mine),
        "rolled_back": any(c["rolled_back"] for c in mine),
        "cleared_at": cleared, "time_to_clear": cleared - at if cleared is not None else None,
        "status": status, "ended_at": ended,
    }


def episodes(store, now: float, since: float | None = None) -> list:
    """Every PLANTED injection's episode since the soak began, oldest first; [] before it has."""

    since = chaos.since(store) if since is None else since

    if since is None:
        return []

    rows = [r for r in store.injections(since=since, statuses=(chaos.PLANTED,)) if r["scenario"] in packs.SCENARIOS]

    if not rows:
        return []

    record = store.record(since)
    cases = {c["incident"]: c for c in sc.cases(record)}
    out = []

    for n, row in enumerate(rows):
        later = next((r["at"] for r in rows[n + 1:] if (r["deployment_crn"], r["scenario"])
                      == (row["deployment_crn"], row["scenario"])), None)
        out.append(episode(store, row, record, cases, now, later))

    return out


def false_alarms(store, found: list, now: float, since: float | None = None) -> list:
    """
    Incidents since the soak began that no fault explains: [{incident,
    pack, scenario, group, opened_at, why}], oldest first. `found` is
    episodes(); a group never planted is not judged.
    """

    since = chaos.since(store) if since is None else since

    if since is None:
        return []

    planted = {}

    for one in found:
        planted.setdefault((one["deployment_crn"], one["group"]), []).append(one)

    out = []

    for incident in sorted(store.record(since)["incidents"], key=lambda i: i["opened_at"]):
        if incident.get("kind") == "rollback":
            continue

        scenario = packs.SCENARIOS.get(incident.get("scenario") or "")
        mine = planted.get((incident["deployment_crn"], incident["process_group"]))
        t = incident["opened_at"]

        if scenario and scenario.family == "NONE":
            why = "a control has no fault"

        elif mine and not any(e["at"] <= t < (e["ended_at"] if e["ended_at"] is not None else now + 1)
                              for e in mine):
            why = "no fault was planted on its group then"

        else:
            continue

        out.append({"incident": incident["id"], "pack": incident.get("pack"),
                    "scenario": incident.get("scenario"), "group": incident["process_group"],
                    "opened_at": t, "why": why})

    return out


def _median(values: list):
    values = [v for v in values if v is not None]

    return statistics.median(values) if values else None


def _share(hits: int, total: int) -> float | None:
    return hits / total if total else None


def summary(found: list, alarms: list, since: float | None, now: float) -> dict:
    """
    The soak's numbers over episodes (any subset of them) and their false
    alarms. A rate leaves out what is still open: detected % counts the
    detected and the missed, cleared % every episode that has ended.
    """

    days = (now - since) / 86400 if since is not None else 0.0
    detected = [e for e in found if e["detected_at"] is not None]
    judged = [e for e in found if e["detected_at"] is not None or e["status"] == MISSED]
    ended = [e for e in found if e["status"] != OPEN]
    cleared = [e for e in found if e["status"] == CLEARED]
    matched = [e for e in detected if e["match"] != sc.UNKNOWN]
    decided = [e for e in found if e["decided_by"]]

    return {
        "days": days, "injections": len(found), "open": len(found) - len(ended),
        "detected": _share(len(detected), len(judged)),
        "mttd": _median([e["mttd"] for e in detected]),
        "design_fix": _share(sum(e["match"] == sc.EXPECTED for e in matched), len(matched)),
        "cleared": _share(len(cleared), len(ended)),
        "time_to_clear": _median([e["time_to_clear"] for e in cleared]),
        "fixes_to_clear": _median([e["fixes"] for e in cleared]),
        "review_wait": _median([e["review_wait"] for e in decided]),
        "auto": _share(sum(e["decided_by"] == "autonomy" for e in decided), len(decided)),
        "false_alarms": len(alarms),
        "false_alarms_per_day": len(alarms) / days if days > 0 else None,
        "regressions": sum(e["regressed"] for e in found),
        "rollbacks": sum(e["rolled_back"] for e in found),
    }
