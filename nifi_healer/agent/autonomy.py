"""
Graduated autonomy: a fix that has proven itself on a process group
approves itself, after a veto window in which any person can approve,
reject or hold it.

    consider(incident_id)   # a run paused at review: open its window if the fix is trusted
    tick(client, store)     # the poller, every POLL_KPI_SECONDS: approve or hand back what is due
    blockers(...)           # why a review waits for a person; [] means it may approve itself

A fix is priors.group_fix(steps): its steps without their values, on one
process group. It is trusted when every try of it there, back to its last
miss, worked, and at least AUTONOMY_AFTER of those wins were approved by
a person: a win it approved itself keeps the trust but never earns it.
A rejection, a fix that didn't help or made things worse, and a WRONG_FIX
label are misses, so any of them takes the trust away again.

Only low-risk flow edits (ELIGIBLE) qualify, at values a person approved
within that run of wins (a number may fall between the lowest and the
highest of them), with no warning on the review and writes armed.

Approving goes through the same path as a reviewer's click (graph.resume:
human_review, then the executor and its guardrails), with REVIEWER as
the reviewer. When the window ends, the rule is checked again, with the
rate limit and drift; a review that no longer qualifies is handed back to
a person with the reason, and stays open.

Windows are rows in the store (auto_approvals), in the incident clock, so
a restart keeps them and the mock's fast-forward moves them.
"""

import config
from nifi_healer.actions import catalog, executor, rollback
from nifi_healer.agent import graph
from nifi_healer.agent.nodes import log
from nifi_healer.cdp.client import get_client
from nifi_healer.eval import scorecard as sc
from nifi_healer.rag import memory, priors
from nifi_healer.sim.model import parse_duration, parse_size
from nifi_healer.store.db import get_store

REVIEWER = sc.AUTO_REVIEWER

# The catalog's low-risk flow edits: concurrency, run schedule, back
# pressure, penalty/yield, retry policy. Not a setting.
ELIGIBLE = frozenset(name for name, a in catalog.CATALOG.items()
                     if a.family == catalog.FLOW_EDIT and a.risk == "low")

WAITING, APPROVED, HELD, CANCELLED, HANDED_BACK = "WAITING", "APPROVED", "HELD", "CANCELLED", "HANDED_BACK"

_PAUSED = "autonomy_paused"
_IDENTITY = ("action", "target", "property", "parameter", "relationship")


# ---------------------------------------------------------------------
# The trust rule
# ---------------------------------------------------------------------


def standing(store, pack: str, group: str, fix: str, found: list | None = None) -> dict:
    """
    {trusted, wins, human_wins, needed, broken_by, values} of one fix on one
    process group: its run of wins back to its last miss (broken_by, or ""),
    and the values a person approved within it, {step: {field: [values]}}.
    """

    found = priors.tries(store.record()) if found is None else found
    mine = [t for t in found if (t["pack"], t["process_group"], t["group_fix"]) == (pack, group, fix)
            and t["outcome"] != sc.PENDING]
    wins, human_wins, broken_by, values = 0, 0, "", {}

    for one in reversed(mine):
        if one["outcome"] != sc.WORKED:
            broken_by = one["outcome"]
            break

        wins += 1

        if not one["auto"]:
            human_wins += 1

            for step in one["steps"]:
                fields = values.setdefault(memory.step_text(step, values=False), {})

                for name, value in step.items():
                    if name not in _IDENTITY and value not in fields.setdefault(name, []):
                        fields[name].append(value)

    return {"trusted": human_wins >= config.AUTONOMY_AFTER, "wins": wins, "human_wins": human_wins,
            "needed": config.AUTONOMY_AFTER, "broken_by": broken_by, "values": values}


def _magnitude(value):
    if isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        return float(value)

    if catalog.is_duration(value):
        return parse_duration(value)

    if catalog.is_size(value):
        return parse_size(value)

    return None


def outside(steps: list, values: dict) -> list:
    """The steps' values that no person approved within the run of wins, and that no range of them covers."""

    out = []

    for step in steps:
        key = memory.step_text(step, values=False)
        approved = values.get(key, {})

        for name, value in step.items():
            if name in _IDENTITY:
                continue

            seen = approved.get(name) or []
            sizes = [_magnitude(v) for v in seen]

            if value in seen:
                continue

            if seen and _magnitude(value) is not None and None not in sizes \
                    and min(sizes) <= _magnitude(value) <= max(sizes):
                continue

            shown = ", ".join(str(v) for v in seen) or "none"
            out.append(f"{key} {name}={value} is not a value a person approved here (approved: {shown})")

    return out


def _eligible(steps: list) -> list:
    """The steps' actions that may not approve themselves."""

    return sorted({str(s.get("action")) for s in steps if s.get("action") not in ELIGIBLE})


def basis(stand: dict) -> str:
    """Why a fix is trusted, as the reviewer reads it."""

    return (f"it worked the last {stand['wins']} time(s) it was applied to this process group, "
            f"{stand['human_wins']} of them approved by a person")


def trusted_fixes(store) -> list:
    """Every low-risk fix trusted now: [{pack, process_group, fix, wins, human_wins}], for the System tab."""

    found = priors.tries(store.record())
    out = []

    for key in sorted({(t["pack"], t["process_group"], t["group_fix"]) for t in found
                       if t["outcome"] != sc.PENDING and not _eligible(t["steps"])}):
        stand = standing(store, *key, found=found)

        if stand["trusted"]:
            out.append({"pack": key[0], "process_group": key[1], "fix": key[2], "wins": stand["wins"],
                        "human_wins": stand["human_wins"]})

    return out


# ---------------------------------------------------------------------
# Switches
# ---------------------------------------------------------------------


def paused(store) -> dict | None:
    """The setting row while a person has paused autonomy ({updated_by, updated_at}), else None."""

    row = store.setting(_PAUSED)

    return row if row and row.get("value") == "true" else None


def pause(store, by: str, now: float) -> None:
    store.set_setting(_PAUSED, "true", by, now)


def unpause(store, by: str, now: float) -> None:
    store.set_setting(_PAUSED, "false", by, now)


# ---------------------------------------------------------------------
# Whether a review may approve itself
# ---------------------------------------------------------------------


def blockers(store, client, incident: dict, payload: dict, firing: bool = False,
             proposal_id=None) -> list:
    """
    Why the paused review must wait for a person; [] when it may approve
    itself. With firing (its window has ended), also: the proposal is the
    one the window opened on (proposal_id), it is not stale, and the
    deployment's rate limit allows a change now.
    """

    out = []
    plan = payload.get("plan") or {}
    steps = [s for s in plan.get("steps") or [] if isinstance(s, dict)]

    if not config.AUTONOMY:
        out.append("autonomy is off (SELFHEAL_AUTONOMY)")

    elif paused(store):
        out.append(f"autonomy is paused (by {paused(store).get('updated_by') or 'someone'})")

    if not config.ARMED:
        out.append("writes are disarmed, so an approval is only a dry run")

    if rollback.is_rollback(incident):
        out.append("a rollback always waits for a person")

    row = next((p for p in store.proposals(incident["id"]) if p["id"] == payload.get("proposal_id")), None)

    if row and (row.get("agent_version") or "").endswith("+review"):
        out.append("these steps are a person's edit")

    if not plan.get("ok"):
        out.append("the proposal failed validation")

    elif _eligible(steps):
        out.append(f"only low-risk flow edits approve themselves, not {', '.join(_eligible(steps))}")

    elif catalog.risk_of(steps) != "low":
        out.append(f"the fix is {catalog.risk_of(steps)} risk")

    if plan.get("warnings"):
        out.append(f"the review has a warning: {plan['warnings'][0]}")

    if steps and not _eligible(steps):
        found = priors.tries(store.record(), pending=True)
        fix = priors.group_fix(steps)
        stand = standing(store, incident["pack"], incident["process_group"], fix, found=found)

        if not stand["trusted"]:
            line = (f"this fix has worked {stand['wins']} time(s) in a row on this process group, "
                    f"{stand['human_wins']} of them approved by a person; at {stand['needed']} it approves itself")
            out.append(line + (f" (its run started again after a try that {stand['broken_by']})"
                               if stand["broken_by"] else ""))

        else:
            out.extend(outside(steps, stand["values"]))

        running = [t["incident"] for t in found if t["outcome"] == sc.PENDING and t["auto"]
                   and (t["pack"], t["process_group"], t["group_fix"]) == (incident["pack"],
                                                                           incident["process_group"], fix)]

        if running:
            out.append(f"its last self-approved run ({running[-1]}) is still being measured")

    if firing:
        if proposal_id is not None and payload.get("proposal_id") != proposal_id:
            out.append("the proposal changed after its window opened")

        deployed = store.deployment(incident["deployment_crn"]) or {}

        if executor.stale(store, incident, deployed.get("flow_version_crn") or ""):
            out.append("the deployment now runs a flow version this app did not deploy")

        refusal = executor.precheck(client, store, incident) if config.ARMED else ""

        if refusal:
            out.append(refusal.rstrip("."))

    return out


def progress(store, client, incident: dict, payload: dict) -> str:
    """The Review Queue's line on autonomy for a review with no window: why it waits for a person."""

    if not config.AUTONOMY or not payload:
        return ""

    found = blockers(store, client, incident, payload)

    return f"**Autonomy:** waits for a person: {found[0]}." if found else ""


# ---------------------------------------------------------------------
# Veto windows
# ---------------------------------------------------------------------


def consider(incident_id: str) -> bool:
    """
    A run paused at review: open its veto window when its fix is trusted.
    One window per incident, ever; True when this call opened it.
    """

    store, client = get_store(), get_client()

    if not config.AUTONOMY or paused(store) or store.auto_approval(incident_id):
        return False

    payload = graph.pending(incident_id)
    incident = store.incident(incident_id)

    if not payload or not incident or blockers(store, client, incident, payload):
        return False

    steps = payload["plan"]["steps"]
    stand = standing(store, incident["pack"], incident["process_group"], priors.group_fix(steps))
    now = client.now()
    due = now + config.AUTONOMY_VETO_SECONDS
    opened = store.add_auto_approval(incident_id, payload.get("proposal_id"), priors.group_fix(steps),
                                     basis(stand), due, now)

    if opened:
        log(f"{incident_id}: trusted fix ({basis(stand)}); approves itself in "
            f"{config.AUTONOMY_VETO_SECONDS // 60} min unless a person decides")

    return opened


def tick(client, store) -> None:
    """Every window that has ended: approve it, or hand it back to a person with the reason."""

    now = client.now()

    for row in store.auto_approvals((WAITING,)):
        if row["due_at"] > now:
            break

        incident_id = row["incident_id"]

        if graph.run_state(incident_id):
            continue      # being diagnosed again or applied: look once it is done

        payload = graph.pending(incident_id)
        incident = store.incident(incident_id)

        if not payload or not incident:
            store.claim_auto_approval(incident_id, CANCELLED, now, reason="no longer waiting for a review",
                                      decided_by=REVIEWER)
            continue

        found = blockers(store, client, incident, payload, firing=True, proposal_id=row["proposal_id"])

        if found:
            if store.claim_auto_approval(incident_id, HANDED_BACK, now, reason=found[0], decided_by=REVIEWER):
                log(f"{incident_id}: handed back to a person: {found[0]}")

            continue

        if store.claim_auto_approval(incident_id, APPROVED, now, reason=row["basis"] or "", decided_by=REVIEWER):
            log(f"{incident_id}: trusted fix approved itself ({row['fix']})")
            graph.resume_in_background(incident_id, {"decision": "approve", "reviewer": REVIEWER,
                                                     "reason": f"trusted fix: {row['basis'] or row['fix']}"})


def hold(store, incident_id: str, by: str, now: float) -> bool:
    """A person keeps the review for themselves; False when its window was not open."""

    return store.claim_auto_approval(incident_id, HELD, now, reason="held for review", decided_by=by)


def cancel(store, incident_id: str, by: str, why: str, now: float) -> bool:
    """A person decided before the window ended; False when it was not open."""

    return store.claim_auto_approval(incident_id, CANCELLED, now, reason=why, decided_by=by)
