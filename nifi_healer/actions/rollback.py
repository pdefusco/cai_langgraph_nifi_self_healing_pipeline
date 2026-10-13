"""
Rollback proposals: undo one applied action, reviewed like any fix.

    why = rollback.blocked(store, action)          # "" when one may be proposed now
    incident = rollback.open_for(store, action, reason, now)

A rollback is an incident of kind "rollback" whose one proposal is
{"action": "ROLLBACK", "action_id": N}, made without the LLM
(agent/nodes.propose_rollback). The executor plans it from the action it
undoes, by that action's family:

    FLOW_EDIT   change-flow-version-in-deployment back to the version the
                action replaced (already in the catalog, so nothing is
                imported), only while the deployment still runs the
                version the action deployed
    PARAMETER   update-flow-in-deployment with each value's "before", only
                while it is still the action's "after"
    SIZING      update-deployment back to the old size, likewise

Every guard of an ordinary fix applies, the cooldown included, and a
human approves it. The poller proposes one when a fix's deciding window
measured WORSENED or a later window flags a regression; a reviewer can
propose one from History for any applied fix.
"""

from nifi_healer.actions import catalog
from nifi_healer.store.db import OPEN_STATUSES

KIND = "rollback"
FAMILIES = (catalog.FLOW_EDIT, catalog.PARAMETER, catalog.SIZING)


def is_rollback(incident: dict | None) -> bool:
    return bool(incident) and incident.get("kind") == KIND


def proposal_of(store, action: dict) -> dict:
    """The proposal the action ran (its steps and validated diff), or {}."""

    return next((p for p in store.proposals(action["incident_id"]) if p["id"] == action.get("proposal_id")), {})


def unfit(store, action: dict | None) -> str:
    """Why this action can never be rolled back, or ""."""

    if not action:
        return "There is no such action."

    if action.get("status") != "APPLIED" or action.get("dry_run"):
        return f"Action {action['id']} was not applied to the tenant ({action.get('status')}), so there is nothing to undo."

    if is_rollback(store.incident(action["incident_id"])):
        return f"Action {action['id']} is itself a rollback; propose a fix instead."

    if action.get("family") not in FAMILIES:
        return f"Action {action['id']} ({action.get('action_type')}) changed nothing that can be set back."

    if action["family"] == catalog.FLOW_EDIT and not (action.get("from_flow_version_crn")
                                                      and action.get("to_flow_version_crn")):
        return f"Action {action['id']} recorded no flow versions to go back between."

    return ""


def blocked(store, action: dict | None, manual: bool = False) -> str:
    """
    Why a rollback of this action cannot be proposed now, or "". Besides
    unfit(): one is open or already applied, its process group has an open
    incident, or (a flow edit) the deployment has moved on to another
    version, which going back would undo too. The poller also stops at a
    rollback proposed before, so a rejection stands; a reviewer may ask
    again.
    """

    why = unfit(store, action)

    if why:
        return why

    for earlier in store.rollbacks_of(action["id"]):
        if earlier["status"] in OPEN_STATUSES:
            return f"{earlier['id']} already proposes rolling back action {action['id']}."

        if any(a["status"] == "APPLIED" for a in store.actions(incident_id=earlier["id"])):
            return f"{earlier['id']} already rolled back action {action['id']}."

        if not manual:
            return f"A rollback of action {action['id']} was proposed before ({earlier['id']}, {earlier['status']})."

    incident = store.incident(action["incident_id"]) or {}
    busy = store.open_incident(action["deployment_crn"], incident.get("process_group") or "")

    if busy:
        return f"{busy['id']} is still {busy['status']} on {busy['process_group']}; it goes first."

    if action["family"] == catalog.FLOW_EDIT:
        deployment = store.deployment(action["deployment_crn"]) or {}
        current = deployment.get("flow_version_crn")

        if current and current != action["to_flow_version_crn"]:
            return (f"The deployment now runs v{deployment.get('flow_version')}, not the version action "
                    f"{action['id']} deployed; going back would undo that later change too.")

    return ""


def open_for(store, action: dict, reason: str, now: float) -> dict:
    """A new rollback incident for the action, on the group and signals of the incident it fixed."""

    fixed = store.incident(action["incident_id"])
    deployment = store.deployment(action["deployment_crn"]) or {}
    expected = action.get("to_flow_version_crn") or deployment.get("flow_version_crn") \
        or fixed.get("expected_flow_version_crn") or ""

    return store.create_incident(
        pack=fixed["pack"], deployment_crn=fixed["deployment_crn"], deployed_flow_crn=fixed["deployed_flow_crn"],
        scenario=fixed["scenario"], process_group=fixed["process_group"], signals=fixed.get("signals") or [],
        summary=f"Roll back action {action['id']} ({action['action_type']}) of {fixed['id']}: {reason}",
        expected_flow_version_crn=expected, now=now, kind=KIND, rollback_of_action_id=action["id"])
