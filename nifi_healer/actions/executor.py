"""
A proposal turned into exact cdp commands, and (once approved) run.

    report = plan(client, incident, steps)     # read-only
    report["ok"], report["problems"], report["diff"], report["commands"]

    record = execute(client, store, incident, proposal)

`plan` is what review shows: the flow diff, the commands, the risk. It
reads the tenant but never writes. `execute` plans again (the tenant may
have moved since review), re-checks every guard, then writes, waits for
the deployment to settle and records one row in `actions`.

    FLOW_EDIT   the incident's flow version -> edits.apply -> validate
                -> df import-flow-definition-version --file <edited>
                -> df change-flow-version-in-deployment
    PARAMETER   dfworkload update-flow-in-deployment --parameter-groups
    SIZING      dfworkload update-deployment --cluster-size --static-node-count

Writes are dry runs unless SELFHEAL_ARMED is set. A dry run records the
argv it would have run and closes the incident as DRY_RUN.

Guards, in order, before any write: the rate limit for the deployment
(cooldown and actions per window), the workload endpoint is reachable,
no half-applied version change, the deployed version is still the one
the incident was diagnosed against, and (inside guardrails.act) the
deployment is the recorded nfh- deployment on the expected service.

One drift is allowed: a version this app deployed itself, for another
incident in the same deployment (S15's fix, then S16's). The flow edit
is planned again on that version and applied only if its diff is still
exactly the one the reviewer approved.

`precheck` checks the rate limit before a review is answered, so a
cooldown leaves the review open instead of failing it.
"""

import json
import os

import config
from nifi_healer.actions import catalog
from nifi_healer.cdp import guardrails
from nifi_healer.cdp.client import Client, command_line
from nifi_healer.flow import edits
from nifi_healer.flow.inventory import find_group, references
from nifi_healer.sim.world import groups_from_definition, parameters_of

NEW_VERSION = "<new flow version crn>"
EDITED_FILE = "<edited flow definition>"


def log(message: str) -> None:
    print(f"[executor] {message}", flush=True)


class Refused(Exception):
    """A guard said no; nothing was written."""


class Failed(Exception):
    """A write was attempted and did not land."""


def _import_args(flow_crn: str, path: str, comments: str) -> list:
    return ["import-flow-definition-version", "--flow-crn", flow_crn, "--file", path,
            "--comments", comments]


def _comments(incident: dict, steps: list) -> str:
    summary = "; ".join(f"{s['action']} {s.get('target') or ''}".strip() for s in steps)

    return f"nifi-healer {incident['id']}: {summary}"[:250]


# ---------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------


def plan(client: Client, incident: dict, steps: list) -> dict:
    steps = [catalog.normalize(s) for s in steps or []]
    report = {
        "ok": False,
        "problems": catalog.check_steps(steps),
        "family": catalog.family_of(steps),
        "steps": steps,
        "risk": catalog.risk_of(steps),
        "changes": [],
        "diff": [],
        "commands": [],
        "base_flow_version_crn": incident.get("expected_flow_version_crn") or "",
    }

    if report["problems"]:
        return report

    try:
        if report["family"] == catalog.FLOW_EDIT:
            _plan_flow_edit(client, incident, steps, report)

        elif report["family"] == catalog.PARAMETER:
            _plan_parameters(client, incident, steps, report)

        elif report["family"] == catalog.SIZING:
            _plan_sizing(client, incident, steps, report)

    except (Refused, guardrails.NotAllowed, edits.EditError) as exc:
        report["problems"].append(str(exc))

    report["ok"] = not report["problems"]

    return report


def public(report: dict) -> dict:
    """The plan without the edited definition, for the store and the UI."""

    return {k: v for k, v in report.items() if not k.startswith("_")}


def _deployed(client: Client, incident: dict) -> dict:
    facts = client.flow_facts(incident["deployment_crn"], incident["deployed_flow_crn"])

    if facts.get("error"):
        raise Refused(f"could not read the deployed flow: {facts['error']}")

    return facts


def rebased(client: Client, store, incident: dict) -> dict:
    """
    The incident to plan a flow edit with. If every version deployed since
    the one the incident was diagnosed on came from this app's own applied
    actions, the incident comes back with the current version as its base
    and `rebased_from` set; any other drift is left for plan() to refuse.
    """

    base = incident.get("expected_flow_version_crn") or ""

    if not base:
        return incident

    current = _deployed(client, incident)["flow_version_crn"]
    ours = {a["to_flow_version_crn"]: a for a in store.actions(deployment_crn=incident["deployment_crn"])
            if a["status"] == "APPLIED" and a.get("to_flow_version_crn")}
    crn, seen = current, set()

    while crn != base and crn in ours and crn not in seen:
        seen.add(crn)
        crn = ours[crn].get("from_flow_version_crn")

    if current == base or crn != base:
        return incident

    return dict(incident, expected_flow_version_crn=current, rebased_from=base,
                rebased_by=ours[current]["incident_id"])


def precheck(client: Client, store, incident: dict) -> str:
    """
    Why an armed approval would be refused for a reason that passes with
    time (the deployment's rate limit), or "". Reads only the store.
    """

    try:
        guardrails.assert_rate_allowed(store.performed_action_times(incident["deployment_crn"]), client.now())

    except guardrails.NotAllowed as exc:
        return str(exc)

    return ""


def _plan_flow_edit(client: Client, incident: dict, steps: list, report: dict) -> None:
    pack, deployment, flow = incident["pack"], incident["deployment_crn"], incident["deployed_flow_crn"]
    group = incident["process_group"]
    facts = _deployed(client, incident)
    base = report["base_flow_version_crn"] or facts["flow_version_crn"]
    report["base_flow_version_crn"] = base

    if facts["flow_version_crn"] != base:
        raise Refused(
            f"the deployment now runs v{facts['flow_version']} ({facts['flow_version_crn']}), not the "
            f"version this incident was diagnosed on ({base}); someone changed it, so diagnose again")

    flow_crn = guardrails.expected_for(pack).get("flow_crn")

    if not flow_crn:
        raise Refused(f"no flow CRN recorded for {pack}")

    definition = client.flow_definition(base)

    if "error" in definition:
        raise Refused(f"could not read flow version {base}: {definition['error']}")

    configuration = client.flow_configuration(deployment, flow)
    groups = (configuration.get("parameterGroups") if "error" not in configuration else None) \
        or groups_from_definition(definition)
    edited, changes = edits.apply(definition, group, steps)
    validation = edits.validate_edit(definition, edited, parameters_of(groups), group)

    report["changes"] = changes
    report["diff"] = validation["diff"]
    report["problems"].extend(validation["problems"])
    report["_definition"] = edited
    report["_flow_crn"] = flow_crn

    listen = bool(guardrails.expected_for(pack).get("has_listen_components"))
    report["commands"] = [
        command_line(client.argv("df", _import_args(flow_crn, EDITED_FILE, _comments(incident, steps)))),
        command_line(client.argv("df", guardrails.change_flow_version_args(deployment, flow, NEW_VERSION,
                                                                           listen))),
    ]


def _plan_parameters(client: Client, incident: dict, steps: list, report: dict) -> None:
    deployment, flow = incident["deployment_crn"], incident["deployed_flow_crn"]
    configuration = client.flow_configuration(deployment, flow)

    if "error" in configuration:
        raise Refused(f"could not read the flow configuration: {configuration['error']}")

    facts = _deployed(client, incident)
    definition = client.flow_definition(facts["flow_version_crn"])

    if "error" in definition:
        raise Refused(f"could not read the deployed flow version: {definition['error']}")

    used = references(find_group(definition, incident["process_group"]) or {})
    by_context = {g.get("name"): g for g in configuration.get("parameterGroups") or []}
    updates = {}

    for step in steps:
        context, name, value = step["target"], step["parameter"], step["value"]
        group = by_context.get(context)

        if group is None:
            raise Refused(f"no parameter context {context!r}; the deployment has "
                          f"{', '.join(sorted(by_context)) or 'none'}")

        parameter = next((p for p in group.get("parameters") or [] if p.get("name") == name), None)

        if parameter is None:
            raise Refused(f"{context} has no parameter {name!r}")

        if parameter.get("sensitive"):
            raise Refused(f"{name} is sensitive; sensitive parameters are changed by hand")

        if name not in used:
            raise Refused(f"{name} is not referenced in {incident['process_group']!r}; "
                          "a proposal may only change its own process group's parameters")

        if parameter.get("value") == value:
            raise Refused(f"{name} is already {value!r}")

        updates.setdefault(context, []).append({"name": name, "value": value})
        report["changes"].append(f"{context}.{name}: {parameter.get('value')!r} -> {value!r}")
        report["diff"].append({"path": f"parameters[{context}].{name}",
                               "before": parameter.get("value"), "after": value})

    args = [
        "update-flow-in-deployment",
        "--deployment-crn", deployment,
        "--deployed-flow-crn", flow,
        "--configuration-version", configuration.get("configurationVersion"),
        "--parameter-groups", json.dumps([{"name": c, "parameters": p} for c, p in updates.items()]),
    ]
    report["_writes"] = [("dfworkload", args)]
    report["commands"] = [command_line(client.argv("dfworkload", args))]


def _plan_sizing(client: Client, incident: dict, steps: list, report: dict) -> None:
    deployment = incident["deployment_crn"]
    sizing = client.deployment_configuration(deployment)

    if "error" in sizing:
        raise Refused(f"could not read the deployment sizing: {sizing['error']}")

    step = steps[-1]
    size, nodes = (sizing.get("clusterSize") or {}).get("name"), sizing.get("staticNodeCount")

    if (size, nodes) == (step["cluster_size"], step["nodes"]):
        raise Refused(f"the deployment is already {size} x{nodes}")

    args = [
        "update-deployment",
        "--deployment-crn", deployment,
        "--configuration-version", sizing.get("configurationVersion"),
        "--cluster-size", json.dumps({"name": step["cluster_size"]}),
        "--static-node-count", step["nodes"],
    ]
    report["changes"] = [f"sizing: {size} x{nodes} -> {step['cluster_size']} x{step['nodes']}"]
    report["diff"] = [{"path": "deployment.clusterSize", "before": size, "after": step["cluster_size"]},
                      {"path": "deployment.staticNodeCount", "before": nodes, "after": step["nodes"]}]
    report["_writes"] = [("dfworkload", args)]
    report["commands"] = [command_line(client.argv("dfworkload", args))]


# ---------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------


def execute(client: Client, store, incident: dict, proposal: dict, dry_run: bool | None = None,
            wait=None) -> dict:
    """
    Run an approved proposal. Returns the action row. `wait()` replaces
    the polling for the deployment to settle (the mock settles at once).
    """

    dry_run = (not config.ARMED) if dry_run is None else bool(dry_run)
    steps = [catalog.normalize(s) for s in proposal.get("action") or []]
    pack, deployment, flow = incident["pack"], incident["deployment_crn"], incident["deployed_flow_crn"]
    started = client.now()
    # Read before this action's own APPLYING row exists, or it would rate-limit itself.
    performed = store.performed_action_times(deployment)
    action_id = store.add_action(
        incident["id"], pack, deployment, "+".join(s["action"] for s in steps) or "NONE",
        dry_run=dry_run, status="APPLYING", proposal_id=proposal.get("id"),
        family=catalog.family_of(steps),
        from_flow_version_crn=incident.get("expected_flow_version_crn") or "", started_at=started)
    store.update_incident(incident["id"], started, status="APPLYING")
    audit = {"argv": [], "result": []}

    if wait is None:
        from nifi_healer import provisioning

        def wait():
            return provisioning._wait_for_flow(client, deployment, flow)

    try:
        to_version = _execute(client, store, incident, steps, proposal, dry_run, wait, audit, performed)

    except (Refused, guardrails.NotAllowed) as exc:
        return _finish(client, store, incident, action_id, audit, "REFUSED", "FAILED", error=str(exc))

    except Failed as exc:
        return _finish(client, store, incident, action_id, audit, "FAILED", "FAILED", error=str(exc))

    if dry_run:
        return _finish(client, store, incident, action_id, audit, "DRY_RUN", "DRY_RUN")

    return _finish(client, store, incident, action_id, audit, "APPLIED", "VERIFYING",
                   to_flow_version_crn=to_version, **audit.get("rebase", {}))


def _finish(client: Client, store, incident: dict, action_id: int, audit: dict, status: str,
            incident_status: str, error: str = "", **fields) -> dict:
    now = client.now()
    store.update_action(action_id, status=status, argv=audit["argv"], result=audit["result"],
                        error=error or None, finished_at=now, **fields)
    store.update_incident(incident["id"], now, status=incident_status)
    log(f"[{incident['pack']}] {incident['id']}: {status}" + (f": {error}" if error else ""))

    return store.action(action_id)


def _record(audit: dict, record: dict) -> dict:
    if record.get("argv"):
        audit["argv"].append(record["argv"])

    audit["result"].append({k: v for k, v in record.items() if k != "argv"})

    if record.get("refused"):
        raise Refused(record["reason"])

    if not record.get("dry_run") and not record.get("performed"):
        result = record.get("result") or {}
        raise Failed(result.get("stderr") or result.get("error") or "the request failed")

    return record


def _execute(client: Client, store, incident: dict, steps: list, proposal: dict, dry_run: bool,
             wait, audit: dict, performed: list) -> str:
    pack, deployment, flow = incident["pack"], incident["deployment_crn"], incident["deployed_flow_crn"]

    if not dry_run:
        guardrails.assert_rate_allowed(performed, client.now())
        unreachable = client.workload_unreachable()

        if unreachable:
            raise Refused(f"the DataFlow workload endpoint is not reachable: {unreachable}")

    guardrails.assert_not_half_applied(client, deployment, flow)

    if catalog.family_of(steps) == catalog.FLOW_EDIT:
        incident = rebased(client, store, incident)

    report = plan(client, incident, steps)

    if not report["ok"]:
        raise Refused("; ".join(report["problems"]))

    if incident.get("rebased_from"):
        if report["diff"] != proposal.get("diff"):
            raise Refused(f"the deployment now runs {incident['expected_flow_version_crn']}, which "
                          f"{incident['rebased_by']} deployed, and on it this edit no longer makes exactly "
                          "the reviewed change; diagnose again")

        log(f"[{pack}] {incident['id']}: planned on {incident['expected_flow_version_crn']} "
            f"({incident['rebased_by']}'s change) instead of {incident['rebased_from']}; the diff is unchanged")
        audit["rebase"] = {"from_flow_version_crn": incident["expected_flow_version_crn"]}

    family = report["family"]

    if family == catalog.NONE:
        return ""

    if family != catalog.FLOW_EDIT:
        for plane, args in report["_writes"]:
            _record(audit, guardrails.act(client, pack, deployment, args, plane=plane, dry_run=dry_run))

            if not dry_run:
                wait()

        return ""

    os.makedirs(config.EDITS_DIR, exist_ok=True)
    path = os.path.join(config.EDITS_DIR, f"{incident['id']}-p{proposal.get('id')}.json")

    with open(path, "w") as handle:
        json.dump(report["_definition"], handle, indent=2, sort_keys=True)

    comments = _comments(incident, steps)
    imported = _record(audit, guardrails.act(
        client, pack, deployment, _import_args(report["_flow_crn"], path, comments),
        plane="df", dry_run=dry_run))

    listen = bool(guardrails.expected_for(pack).get("has_listen_components"))

    if dry_run:
        audit["argv"].append(client.argv("df", guardrails.change_flow_version_args(
            deployment, flow, NEW_VERSION, listen)))
        return ""

    version = imported.get("result") or {}
    new_crn = version.get("crn") or (version.get("flowVersion") or {}).get("crn")

    if not new_crn:
        raise Failed(f"the import returned no flow version CRN: {version}")

    store.note_flow_version(new_crn, pack, report["_flow_crn"], version.get("version"), client.now(),
                            source="agent", incident_id=incident["id"], comments=comments)

    try:
        _record(audit, guardrails.change_flow_version(client, pack, deployment, flow, new_crn,
                                                      dry_run=False))

    except (Refused, Failed) as exc:
        raise Failed(f"v{version.get('version')} was imported but not deployed: {exc}") from None

    wait()
    facts = client.flow_facts(deployment, flow)

    if facts.get("flow_version_crn") != new_crn:
        raise Failed(f"the deployment runs {facts.get('flow_version_crn')} ({facts.get('state')}), "
                     f"not the new {new_crn}")

    return new_crn
