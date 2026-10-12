"""
Guardrails every mutating DataFlow call passes through.

The CDP account and DataFlow service are shared, so the cost of a wrong
CRN is changing a colleague's flow. The checks are deliberately
redundant; any one of them alone can be defeated by a stale file, a
mistyped override or a copied CRN.

`act()` applies the deployment guard itself, so no mutation can skip it,
and checks `dry_run` *after* the guard so a dry run exercises the same
refusals an armed run would.

Ported from SIB self_healing/df_api.py, with two corrections: the
expected CRNs come from the fleet registry by pack name (never from the
target itself, which would make the comparison tautological), and every
check raises `NotAllowed` rather than some returning a dict.
"""

import config
from nifi_healer import fleet
from nifi_healer.cdp.client import Client, is_half_applied


class NotAllowed(Exception):
    """A mutating call was refused by a guardrail."""


def expected_for(pack: str) -> dict:
    entry = fleet.pack(pack)

    if not entry:
        raise NotAllowed(
            f"pack {pack!r} is not in the fleet registry ({config.FLEET_FILE}), "
            "so there is nothing this app is allowed to act on. Run the "
            "provisioning scripts, or provision/05_register_fleet.py."
        )

    return entry


def assert_action_allowed(client: Client, pack: str, deployment_crn: str) -> dict:
    """
    Refuse to mutate anything that is not our deployment for `pack`.

      1. not on the denylist of pre-existing deployments
      2. exactly the CRN recorded for this pack at provisioning time
      3. its live name carries our prefix
      4. it lives in the configured DataFlow service

    Returns the live deployment object.
    """

    if deployment_crn in config.DENYLIST_DEPLOYMENT_CRNS:
        raise NotAllowed(f"{deployment_crn} is on the denylist. Refusing.")

    expected = expected_for(pack).get("deployment_crn")

    if not expected:
        raise NotAllowed(f"no deployment CRN is recorded for pack {pack!r}.")

    if deployment_crn != expected:
        raise NotAllowed(
            "target does not match the deployment recorded for this pack.\n"
            f"  target:   {deployment_crn}\n  recorded: {expected}"
        )

    deployment = client.describe_deployment(deployment_crn)

    if "error" in deployment:
        raise NotAllowed(
            "could not read the deployment to verify it is ours, so the action "
            f"is refused: {deployment.get('error')}"
        )

    name = deployment.get("name") or ""

    if not name.startswith(config.PREFIX):
        raise NotAllowed(
            f"deployment is named {name!r}, which lacks the {config.PREFIX!r} prefix."
        )

    service_crn = (deployment.get("service") or {}).get("crn") or ""

    if not config.SERVICE_CRN:
        raise NotAllowed("SELFHEAL_SERVICE_CRN is not set, so the service cannot be verified.")

    if service_crn != config.SERVICE_CRN:
        raise NotAllowed(
            "deployment is in an unexpected DataFlow service.\n"
            f"  found:    {service_crn}\n  expected: {config.SERVICE_CRN}"
        )

    return deployment


def assert_flow_version_allowed(client: Client, pack: str, flow_version_crn: str) -> dict:
    """
    Refuse to point our deployment at a flow version that is not ours.

    change-flow-version-in-deployment takes a second CRN, the version to
    deploy, which `assert_action_allowed` never sees. A correct deployment
    plus someone else's version CRN would pass every other check.

      1. the version CRN is our recorded flow CRN plus "/..."
      2. that flow really carries our prefix, re-checked locally because
         the server-side search is a substring match
      3. the version exists in that flow's own version list

    Returns the version record.
    """

    if not flow_version_crn:
        raise NotAllowed("no flow-version CRN was supplied.")

    expected_flow_crn = expected_for(pack).get("flow_crn")

    if not expected_flow_crn:
        raise NotAllowed(f"no flow CRN is recorded for pack {pack!r}.")

    # The trailing separator matters: without it nfh-p1-x-2 would pass as
    # a version of nfh-p1-x.
    if not flow_version_crn.startswith(expected_flow_crn + "/"):
        raise NotAllowed(
            "the flow-version CRN does not belong to the flow recorded for this pack.\n"
            f"  version: {flow_version_crn}\n  flow:    {expected_flow_crn}"
        )

    ours = [f for f in client.list_flow_definitions(config.PREFIX)
            if f.get("crn") == expected_flow_crn]

    if not ours:
        raise NotAllowed(
            f"{expected_flow_crn} is not among the flows matching {config.PREFIX!r}."
        )

    name = ours[0].get("name") or ""

    if not name.startswith(config.PREFIX):
        raise NotAllowed(f"the flow is named {name!r}, which lacks the {config.PREFIX!r} prefix.")

    versions = client.list_flow_versions(expected_flow_crn)

    if "error" in versions:
        raise NotAllowed(f"could not list the flow's versions: {versions.get('error')}")

    matches = [v for v in versions.get("flowVersions") or []
               if isinstance(v, dict) and v.get("crn") == flow_version_crn]

    if not matches:
        raise NotAllowed(f"{flow_version_crn} is not a version of {name!r}.")

    return matches[0]


def assert_not_half_applied(client: Client, deployment_crn: str, deployed_flow_crn: str) -> dict:
    facts = client.flow_facts(deployment_crn, deployed_flow_crn)

    if facts.get("error"):
        raise NotAllowed(f"could not read the deployed flow: {facts['error']}")

    if is_half_applied(facts):
        raise NotAllowed(
            f"a previous version change is half-applied ({facts['valid_actions']}). "
            "Resolve it by hand with retry- or cancel-change-flow-version-in-deployment."
        )

    return facts


def assert_rate_allowed(action_times: list, now: float) -> None:
    """
    Cooldown since the last action, and a cap per window, per deployment.

    `action_times` are the epoch seconds of previous *performed* (not dry
    run) actions against this deployment.
    """

    recent = sorted(t for t in action_times if t is not None)

    if recent and now - recent[-1] < config.COOLDOWN_SECONDS:
        wait = int(config.COOLDOWN_SECONDS - (now - recent[-1]))

        raise NotAllowed(f"cooldown: the last action was {int(now - recent[-1])}s ago; wait {wait}s.")

    in_window = [t for t in recent if now - t < config.ACTION_WINDOW_SECONDS]

    if len(in_window) >= config.MAX_ACTIONS_PER_WINDOW:
        raise NotAllowed(
            f"{len(in_window)} actions in the last {config.ACTION_WINDOW_SECONDS}s, "
            f"the maximum is {config.MAX_ACTIONS_PER_WINDOW}."
        )


def act(client: Client, pack: str, deployment_crn: str, args: list,
        plane: str = "dfworkload", dry_run: bool = True) -> dict:
    """
    Perform one mutating call, guardrail first.

    Returns an audit record. It never contains a credential: `argv` is
    the list this module built, and credentials travel in the environment.
    """

    argv = client.argv(plane, args)

    try:
        assert_action_allowed(client, pack, deployment_crn)

    except NotAllowed as exc:
        return {"performed": False, "refused": True, "reason": str(exc), "argv": argv}

    if dry_run:
        return {"performed": False, "dry_run": True, "plane": plane, "argv": argv}

    result = client.dfw(*args) if plane == "dfworkload" else client.df(*args)

    return {"performed": "error" not in result, "plane": plane, "argv": argv, "result": result}


def change_flow_version_args(deployment_crn: str, deployed_flow_crn: str,
                             flow_version_crn: str, has_listen_components: bool = False) -> list:
    """
    The swap command.

    The CLI extension requires --service-crn and --deployed-flow-crn as
    well as the two obvious CRNs (cdpcli/extensions/df/
    changeflowversionindeployment.py, `required`).

    ONLY_RESTART_AFFECTED_COMPONENTS leaves queues in place, which is the
    only way a fix can be measured against a backlog that was not emptied
    for us. STOP_AND_EMPTY_QUEUES is never used: it "fixes" every queue
    KPI whether or not the change did anything. `--kpis` is never passed:
    it would replace every configured KPI.

    A flow with a Listen* processor (p5's partner stub) has no inbound
    connection endpoint, by design: it is only called from inside the
    same pod. CDF validates listen components against the deployment's
    inbound configuration, so those packs skip that check.
    """

    args = [
        "change-flow-version-in-deployment",
        "--service-crn", config.SERVICE_CRN,
        "--deployment-crn", deployment_crn,
        "--deployed-flow-crn", deployed_flow_crn,
        "--flow-version-crn", flow_version_crn,
        "--strategy", "ONLY_RESTART_AFFECTED_COMPONENTS",
    ]

    if has_listen_components:
        args.append("--ignore-deployment-inbound-configuration-checks")

    return args


def change_flow_version(client: Client, pack: str, deployment_crn: str,
                        deployed_flow_crn: str, flow_version_crn: str,
                        dry_run: bool = True) -> dict:
    """Both guards, then the swap."""

    try:
        version = assert_flow_version_allowed(client, pack, flow_version_crn)

    except NotAllowed as exc:
        return {"performed": False, "refused": True, "reason": str(exc), "argv": []}

    listen = bool(expected_for(pack).get("has_listen_components"))

    result = act(
        client, pack, deployment_crn,
        change_flow_version_args(deployment_crn, deployed_flow_crn, flow_version_crn, listen),
        plane="df", dry_run=dry_run,
    )

    result["target_version"] = version.get("version")

    return result
