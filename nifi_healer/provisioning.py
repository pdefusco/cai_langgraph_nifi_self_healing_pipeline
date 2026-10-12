"""
Provisioning steps, shared by the provision/ scripts and the mock
bootstrap, so the mock tenant is built by exactly the argv a live one is.

Each step is idempotent (it finds what an earlier run created, by name)
and records what it created in the fleet file, which is what the
guardrails later compare against:

    import_flow          df import-flow-definition
    create_deployment    df create-deployment, then wait for it
    configure_kpis       dfworkload get-flow-configuration-metadata-in-deployment
                         dfworkload get-flow-configuration-in-deployment
                         dfworkload update-flow-in-deployment --kpis
                         then wait for kpisDirty=false and find the charts
    reset_pack           back to the faulty baseline: flow version,
                         parameters, sizing (each through the guardrails)
    teardown_pack        dfworkload terminate-deployment, then df delete-flow
    verify_registration  the recorded CRNs, read back live
    adopt                rebuild a lost fleet entry, by exact name

`wait` is how a step waits for the tenant; the mock bootstrap passes one
that applies pending changes at once instead of sleeping through them.

Ported from SIB provision/01_import_flow.py, 02_create_deployment.py and
03_configure_kpi.py.
"""

import json
import os

import config
from flows import packs
from flows.build_all import render
from nifi_healer import fleet
from nifi_healer.cdp.client import STEADY_STATES, Client, command_line, require
from nifi_healer.kpi import metrics

DEPLOY_DEADLINE = 45 * 60
KPI_DEADLINE = 10 * 60


def log(message: str) -> None:
    print(message, flush=True)


def definition_file(pack_name: str) -> str:
    """flows/out/<pack>.json, written fresh from the builder."""

    path = os.path.join(config.FLOWS_OUT_DIR, f"{pack_name}.json")
    text = render(packs.build(pack_name))
    os.makedirs(config.FLOWS_OUT_DIR, exist_ok=True)

    if not os.path.exists(path) or open(path).read() != text:
        with open(path, "w") as handle:
            handle.write(text)

    return path


def _flow_by_name(client: Client, name: str) -> dict | None:
    # The search term is a substring match; the name must be exact.
    return next((f for f in client.list_flow_definitions(name) if f.get("name") == name), None)


def import_flow(client: Client, pack_name: str) -> dict:
    name = packs.flow_name(pack_name)
    existing = _flow_by_name(client, name)

    if existing:
        crn = existing["crn"]
        recorded = fleet.pack(pack_name)
        baseline = recorded.get("baseline_flow_version_crn") if recorded.get("flow_crn") == crn else None

        if not baseline:
            versions = require(client.list_flow_versions(crn), f"list versions of {name}")
            first = min(versions.get("flowVersions") or [], key=lambda v: v.get("version") or 0)
            baseline = first["crn"]

        log(f"  [import] {name} already in the catalog ({existing.get('versionCount')} version(s))")

    else:
        result = require(client.df(
            "import-flow-definition",
            "--file", definition_file(pack_name),
            "--name", name,
            "--description", f"Self-healing test pack {pack_name}.",
            "--comments", "baseline: the faulty version every reset returns to",
        ), f"import {name}")
        crn = result["crn"]
        baseline = result["versions"][0]["crn"]
        log(f"  [import] {name} -> {baseline}")

    return fleet.update_pack(pack_name, flow_name=name, flow_crn=crn,
                             baseline_flow_version_crn=baseline)


def create_deployment_args(pack_name: str, flow_version_crn: str) -> list:
    pack = packs.PACKS[pack_name]
    args = [
        "create-deployment",
        "--service-crn", config.SERVICE_CRN,
        "--flow-version-crn", flow_version_crn,
        "--deployment-name", packs.flow_name(pack_name),
        "--cluster-size", json.dumps({"name": pack.cluster_size}),
        "--static-node-count", pack.nodes,
        "--cfm-nifi-version", config.NIFI_VERSION,
        "--auto-start-flow",
    ]

    if pack.has_listen_components:
        args.append("--ignore-deployment-inbound-configuration-checks")

    return args


def start_deployment(client: Client, pack_name: str) -> str:
    """Request the deployment (or find the one already requested). Returns its CRN."""

    name = packs.flow_name(pack_name)
    entry = fleet.pack(pack_name)

    if not entry.get("baseline_flow_version_crn"):
        raise SystemExit(f"{pack_name}: no flow version recorded; import the flow first.")

    existing = next((d for d in client.list_deployments() if d.get("name") == name), None)

    if existing:
        crn = existing["crn"]

        if existing.get("service", {}).get("crn") != config.SERVICE_CRN:
            raise SystemExit(f"{name} exists in another service; refusing to adopt it.")

        log(f"  [deploy] {name} already exists ({(existing.get('status') or {}).get('state')})")

        return crn

    result = require(client.df(*create_deployment_args(pack_name, entry["baseline_flow_version_crn"])),
                     f"create deployment {name}")
    crn = result["deploymentCrn"]
    log(f"  [deploy] {name} -> {crn}")

    return crn


def create_deployment(client: Client, pack_name: str, wait=None, crn: str = "") -> dict:
    """
    Start the deployment unless `crn` says it was started already, wait
    for it, and record it. `wait(crn)` returns the state it reached.
    """

    pack = packs.PACKS[pack_name]
    name = packs.flow_name(pack_name)
    crn = crn or start_deployment(client, pack_name)
    state = (wait or (lambda c: client.wait_for_steady_state(c, DEPLOY_DEADLINE)))(crn)

    if state not in ("GOOD_HEALTH", "CONCERNING_HEALTH", "BAD_HEALTH"):
        raise SystemExit(f"{name} did not reach a steady state (last: {state}).")

    deployed_flow = client.deployed_flow_crn(crn)

    if not deployed_flow:
        raise SystemExit(f"{name} is {state} but has no deployed flow.")

    return fleet.update_pack(pack_name, deployment_name=name, deployment_crn=crn,
                             deployed_flow_crn=deployed_flow,
                             cluster_size=pack.cluster_size, nodes=pack.nodes,
                             has_listen_components=pack.has_listen_components)


def component_ids(metadata: dict) -> dict:
    """{(scope, component name): componentId} from the KPI metadata."""

    found = {}

    for scope in (metadata.get("kpiMetaData") or {}).get("kpiScopes") or []:
        for group in scope.get("contextGroups") or []:
            for component in group.get("scopeComponents") or []:
                found[(scope.get("type"), component.get("name"))] = component.get("id")

    return found


def offered_metrics(metadata: dict) -> dict:
    """{scope: {metricId, ...}} the tenant offers."""

    return {
        scope.get("type"): {m.get("id") for m in scope.get("metricTypes") or []}
        for scope in (metadata.get("kpiMetaData") or {}).get("kpiScopes") or []
    }


def kpi_payloads(pack_name: str, metadata: dict) -> list:
    """[(scenario Kpi, ConfiguredKpi)] for every KPI in the pack."""

    ids = component_ids(metadata)
    offered = offered_metrics(metadata)
    out = []

    for kpi in packs.PACKS[pack_name].kpis:
        entry = metrics.metric(kpi.metric)
        scope = metrics.KIND_SCOPES[kpi.kind]

        if entry.metric_id not in offered.get(scope, set()):
            raise SystemExit(
                f"{kpi.key}: {entry.metric_id} is not offered for {scope}. Run "
                "provision/03_discover_kpi_metadata.py and correct provision/out/metric_overrides.json."
            )

        component_id = ""

        if scope != "SYSTEM":
            component_id = ids.get((scope, kpi.component))

            if not component_id:
                raise SystemExit(f"{kpi.key}: no {scope} named {kpi.component!r} in the metadata.")

        out.append((kpi, metrics.kpi_payload(kpi, component_id)))

    return out


def merged_kpis(existing: list, ours: list) -> list:
    """
    The whole list to send: KPIs someone else configured, kept as read,
    then ours. A KPI that measures the same thing as one of ours is ours.
    """

    kept = [k for k in existing if not any(metrics.same_kpi(k, o) for o in ours)]

    for payload in ours:
        previous = next((k for k in existing if metrics.same_kpi(k, payload)), None)

        if previous and previous.get("id"):
            payload = dict(payload, id=previous["id"])

        kept.append(payload)

    return kept


def configure_kpis(client: Client, pack_name: str, wait=None) -> dict:
    entry = fleet.pack(pack_name)
    deployment, flow = entry.get("deployment_crn"), entry.get("deployed_flow_crn")

    if not (deployment and flow):
        raise SystemExit(f"{pack_name}: no deployment recorded; create it first.")

    metadata = client.flow_configuration_metadata(deployment, flow)
    require(metadata, f"read KPI metadata for {pack_name}")
    payloads = kpi_payloads(pack_name, metadata)

    for attempt in (1, 2):
        configuration = require(client.flow_configuration(deployment, flow),
                                f"read flow configuration for {pack_name}")
        kpis = merged_kpis(configuration.get("kpis") or [], [p for _, p in payloads])
        result = client.dfw(
            "update-flow-in-deployment",
            "--deployment-crn", deployment,
            "--deployed-flow-crn", flow,
            "--configuration-version", configuration.get("configurationVersion"),
            "--kpis", json.dumps(kpis),
        )

        if "error" not in result:
            break

        if attempt == 2 or "CONFLICT" not in (result.get("stderr") or ""):
            require(result, f"configure KPIs for {pack_name}")

        log("  [kpis] configuration version moved; re-reading once")

    log(f"  [kpis] {pack_name}: {len(payloads)} KPI(s) written, waiting for kpisDirty=false")
    (wait or (lambda: _wait_for_clean(client, deployment, flow)))()

    charts = client.metric_charts(deployment, flow)["charts"]
    recorded = []

    for kpi, payload in payloads:
        chart = metrics.find_chart(charts, kpi.kind, kpi.component, kpi.metric)
        recorded.append({
            "scenario": kpi.scenario,
            "key": kpi.key,
            "kind": kpi.kind,
            "component": kpi.component,
            "metric": kpi.metric,
            "above": kpi.above,
            "below": kpi.below,
            "tolerance_seconds": kpi.tolerance_seconds,
            "good": kpi.good,
            "primary": kpi.primary,
            "component_id": payload.get("componentId", ""),
            "chart": list(metrics.chart_key(chart)) if chart else None,
        })

    missing = [r["key"] for r in recorded if not r["chart"]]

    if missing:
        log(f"  [kpis] no chart yet for: {', '.join(missing)} (charts can lag; 05 re-checks)")

    return fleet.update_pack(pack_name, kpis=recorded, kpis_configured=not missing)


def _wait_for_clean(client: Client, deployment: str, flow: str) -> None:
    started = client.now()

    while client.now() - started < KPI_DEADLINE:
        configuration = client.flow_configuration(deployment, flow)

        if "error" not in configuration and not configuration.get("kpisDirty"):
            return

        client.sleep(10)

    raise SystemExit("kpisDirty did not clear within the deadline.")


# ---------------------------------------------------------------------
# Reset: put a pack back to its faulty baseline, so the same experiment
# can be run again (Phases 2-3 replay against these).
# ---------------------------------------------------------------------


def _wait_for_flow(client: Client, deployment: str, flow: str) -> str:
    """Poll the deployed flow until a version swap has finished."""

    started = client.now()

    while client.now() - started < DEPLOY_DEADLINE:
        facts = client.flow_facts(deployment, flow)

        if facts["state"] in STEADY_STATES and client.deployment_facts(deployment)["state"] in STEADY_STATES:
            return facts["state"]

        client.sleep(20)

    return facts["state"]


def reset_steps(client: Client, pack_name: str) -> list:
    """
    [(label, plane, args)] that return the pack to its baseline: the
    first flow version, the scenario parameter values, and the pack's
    sizing. Only what differs from the baseline is included.
    """

    pack = packs.PACKS[pack_name]
    entry = fleet.pack(pack_name)
    deployment, flow = entry.get("deployment_crn"), entry.get("deployed_flow_crn")

    if not (deployment and flow):
        raise SystemExit(f"{pack_name}: no deployment recorded.")

    steps = []
    facts = client.flow_facts(deployment, flow)
    baseline = entry.get("baseline_flow_version_crn")

    if facts.get("error"):
        raise SystemExit(f"{pack_name}: could not read the deployed flow: {facts['error']}")

    if facts["flow_version_crn"] != baseline:
        steps.append((f"flow version v{facts['flow_version']} -> baseline", "df", "version", baseline))

    if pack.parameters:
        configuration = require(client.flow_configuration(deployment, flow),
                                f"read flow configuration for {pack_name}")
        groups = []

        for group in configuration.get("parameterGroups") or []:
            changed = [{"name": p["name"], "value": pack.parameters[p["name"]]}
                       for p in group.get("parameters") or []
                       if p.get("name") in pack.parameters
                       and p.get("value") != pack.parameters[p["name"]]]

            if changed:
                groups.append({"name": group["name"], "parameters": changed})

        if groups:
            names = ", ".join(p["name"] for g in groups for p in g["parameters"])
            steps.append((f"parameters {names} -> baseline", "dfworkload", "parameters", groups))

    sizing = require(client.deployment_configuration(deployment), f"read sizing for {pack_name}")
    size = (sizing.get("clusterSize") or {}).get("name")
    nodes = sizing.get("staticNodeCount")

    if size != pack.cluster_size or nodes != pack.nodes:
        steps.append((f"sizing {size} x{nodes} -> {pack.cluster_size} x{pack.nodes}",
                      "dfworkload", "sizing", None))

    return steps


def reset_pack(client: Client, pack_name: str, dry_run: bool = True, wait=None) -> list:
    """
    Apply `reset_steps`, each through the guardrails, waiting between
    them because each write needs a settled deployment. Returns the
    audit records. `wait()` replaces the polling (the mock settles).
    """

    from nifi_healer.cdp import guardrails

    entry = fleet.pack(pack_name)
    deployment, flow = entry["deployment_crn"], entry["deployed_flow_crn"]
    pack = packs.PACKS[pack_name]
    wait = wait or (lambda: _wait_for_flow(client, deployment, flow))
    records = []

    for label, plane, kind, value in reset_steps(client, pack_name):
        log(f"  [reset] {label}")

        # Configuration versions are read immediately before each write.
        if kind == "version":
            record = guardrails.change_flow_version(client, pack_name, deployment, flow, value,
                                                    dry_run=dry_run)

        elif kind == "parameters":
            configuration = require(client.flow_configuration(deployment, flow), "read configuration")
            record = guardrails.act(client, pack_name, deployment, [
                "update-flow-in-deployment",
                "--deployment-crn", deployment,
                "--deployed-flow-crn", flow,
                "--configuration-version", configuration.get("configurationVersion"),
                "--parameter-groups", json.dumps(value),
            ], plane=plane, dry_run=dry_run)

        else:
            sizing = require(client.deployment_configuration(deployment), "read sizing")
            record = guardrails.act(client, pack_name, deployment, [
                "update-deployment",
                "--deployment-crn", deployment,
                "--configuration-version", sizing.get("configurationVersion"),
                "--cluster-size", json.dumps({"name": pack.cluster_size}),
                "--static-node-count", pack.nodes,
            ], plane=plane, dry_run=dry_run)

        record["step"] = label
        records.append(record)

        if record.get("refused"):
            raise SystemExit(f"{pack_name}: refused: {record['reason']}")

        if dry_run:
            log("    would run: " + command_line(record["argv"]))
            continue

        if not record.get("performed"):
            require(record.get("result") or {"error": "the request failed"}, label)

        wait()

    if not records:
        log(f"  [reset] {pack_name} is already at its baseline")

    return records


# ---------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------


def teardown_pack(client: Client, pack_name: str, keep_flow: bool = False,
                  dry_run: bool = True, deployment_override: str = "") -> bool:
    """
    Terminate the recorded deployment, then delete the recorded catalog
    flow (a referenced flow cannot be deleted, so the order is fixed).
    Targets come from the fleet file, never from a name search, and each
    is re-checked live. True when everything asked for is gone.
    """

    if not (deployment_override and dry_run):
        return _teardown_pack(client, pack_name, keep_flow, dry_run, deployment_override)

    # A dry run leaves the fleet file as it found it.
    recorded = fleet.pack(pack_name).get("deployment_crn")

    try:
        return _teardown_pack(client, pack_name, keep_flow, dry_run, deployment_override)

    finally:
        fleet.update_pack(pack_name, deployment_crn=recorded)


def _teardown_pack(client: Client, pack_name: str, keep_flow: bool, dry_run: bool,
                   deployment_override: str) -> bool:
    from nifi_healer.cdp import guardrails

    entry = fleet.pack(pack_name)
    deployment = deployment_override or entry.get("deployment_crn")
    flow_crn = entry.get("flow_crn")
    gone = True

    if deployment:
        if deployment_override:
            # Keeps the prefix, service and denylist checks; only the
            # comparison with the (lost) fleet record is bypassed.
            fleet.update_pack(pack_name, deployment_crn=deployment)

        state = client.deployment_facts(deployment)["state"]
        log(f"  [teardown] deployment {deployment} ({state})")

        if state == "UNKNOWN" and not _listed(client, deployment):
            log("    not listed; already gone")

        else:
            record = guardrails.act(client, pack_name, deployment,
                                    ["terminate-deployment", "--deployment-crn", deployment],
                                    plane="dfworkload", dry_run=dry_run)

            if record.get("refused"):
                raise SystemExit(f"{pack_name}: refused: {record['reason']}")

            if dry_run:
                log("    would run: " + command_line(record["argv"]))

            elif not record.get("performed"):
                require(record.get("result") or {"error": "terminate failed"}, "terminate")

            else:
                gone = _wait_for_gone(client, deployment)

    if flow_crn and not keep_flow:
        if not gone:
            log("    skipping the flow: its deployment is not gone yet")
            return False

        match = next((f for f in client.list_flow_definitions(config.PREFIX)
                      if f.get("crn") == flow_crn), None)

        if not match:
            log(f"  [teardown] flow {flow_crn} not in the catalog; nothing to delete")

        elif not (match.get("name") or "").startswith(config.PREFIX):
            raise SystemExit(f"flow {flow_crn} is named {match.get('name')!r}, without the "
                             f"{config.PREFIX!r} prefix. Refusing to delete it.")

        elif dry_run:
            log(f"  [teardown] would run: cdp df delete-flow --flow-crn {flow_crn}")

        else:
            result = client.df("delete-flow", "--flow-crn", flow_crn)

            if "error" in result:
                log(f"    could not delete the flow: {result.get('stderr') or result['error']}")
                return False

            log(f"  [teardown] deleted flow {match['name']} ({match.get('versionCount')} version(s))")

    if dry_run:
        return True

    # The guardrails authorise against these CRNs; a fleet entry naming
    # a terminated deployment must not survive the teardown.
    data = fleet.read()

    if keep_flow:
        kept = {k: entry[k] for k in ("flow_name", "flow_crn", "baseline_flow_version_crn") if k in entry}
        data["packs"][pack_name] = kept

    else:
        data["packs"].pop(pack_name, None)

    fleet.write(data)

    return gone


def _listed(client: Client, deployment: str) -> bool:
    return any(d.get("crn") == deployment for d in client.list_deployments())


def _wait_for_gone(client: Client, deployment: str) -> bool:
    started = client.now()

    while client.now() - started < 30 * 60:
        state = client.deployment_facts(deployment)["state"]

        # A failed read only counts as gone once the listing agrees, or a
        # transient API error would be reported as a finished teardown.
        if state == "TERMINATED" or (state == "UNKNOWN" and not _listed(client, deployment)):
            log("    terminated")
            return True

        client.sleep(20)

    log("    still terminating after 30 minutes; check the CDF UI")
    return False


# ---------------------------------------------------------------------
# Registration: confirm the fleet file still describes the tenant
# ---------------------------------------------------------------------


def recheck_charts(client: Client, pack_name: str) -> dict:
    """Look again for KPI charts 04 could not find yet."""

    entry = fleet.pack(pack_name)
    kpis = entry.get("kpis") or []

    if all(k.get("chart") for k in kpis):
        return entry

    charts = client.metric_charts(entry["deployment_crn"], entry["deployed_flow_crn"])["charts"]

    for kpi in kpis:
        if not kpi.get("chart"):
            chart = metrics.find_chart(charts, kpi["kind"], kpi["component"], kpi["metric"])
            kpi["chart"] = list(metrics.chart_key(chart)) if chart else None

    return fleet.update_pack(pack_name, kpis=kpis,
                             kpis_configured=bool(kpis) and all(k["chart"] for k in kpis))


def verify_registration(client: Client, pack_name: str) -> list:
    """Problems with the recorded CRNs, read live. Empty when all is well."""

    entry = fleet.pack(pack_name)
    problems = []

    if not entry.get("deployment_crn"):
        return ["no deployment recorded"]

    deployment = client.describe_deployment(entry["deployment_crn"])

    if "error" in deployment:
        return [f"the recorded deployment cannot be read: {deployment['error']}"]

    if deployment.get("name") != entry.get("deployment_name"):
        problems.append(f"deployment is named {deployment.get('name')!r}, "
                        f"recorded as {entry.get('deployment_name')!r}")

    if (deployment.get("service") or {}).get("crn") != config.SERVICE_CRN:
        problems.append("deployment is not in SELFHEAL_SERVICE_CRN")

    if deployment.get("flowCrn") != entry.get("flow_crn"):
        problems.append(f"deployment runs flow {deployment.get('flowCrn')}, "
                        f"recorded {entry.get('flow_crn')}")

    live_flow = client.deployed_flow_crn(entry["deployment_crn"])

    if live_flow != entry.get("deployed_flow_crn"):
        problems.append(f"deployed flow is {live_flow or 'absent'}, recorded {entry.get('deployed_flow_crn')}")

    return problems


def adopt(client: Client, pack_name: str) -> dict:
    """
    Rebuild a lost fleet entry from the tenant, by exact name within our
    service. Weaker than a record made at creation time (it trusts the
    name), so only on request, and only for names carrying our prefix.
    """

    name = packs.flow_name(pack_name)

    if not name.startswith(config.PREFIX):
        raise SystemExit(f"{name} lacks the {config.PREFIX!r} prefix.")

    if fleet.pack(pack_name).get("deployment_crn"):
        raise SystemExit(f"{pack_name} is already registered; adopt only rebuilds a lost entry.")

    flows = [f for f in client.list_flow_definitions(name) if f.get("name") == name]
    deployments = [d for d in client.list_deployments()
                   if d.get("name") == name and (d.get("service") or {}).get("crn") == config.SERVICE_CRN]

    if len(flows) != 1 or len(deployments) > 1:
        raise SystemExit(f"{name}: found {len(flows)} flow(s) and {len(deployments)} deployment(s) "
                         "by that name; adopt needs exactly one of each. Refusing to guess.")

    import_flow(client, pack_name)

    if deployments:
        # The deployment is already up: record it as it stands, no waiting.
        create_deployment(client, pack_name, wait=lambda crn: client.deployment_facts(crn)["state"],
                          crn=deployments[0]["crn"])

    # KPI charts are not recorded here; 04 finds ours again (it is
    # idempotent: our KPIs are matched and keep their ids).
    return fleet.update_pack(pack_name, kpis=[], kpis_configured=False)
