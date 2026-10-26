"""
A `Client` that answers CDP CLI argv from the simulated tenant.

It parses the same argument lists LiveClient would hand to cdpcli and
returns responses shaped like the real ones, field names included, so
everything above the client (guardrails, provisioning, the poller, the
executor) runs unchanged in mock mode. Refusals come back in LiveClient's
error shape.

Every argv is first checked against the installed cdpcli's own command
and option tables (cli_spec), so a command line the real CLI would
reject is rejected here too.

Chart surfaces, as observed on a live tenant:

    list-flow-kpis-in-deployment     connection and processor KPIs
    list-deployment-kpis             system-scope KPIs
    list-deployment-system-metrics   CPU, whether or not a KPI exists

A chart only exists for a configured KPI, and only from the moment it
was configured.

Time is simulated: `now()` is the world clock and `sleep()` waits
1/MOCK_SPEED of the requested seconds.
"""

import json
import time
from datetime import datetime, timezone

import config
from nifi_healer.cdp import cli_spec
from nifi_healer.cdp.client import Client, command_line
from nifi_healer.kpi import metrics
from nifi_healer.runtime import telemetry
from nifi_healer.sim import world as worlds
from nifi_healer.sim.world import MockError

CHART_TYPES = {"connection": "CONNECTION", "processor": "PROCESSOR", "system": "SYSTEM"}
CORES_PER_NODE = {"EXTRA_SMALL": 2, "SMALL": 3, "MEDIUM": 6, "LARGE": 12}

# The JVM's share of a node's memory, and how much more each CPU percent takes.
_MEMORY_BASE, _MEMORY_PER_CPU = 9.0, 0.05


def parse_options(tokens: list) -> dict:
    """
    `--flag value` pairs -> {"flag": value}. A flag with no value is True;
    a value that looks like JSON is parsed; `file://path` is read, as
    cdpcli does.
    """

    options = {}
    index = 0

    while index < len(tokens):
        token = str(tokens[index])

        if not token.startswith("--"):
            raise MockError(f"Unexpected argument {token!r}.")

        name = token[2:]
        following = tokens[index + 1] if index + 1 < len(tokens) else None

        if following is None or str(following).startswith("--"):
            options[name] = True
            index += 1

            continue

        value = str(following)

        if value.startswith("file://"):
            with open(value[len("file://"):]) as handle:
                value = handle.read()

        if value[:1] in "{[":
            try:
                value = json.loads(value)

            except ValueError:
                raise MockError(f"--{name} is not valid JSON.") from None

        options[name] = value
        index += 2

    return options


def _ms(seconds: float) -> int:
    return int(seconds * 1000)


def _public(kpi: dict) -> dict:
    return {k: v for k, v in kpi.items() if not k.startswith("_")}


def _derived_system_charts(cpu: dict, deployment: dict) -> list:
    """
    Memory Utilization and Core Allocation on the CPU chart's buckets, as
    list-deployment-system-metrics returns them live. Cores is today's
    allocation for every bucket, also before a resize.
    """

    cores = float(CORES_PER_NODE[deployment["cluster_size"]] * deployment["nodes"])
    charts = []

    for name, value in (("Memory Utilization", lambda v: _MEMORY_BASE + _MEMORY_PER_CPU * v),
                        ("Core Allocation", lambda v: cores)):
        datas = [dict(d, value=round(value(d["value"]), 3)) for d in cpu["metrics"]["datas"]]
        values = [d["value"] for d in datas]
        charts.append({
            "name": name, "unitType": "RATIO" if name.startswith("Memory") else "COUNT",
            "componentType": "SYSTEM", "componentName": None,
            "metrics": {"averageValue": sum(values) / len(values) if values else None,
                        "currentValue": values[-1] if values else None, "datas": datas},
        })

    return charts


class MockClient(Client):
    kind = "mock"

    def __init__(self, world=None):
        self.world = world or worlds.get_world()

    def now(self) -> float:
        return self.world.now()

    def sleep(self, seconds: float) -> None:
        if self.world.stepped:
            self.world.skip(max(0.0, seconds))     # no wall time reaches a stepped clock
            return

        time.sleep(max(0.0, seconds) / self.world.speed)

    def workload_endpoint(self) -> str:
        return "mock (in process)"

    def workload_unreachable(self, timeout: float = 5.0) -> str:
        return ""

    # -- transport ----------------------------------------------------

    def run(self, args: list) -> dict:
        started = time.monotonic()
        result = self._run(args)
        telemetry.record("cdp", time.monotonic() - started, not (isinstance(result, dict) and "error" in result))

        return result

    def _run(self, args: list) -> dict:
        args = [str(a) for a in args]

        if len(args) < 2:
            return {"error": "The CDP CLI returned an error.", "command": command_line(args),
                    "stderr": "usage: cdp <service> <operation> [options]"}

        plane, operation = args[0], args[1]
        handler = getattr(self, f"_{plane}_{operation.replace('-', '_')}", None)

        try:
            options = parse_options(args[2:])
            rejected = cli_spec.check(plane, operation, options)

            if rejected:
                # What cdpcli itself would say, before any request is sent.
                return {"error": "The CDP CLI returned an error.", "command": command_line(args),
                        "return_code": 252, "stderr": rejected, "hint": ""}

            if handler is None:
                raise MockError(f"mock tenant: {plane} {operation} is not simulated.",
                                400, "UNSUPPORTED")

            if plane == "dfworkload" and options.get("environment-crn") != worlds.environment_crn():
                raise MockError(f"Environment {options.get('environment-crn')} not found.",
                                404, "NOT_FOUND")

            return handler(options)

        except MockError as exc:
            stderr = (f"An error occurred: {exc} (Status Code: {exc.status}; Error Code: {exc.code}; "
                      f"Service: {plane}; Operation: {operation}; Request ID: mock)")

            return {"error": "The CDP CLI returned an error.", "command": command_line(args),
                    "return_code": 254, "stderr": stderr, "hint": ""}

    @staticmethod
    def _required(options: dict, *names) -> list:
        missing = [f"--{n}" for n in names if not options.get(n)]

        if missing:
            raise MockError(f"the following arguments are required: {', '.join(missing)}")

        return [options[n] for n in names]

    # -- response shapes ----------------------------------------------

    def _flow_version(self, version: dict) -> dict:
        return {
            "crn": version["crn"],
            "bucketIdentifier": "mock-bucket",
            "author": version["author"],
            "version": version["version"],
            "timestamp": _ms(version["timestamp"]),
            "deploymentCount": sum(1 for d in self.world.deployments()
                                   if d["flow_version_crn"] == version["crn"]),
            "comments": version["comments"],
            "tags": version["tags"],
        }

    def _flow_summary(self, flow: dict) -> dict:
        return {
            "crn": flow["crn"],
            "name": flow["name"],
            "description": flow["description"],
            "versionCount": len(flow["versions"]),
            "createdTimestamp": _ms(flow["created"]),
            "modifiedTimestamp": _ms(flow["modified"]),
            "artifactType": "flow",
        }

    def _deployment_summary(self, deployment: dict) -> dict:
        state, detailed, message = self.world.state(deployment)
        version = self.world.version(deployment["flow_version_crn"])
        flow = self.world.flow(deployment["flow_crn"])
        running = state not in ("DEPLOYING",)
        warnings = len(self.world.alerts(deployment)) if running else 0

        return {
            "crn": deployment["crn"],
            "name": deployment["name"],
            "status": {"state": state, "detailedState": detailed, "message": message},
            "service": {"crn": deployment["service_crn"], "name": "nfh-mock",
                        "cloudProvider": "AWS", "region": worlds.REGION,
                        "environmentCrn": worlds.environment_crn()},
            "created": _ms(deployment["created"]),
            "updated": _ms(deployment["updated"]),
            "clusterSize": deployment["cluster_size"],
            "flowVersionCrn": deployment["flow_version_crn"],
            "flowCrn": deployment["flow_crn"],
            "flowName": flow["name"],
            "flowVersion": version["version"],
            "currentNodeCount": deployment["nodes"] if running else 0,
            "staticNodeCount": deployment["nodes"],
            "autoscalingEnabled": False,
            "configurationVersion": deployment["deployment_configuration_version"],
            "cfmNifiVersion": deployment["nifi_version"],
            "activeInfoAlertCount": 0,
            "activeWarningAlertCount": warnings,
            "activeErrorAlertCount": 0,
            "deployedByName": "mock",
            "artifactTypeName": "flow",
            "validActions": [] if deployment["pending"] else
            ["UPDATE", "CHANGE_FLOW_VERSION", "TERMINATE"],
        }

    def _deployed_flow(self, deployment: dict) -> dict:
        state, detailed, message = self.world.state(deployment)
        changing = self.world.pending(deployment, "version")

        if changing:
            state, detailed, message = "UPDATING", "CHANGING_FLOW_VERSION", \
                "The flow version is being changed."

        version = self.world.version(deployment["flow_version_crn"])

        return {
            "crn": deployment["deployed_flow_crn"],
            "name": deployment["name"],
            "status": {"state": state, "detailedState": detailed, "message": message},
            "flowVersion": version["version"],
            "flowVersionCrn": deployment["flow_version_crn"],
            "flowCrn": deployment["flow_crn"],
            "validActions": [] if deployment["pending"] else ["CHANGE_FLOW_VERSION"],
        }

    def _flow_configuration(self, deployment: dict) -> dict:
        groups = []

        for group in deployment["parameter_groups"]:
            groups.append({
                "name": group["name"],
                "parameters": [
                    dict(p, value=None if p.get("sensitive") else p.get("value"))
                    for p in group["parameters"]
                ],
            })

        return {
            "configurationVersion": deployment["configuration_version"],
            "kpis": [_public(k) for k in deployment["kpis"]
                     if k.get("metricComponentType") in worlds.FLOW_SCOPES],
            "kpisDirty": deployment["kpis_dirty"],
            "parametersDirty": deployment["parameters_dirty"],
            "parameterGroups": groups,
        }

    def _deployment_configuration(self, deployment: dict) -> dict:
        return {
            "configurationVersion": deployment["deployment_configuration_version"],
            "clusterSize": {"name": deployment["cluster_size"],
                            "coresPerNode": CORES_PER_NODE[deployment["cluster_size"]]},
            "staticNodeCount": deployment["nodes"],
            "autoScalingEnabled": False,
            "cfmNifiVersion": deployment["nifi_version"],
            "kpis": [_public(k) for k in deployment["kpis"]
                     if k.get("metricComponentType") in worlds.DEPLOYMENT_SCOPES],
            "kpisDirty": deployment["kpis_dirty"],
        }

    def _chart(self, deployment: dict, key: str, period: str, since: float,
               kpi: dict | None = None) -> dict | None:
        kind, name, logical = key.split("|")

        if not logical or (kind != "system" and not name):
            return None

        entry = metrics.metric(logical)
        datas = self.world.chart_data(deployment, key, period, since)
        values = [d["value"] for d in datas]
        chart = {
            "name": entry.label,
            "unitType": entry.unit_id,
            "componentType": CHART_TYPES[kind],
            "componentName": name,
            "metrics": {
                "averageValue": sum(values) / len(values) if values else None,
                "currentValue": values[-1] if values else None,
                "datas": datas,
            },
        }
        alert = (kpi or {}).get("alert")

        if alert:
            tolerance = alert.get("frequencyTolerance") or {}
            chart["alert"] = {
                "frequencyTolerance": {"unit": (tolerance.get("unit") or {}).get("id"),
                                       "value": tolerance.get("value")},
                "thresholdMoreThan": (alert.get("thresholdMoreThan") or {}).get("value"),
                "thresholdLessThan": (alert.get("thresholdLessThan") or {}).get("value"),
            }

        return chart

    def _kpi_charts(self, deployment: dict, period: str, system: bool) -> list:
        charts = []

        for kpi in deployment["kpis"]:
            if (kpi.get("metricComponentType") == "SYSTEM") != system:
                continue

            chart = self._chart(deployment, self.world.kpi_key(deployment, kpi), period,
                                kpi.get("_created", 0.0), kpi)

            if chart:
                charts.append(chart)

        return charts

    def _running(self, options: dict, flow: bool = False) -> dict:
        deployment = self.world.deployment(
            options.get("deployment-crn"),
            options.get("deployed-flow-crn") if flow else None,
        )

        if self.world.pending(deployment, "deploy"):
            raise MockError("The deployment is still being created.", 409, "CONFLICT")

        return deployment

    # -- df: catalog --------------------------------------------------

    def _df_list_flow_definitions(self, options: dict) -> dict:
        with self.world.reading():
            return {"flows": [self._flow_summary(f)
                              for f in self.world.flows(options.get("search-term") or "")]}

    def _df_describe_flow(self, options: dict) -> dict:
        (flow_crn,) = self._required(options, "flow-crn")

        with self.world.reading():
            flow = self.world.flow(flow_crn)
            detail = self._flow_summary(flow)
            detail["versions"] = [self._flow_version(v) for v in flow["versions"]]

            return {"flowDetail": detail}

    def _df_list_flow_definition_versions(self, options: dict) -> dict:
        (flow_crn,) = self._required(options, "flow-crn")

        with self.world.reading():
            versions = self.world.flow(flow_crn)["versions"]

            return {"flowVersions": [self._flow_version(v) for v in reversed(versions)]}

    def _df_get_flow_version(self, options: dict) -> dict:
        (crn,) = self._required(options, "flow-version-crn")

        # What cdpcli prints, not what the API returns: the CLI decodes the
        # base64 `flowDefinition` and prints the flow JSON itself.
        with self.world.reading():
            return json.loads(json.dumps(self.world.version(crn)["definition"]))

    def _definition_file(self, options: dict) -> dict:
        (path,) = self._required(options, "file")

        if isinstance(path, dict):
            return path

        try:
            with open(path) as handle:
                return json.load(handle)

        except OSError as exc:
            raise MockError(f"Cannot read --file {path}: {exc.strerror}.") from None

        except ValueError:
            raise MockError(f"--file {path} is not JSON.") from None

    def _df_import_flow_definition(self, options: dict) -> dict:
        (name,) = self._required(options, "name")
        flow = self.world.import_flow(name, self._definition_file(options),
                                      options.get("description") or "",
                                      options.get("comments") or "")

        with self.world.reading():
            result = self._flow_summary(flow)
            result["versions"] = [self._flow_version(v) for v in flow["versions"]]

            return result

    def _df_import_flow_definition_version(self, options: dict) -> dict:
        (flow_crn,) = self._required(options, "flow-crn")
        version = self.world.import_version(flow_crn, self._definition_file(options),
                                            options.get("comments") or "")

        with self.world.reading():
            return self._flow_version(version)

    def _df_delete_flow(self, options: dict) -> dict:
        (flow_crn,) = self._required(options, "flow-crn")
        flow = self.world.delete_flow(flow_crn)

        return {"flow": self._flow_summary(flow)}

    # -- df: services and deployments ---------------------------------

    def _service(self) -> dict:
        return {"crn": worlds.service_crn(), "name": "nfh-mock", "cloudPlatform": "AWS",
                "region": worlds.REGION, "environmentCrn": worlds.environment_crn(),
                "status": {"state": "GOOD_HEALTH"}}

    def _df_list_services(self, options: dict) -> dict:
        return {"services": [self._service()]}

    def _df_describe_service(self, options: dict) -> dict:
        (crn,) = self._required(options, "service-crn")

        if crn != worlds.service_crn():
            raise MockError(f"Service {crn} not found.", 404, "NOT_FOUND")

        return {"service": self._service()}

    def _df_list_deployments(self, options: dict) -> dict:
        with self.world.reading():
            deployments = [self._deployment_summary(d) for d in self.world.deployments()]
            term = (options.get("search-term") or "").lower()

            return {"deployments": [d for d in deployments if term in d["name"].lower()]}

    def _df_describe_deployment(self, options: dict) -> dict:
        (crn,) = self._required(options, "deployment-crn")

        with self.world.reading():
            return {"deployment": self._deployment_summary(self.world.deployment(crn))}

    def _df_create_deployment(self, options: dict) -> dict:
        service, version, name = self._required(options, "service-crn", "flow-version-crn",
                                                "deployment-name")
        size = options.get("cluster-size") or {"name": "EXTRA_SMALL"}
        deployment = self.world.create_deployment(
            service, version, name,
            cluster_size=size.get("name") if isinstance(size, dict) else str(size),
            nodes=int(options.get("static-node-count") or 1),
            nifi_version=options.get("cfm-nifi-version") or "",
            parameter_groups=options.get("parameter-groups"),
            kpis=options.get("kpis"),
            ignore_inbound=bool(options.get("ignore-deployment-inbound-configuration-checks")),
        )

        return {"deploymentCrn": deployment["crn"]}

    def _dfworkload_terminate_deployment(self, options: dict) -> dict:
        (crn,) = self._required(options, "deployment-crn")

        with self.world.reading():
            summary = self._deployment_summary(self.world.deployment(crn))

        self.world.terminate_deployment(crn)
        summary["status"] = {"state": "TERMINATING", "detailedState": "TERMINATING", "message": ""}

        return {"deployment": summary}

    def _df_list_flows_in_deployment(self, options: dict) -> dict:
        (crn,) = self._required(options, "deployment-crn")

        with self.world.reading():
            deployment = self.world.deployment(crn)

            if self.world.pending(deployment, "deploy"):
                return {"deployedFlows": []}

            return {"deployedFlows": [self._deployed_flow(deployment)]}

    def _df_describe_flow_in_deployment(self, options: dict) -> dict:
        self._required(options, "deployment-crn", "deployed-flow-crn")

        with self.world.reading():
            return {"deployedFlow": self._deployed_flow(self._running(options, flow=True))}

    def _df_change_flow_version_in_deployment(self, options: dict) -> dict:
        _, deployment_crn, deployed_flow_crn, version_crn = self._required(
            options, "service-crn", "deployment-crn", "deployed-flow-crn", "flow-version-crn")

        if options.get("kpis"):
            # Accepted by the API, and it replaces every KPI. The app never
            # sends it; the mock refuses so a regression shows up in tests.
            raise MockError("mock tenant: --kpis on a version change would replace every KPI.")

        self.world.change_flow_version(
            deployment_crn, deployed_flow_crn, version_crn,
            strategy=options.get("strategy") or "STOP_AND_PROCESS_DATA",
            ignore_inbound=bool(options.get("ignore-deployment-inbound-configuration-checks")),
        )

        return {"crn": deployed_flow_crn}

    # -- df: metrics and alerts ---------------------------------------

    def _df_list_flow_kpis_in_deployment(self, options: dict) -> dict:
        self._required(options, "deployment-crn", "deployed-flow-crn")

        with self.world.reading():
            deployment = self._running(options, flow=True)
            period = options.get("metrics-time-period") or "LAST_THIRTY_MINUTES"

            return {"metricCharts": self._kpi_charts(deployment, period, system=False)}

    def _df_list_deployment_kpis(self, options: dict) -> dict:
        self._required(options, "deployment-crn")

        with self.world.reading():
            deployment = self._running(options)
            period = options.get("metrics-time-period") or "LAST_THIRTY_MINUTES"

            return {"metricCharts": self._kpi_charts(deployment, period, system=True)}

    def _df_list_deployment_system_metrics(self, options: dict) -> dict:
        self._required(options, "deployment-crn")

        with self.world.reading():
            deployment = self._running(options)
            period = options.get("metrics-time-period") or "LAST_THIRTY_MINUTES"
            chart = self._chart(deployment, "system||cpu", period, deployment["created"])

            return {"metricCharts": [chart] + _derived_system_charts(chart, deployment)}

    def _df_list_deployment_active_alerts(self, options: dict) -> dict:
        (crn,) = self._required(options, "deployment-crn")

        with self.world.reading():
            deployment = self.world.deployment(crn)
            now = self.world.now()
            events = []

            for kpi, first in self.world.alerts(deployment):
                key = self.world.kpi_key(deployment, kpi)
                kind, name, logical = key.split("|")
                label = metrics.metric(logical).label

                events.append({
                    "crn": f"{deployment['crn']}/alert/{kpi['id']}",
                    "name": f"{label} on {name}" if name else label,
                    "severity": "WARNING",
                    "firstOccurrence": _ms(first),
                    "lastOccurrence": _ms(now),
                    "eventType": "ALERT",
                    "alertType": "KPI",
                    "message": f"{label} has breached its threshold on {name or deployment['name']}.",
                })

            return {"eventSummaries": events}

    def _df_list_deployment_events(self, options: dict) -> dict:
        (crn,) = self._required(options, "deployment-crn")

        with self.world.reading():
            deployment = self.world.deployment(crn)
            events = sorted(deployment.get("events") or [], key=lambda e: -e["t"])
            limit = int(options.get("max-items") or 20)

            return {"eventSummaries": [
                {"crn": event["crn"], "name": event["name"], "severity": "INFO",
                 "firstOccurrence": _ms(event["t"]), "eventType": "STATUS_UPDATE"}
                for event in events[:limit]
            ]}

    # -- dfworkload -----------------------------------------------------

    def _dfworkload_list_nifi_versions(self, options: dict) -> dict:
        return {"nifiVersions": [config.NIFI_VERSION]}

    def _dfworkload_get_flow_configuration_in_deployment(self, options: dict) -> dict:
        self._required(options, "deployment-crn", "deployed-flow-crn")

        with self.world.reading():
            return {"deployedFlowConfiguration":
                    self._flow_configuration(self._running(options, flow=True))}

    def _dfworkload_get_flow_configuration_metadata_in_deployment(self, options: dict) -> dict:
        self._required(options, "deployment-crn", "deployed-flow-crn")

        with self.world.reading():
            return {"deployedFlowConfigurationMetadata":
                    self.world.kpi_metadata(self._running(options, flow=True))}

    def _dfworkload_update_flow_in_deployment(self, options: dict) -> dict:
        deployment_crn, deployed_flow_crn, version = self._required(
            options, "deployment-crn", "deployed-flow-crn", "configuration-version")

        with self.world.reading():
            self._running(options, flow=True)

        deployment = self.world.update_flow_configuration(
            deployment_crn, deployed_flow_crn, int(version),
            kpis=options.get("kpis"), parameter_groups=options.get("parameter-groups"),
        )

        return {"deployedFlowConfiguration": self._flow_configuration(deployment)}

    def _dfworkload_get_deployment_configuration(self, options: dict) -> dict:
        self._required(options, "deployment-crn")

        with self.world.reading():
            return {"deploymentConfiguration":
                    self._deployment_configuration(self._running(options))}

    def _dfworkload_get_deployment_configuration_metadata(self, options: dict) -> dict:
        self._required(options, "deployment-crn")

        with self.world.reading():
            deployment = self._running(options)

            return {"deploymentConfigurationMetadata": {
                "flowCrn": deployment["flow_crn"],
                "flowVersionCrn": deployment["flow_version_crn"],
                "kpiMetaData": self.world.kpi_metadata(deployment, worlds.DEPLOYMENT_SCOPES)["kpiMetaData"],
            }}

    def _dfworkload_update_deployment(self, options: dict) -> dict:
        crn, version = self._required(options, "deployment-crn", "configuration-version")
        size = options.get("cluster-size")
        nodes = options.get("static-node-count")
        deployment = self.world.update_deployment(
            crn, int(version),
            cluster_size=size.get("name") if isinstance(size, dict) else size,
            nodes=int(nodes) if nodes else None,
            kpis=options.get("kpis"),
        )

        return {"deploymentConfiguration": self._deployment_configuration(deployment)}

    # -- iam ------------------------------------------------------------

    def _iam_generate_workload_auth_token(self, options: dict) -> dict:
        expires = datetime.fromtimestamp(time.time() + 3600, timezone.utc)

        return {"endpointUrl": "https://dfworkload.mock.invalid",
                "expireAt": expires.isoformat(), "token": "mock"}
