"""
The mock tenant: a flow catalog, deployments, and a simulation per
deployment, on a clock that runs MOCK_SPEED times faster than the wall.

It keeps the state the DataFlow API exposes and nothing it does not:

  * the catalog stores each version's definition, never edited in place
  * a deployment runs one catalog version with its parameter groups and
    KPIs; a write is accepted at once and applied after a delay, the way
    CDF answers before the work is done (version ~60 s, configuration
    ~30 s, sizing ~5 min with the flow paused while nodes restart)
  * configuration writes are optimistic: a stale --configuration-version
    is refused
  * an alert fires once a KPI has breached for its frequency tolerance

`MockClient` (nifi_healer/cdp/mock_client.py) turns CLI argv into calls
here and shapes the answers like the real responses.

The whole world is one JSON file (MOCK_WORLD_FILE). Separate processes
(the app, a provisioning script) share it: each reloads when the file is
newer than what it last read, and writes after every mutation.
"""

import contextlib
import copy
import fcntl
import json
import os
import threading
import time
import uuid

import config
from flows.builder.lib import iter_connections, walk_groups
from nifi_healer.kpi import metrics
from nifi_healer.sim.engine import PERIODS, Simulation

REGION = "us-west-1"
ACCOUNT = "mock"

DEPLOY_SECONDS = 180
VERSION_SECONDS = 60
CONFIGURATION_SECONDS = 30
SIZING_SECONDS = 300

# The longest stretch a simulation catches up after the app was down; any
# more is skipped, leaving a gap in the charts as a real outage would.
MAX_CATCH_UP = 6 * 3600

# Status events kept per deployment; a week-long run would otherwise grow the world file for ever.
MAX_EVENTS = 2000

SAVE_EVERY = 60.0              # wall seconds between saves of a quiet world

TOLERANCE_UNITS = {"SECONDS": 1, "MINUTES": 60, "HOURS": 3600, "DAYS": 86400}

CLUSTER_SIZES = ("EXTRA_SMALL", "SMALL", "MEDIUM", "LARGE")

# KPI scopes by the command that owns them, as on the tenant: the flow's
# from get-flow-configuration-metadata-in-deployment and written with
# update-flow-in-deployment, the deployment's from get-deployment-
# configuration-metadata and written with update-deployment.
FLOW_SCOPES = ("NIFI_CONNECTION", "NIFI_PROCESSOR")
DEPLOYMENT_SCOPES = ("NIFI_FLOW", "SYSTEM")


class MockError(Exception):
    """An API refusal; `status` and `code` mirror the CDP error fields."""

    def __init__(self, message: str, status: int = 400, code: str = "INVALID_ARGUMENT"):
        super().__init__(message)
        self.status = status
        self.code = code


def _crn(kind: str, resource: str) -> str:
    return f"crn:cdp:df:{REGION}:{ACCOUNT}:{kind}:{resource}"


def service_crn() -> str:
    return config.MOCK_SERVICE_CRN


def environment_crn() -> str:
    return config.MOCK_ENVIRONMENT_CRN


def parameters_of(groups: list) -> dict:
    """Parameter groups -> {name: value}, the way the flow resolves #{name}."""

    flat = {}

    for group in groups or []:
        for parameter in group.get("parameters") or []:
            flat[parameter["name"]] = parameter.get("value")

    return flat


def groups_from_definition(definition: dict) -> list:
    """A definition's parameter contexts as CDF's parameter groups."""

    groups = []

    for name, context in sorted((definition.get("parameterContexts") or {}).items()):
        groups.append({
            "name": name,
            "parameters": [
                {
                    "name": p["name"],
                    "value": p.get("value"),
                    "sensitive": bool(p.get("sensitive")),
                    "description": p.get("description") or "",
                    "type": "TEXT",
                }
                for p in context.get("parameters") or []
            ],
        })

    return groups


def has_listen_components(definition: dict) -> bool:
    for _, group in walk_groups(definition["flowContents"]):
        for processor in group.get("processors") or []:
            if processor.get("type", "").endswith(".ListenHTTP"):
                return True

    return False


def tolerance_seconds(alert: dict) -> float:
    tolerance = alert.get("frequencyTolerance") or {}
    unit = ((tolerance.get("unit") or {}).get("id") or "MINUTES").upper()

    return float(tolerance.get("value") or 0) * TOLERANCE_UNITS.get(unit, 60)


class World:
    def __init__(self, path: str | None = None, speed: float | None = None):
        self.path = path or config.MOCK_WORLD_FILE
        self.speed = speed or config.MOCK_SPEED
        self.lock = threading.RLock()
        self._depth = 0
        self.sims = {}
        self._read_mtime = None
        self._saved_wall = 0.0
        self._held = None           # the sim time the clock is stopped at, inside held()
        self.stepped = False        # the clock moves only by skip(), see now()
        self._fresh()
        self._load()

    # -----------------------------------------------------------------
    # Clock and persistence
    # -----------------------------------------------------------------

    def _fresh(self, epoch: float | None = None) -> None:
        wall = time.time()
        self.data = {"format": 1, "wall_epoch": wall, "sim_epoch": wall if epoch is None else epoch,
                     "flows": {}, "deployments": {}}
        self.sims = {}

    def now(self) -> float:
        """
        Sim time. On a stepped clock (`stepped`, the replay's) no wall time
        reaches it: it moves only by skip(), and by MockClient.sleep, so
        what a run sees doesn't hang on how long its polls and LLM calls
        take, and the same run sees the same flows every time (NFH-84).
        """

        if self._held is not None:
            return self._held

        if self.stepped:
            return self.data["sim_epoch"]

        return self.data["sim_epoch"] + (time.time() - self.data["wall_epoch"]) * self.speed

    def _load(self) -> None:
        try:
            mtime = os.path.getmtime(self.path)

        except OSError:
            return

        if mtime == self._read_mtime:
            return

        with open(self.path) as handle:
            saved = json.load(handle)

        sims = saved.pop("sims", {})
        self.data = saved
        self.sims = {}

        for crn, state in sims.items():
            deployment = self.data["deployments"].get(crn)

            if deployment:
                self.sims[crn] = Simulation.from_dict(
                    state, self._definition(deployment["flow_version_crn"]),
                    parameters_of(deployment["parameter_groups"]),
                    deployment["cluster_size"], deployment["nodes"],
                )

        self._read_mtime = mtime

    def save(self) -> None:
        with self.lock:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            payload = dict(self.data)
            payload["sims"] = {crn: sim.to_dict() for crn, sim in self.sims.items()}
            tmp = f"{self.path}.{os.getpid()}.tmp"

            with open(tmp, "w") as handle:
                json.dump(payload, handle, separators=(",", ":"))

            os.replace(tmp, self.path)
            self._read_mtime = os.path.getmtime(self.path)
            self._saved_wall = time.time()

    @contextlib.contextmanager
    def _exclusive(self):
        """
        The thread lock, plus an flock on a sidecar file so the app and a
        provision script run by hand cannot interleave load and save.
        Re-entrant: settle() mutates from inside a read.
        """

        with self.lock:
            if self._depth:
                self._depth += 1

                try:
                    yield

                finally:
                    self._depth -= 1

                return

            os.makedirs(os.path.dirname(self.path), exist_ok=True)

            with open(f"{self.path}.lock", "a") as handle:
                fcntl.flock(handle, fcntl.LOCK_EX)
                self._depth = 1

                try:
                    yield

                finally:
                    self._depth = 0
                    fcntl.flock(handle, fcntl.LOCK_UN)

    @contextlib.contextmanager
    def reading(self):
        """Current state for a read: reload if another process wrote, then advance."""

        with self._exclusive():
            self._load()
            self.advance()

            yield self

            if time.time() - self._saved_wall > SAVE_EVERY:
                self.save()

    @contextlib.contextmanager
    def mutating(self):
        with self._exclusive():
            self._load()
            self.advance()

            yield self

            self.save()

    def reset(self, epoch: float | None = None) -> None:
        """Start over with no flows or deployments, the clock at `epoch` (sim time), or now."""

        with self.lock:
            self._fresh(epoch)

            with contextlib.suppress(OSError):
                os.remove(self.path)

            self._read_mtime = None

    # -----------------------------------------------------------------
    # Time passing
    # -----------------------------------------------------------------

    def advance(self) -> None:
        now = self.now()

        for crn, deployment in self.data["deployments"].items():
            sim = self.sims.get(crn)

            if sim and now - sim.t > MAX_CATCH_UP:
                sim.t = now - MAX_CATCH_UP

            for change in sorted(deployment["pending"], key=lambda c: c["apply_at"]):
                if change["apply_at"] > now:
                    break

                if sim:
                    sim.advance(change["apply_at"])

                self._apply(deployment, change)
                deployment["pending"].remove(change)
                sim = self.sims.get(crn)

            if sim:
                sim.advance(now)

    def _apply(self, deployment: dict, change: dict) -> None:
        kind = change["kind"]
        crn = deployment["crn"]
        at = change["apply_at"]

        if kind == "deploy":
            self._event(deployment, "Deployment Successful", at)
            self.sims[crn] = Simulation(
                self._definition(deployment["flow_version_crn"]),
                parameters_of(deployment["parameter_groups"]),
                deployment["cluster_size"], deployment["nodes"], start=at,
            )

        elif kind == "version":
            deployment["flow_version_crn"] = change["flow_version_crn"]
            self._event(deployment, "Change Flow Version Completed", at)

            if change.get("empty_queues"):
                for queue in self.sims[crn].queues.values():
                    queue.clear()

        elif kind == "configuration":
            if deployment["parameters_dirty"]:
                self._event(deployment, "Parameters Updated", at)

            if deployment["kpis_dirty"]:
                self._event(deployment, "Alert Rules Updated", at)

            deployment["kpis_dirty"] = False
            deployment["parameters_dirty"] = False

        elif kind == "sizing":
            deployment["cluster_size"] = change["cluster_size"]
            deployment["nodes"] = change["nodes"]
            self._event(deployment, "Cluster Size Updated", at)

        deployment["updated"] = at

        if crn in self.sims and kind != "deploy":
            self.sims[crn].configure(
                self._definition(deployment["flow_version_crn"]),
                parameters_of(deployment["parameter_groups"]),
                deployment["cluster_size"], deployment["nodes"],
            )

    @staticmethod
    def _event(deployment: dict, name: str, at: float) -> None:
        """
        A status event, as list-deployment-events reports it: what changed,
        not the values. Only the newest MAX_EVENTS are kept, numbered on
        from the last so a dropped event's crn is never reused.
        """

        events = deployment.setdefault("events", [])
        deployment["event_seq"] = deployment.get("event_seq", len(events)) + 1
        events.append({"crn": f"{deployment['crn']}/event/{deployment['event_seq']}", "name": name, "t": at})
        del events[:-MAX_EVENTS]

    def settle(self, deployment_crn: str) -> None:
        """Apply every pending change now. Mock bootstrap and tests only."""

        with self.mutating():
            for change in self._deployment(deployment_crn)["pending"]:
                change["apply_at"] = min(change["apply_at"], self.now())

            self.advance()

    def skip(self, seconds: float) -> None:
        """
        Move the shared clock forward `seconds`, simulating the gap. Every
        process sees the jump, since the epochs live in the world file.
        Mock-only: tests and the demo's fast-forward button.
        """

        with self.mutating():
            self.data["sim_epoch"] += min(seconds, MAX_CATCH_UP)
            self.advance()

    @contextlib.contextmanager
    def held(self):
        """
        Stop this process's clock for the block: the wall time spent in it
        never reaches the simulation. Mock-only: the replay, while the LLM
        reads incidents, which on the mock's speed would otherwise run the
        flows on for minutes with nothing sampling them. Nothing in the
        block may wait for the mock (a pending change would never apply).
        """

        with self.mutating():
            self._held = self.now()

        try:
            yield

        finally:
            with self.mutating():
                self.data["sim_epoch"], self.data["wall_epoch"] = self._held, time.time()
                self._held = None

    def backfill(self, deployment_crn: str, seconds: float) -> None:
        """
        Give a new deployment `seconds` of history, as if it had been
        running that long. Mock bootstrap only: a fresh live deployment
        has an empty chart, which would make a demo wait an hour.
        """

        with self.mutating():
            deployment = self._deployment(deployment_crn)

            if deployment["pending"]:
                raise MockError("the deployment still has changes in flight", 409, "CONFLICT")

            start = self.now() - seconds
            sim = Simulation(
                self._definition(deployment["flow_version_crn"]),
                parameters_of(deployment["parameter_groups"]),
                deployment["cluster_size"], deployment["nodes"], start=start,
            )
            sim.advance(self.now())
            self.sims[deployment_crn] = sim
            deployment["created"] = min(deployment["created"], start)

            for event in deployment.get("events") or []:
                event["t"] = min(event["t"], start)

            for kpi in deployment["kpis"]:
                kpi["_created"] = min(kpi.get("_created", start), start)

    # -----------------------------------------------------------------
    # Lookups
    # -----------------------------------------------------------------

    def _definition(self, flow_version_crn: str) -> dict:
        return self.version(flow_version_crn)["definition"]

    def version(self, flow_version_crn: str) -> dict:
        flow_crn = flow_version_crn.rsplit("/", 1)[0]
        flow = self.data["flows"].get(flow_crn)

        for version in (flow or {}).get("versions") or []:
            if version["crn"] == flow_version_crn:
                return version

        raise MockError(f"Flow version {flow_version_crn} not found.", 404, "NOT_FOUND")

    def flow(self, flow_crn: str) -> dict:
        flow = self.data["flows"].get(flow_crn)

        if not flow:
            raise MockError(f"Flow definition {flow_crn} not found.", 404, "NOT_FOUND")

        return flow

    def _deployment(self, deployment_crn: str) -> dict:
        deployment = self.data["deployments"].get(deployment_crn)

        if not deployment:
            raise MockError(f"Deployment {deployment_crn} not found.", 404, "NOT_FOUND")

        return deployment

    def deployment(self, deployment_crn: str, deployed_flow_crn: str | None = None) -> dict:
        deployment = self._deployment(deployment_crn)

        if deployed_flow_crn is not None and deployed_flow_crn != deployment["deployed_flow_crn"]:
            raise MockError(f"Deployed flow {deployed_flow_crn} not found in the deployment.",
                            404, "NOT_FOUND")

        return deployment

    def deployments(self) -> list:
        return list(self.data["deployments"].values())

    def flows(self, search_term: str = "") -> list:
        term = (search_term or "").lower()

        return [f for f in self.data["flows"].values() if term in f["name"].lower()]

    def sim(self, deployment_crn: str):
        return self.sims.get(deployment_crn)

    def pending(self, deployment: dict, kind: str):
        return next((c for c in deployment["pending"] if c["kind"] == kind), None)

    # -----------------------------------------------------------------
    # Catalog
    # -----------------------------------------------------------------

    def _new_version(self, flow: dict, definition: dict, comments: str) -> dict:
        number = len(flow["versions"]) + 1
        version = {
            "crn": f"{flow['crn']}/v.{number}",
            "version": number,
            "comments": comments or "",
            "author": "mock",
            "timestamp": self.now(),
            "tags": [],
            "definition": definition,
        }
        flow["versions"].append(version)
        flow["modified"] = version["timestamp"]

        return version

    def import_flow(self, name: str, definition: dict, description: str = "",
                    comments: str = "") -> dict:
        with self.mutating():
            if any(f["name"] == name for f in self.data["flows"].values()):
                raise MockError(f"A flow definition named {name} already exists.", 409, "ALREADY_EXISTS")

            _check_definition(definition)
            crn = _crn("flow", name)
            flow = {"crn": crn, "name": name, "description": description or "",
                    "created": self.now(), "modified": self.now(), "versions": []}
            self.data["flows"][crn] = flow
            self._new_version(flow, definition, comments)

            return flow

    def import_version(self, flow_crn: str, definition: dict, comments: str = "") -> dict:
        with self.mutating():
            _check_definition(definition)

            return self._new_version(self.flow(flow_crn), definition, comments)

    def delete_flow(self, flow_crn: str) -> dict:
        with self.mutating():
            flow = self.flow(flow_crn)

            for deployment in self.data["deployments"].values():
                if deployment["flow_crn"] == flow_crn:
                    raise MockError(f"Flow {flow['name']} is used by deployment "
                                    f"{deployment['name']}.", 409, "CONFLICT")

            return self.data["flows"].pop(flow_crn)

    # -----------------------------------------------------------------
    # Deployments
    # -----------------------------------------------------------------

    def create_deployment(self, service: str, flow_version_crn: str, name: str,
                          cluster_size: str = "EXTRA_SMALL", nodes: int = 1,
                          nifi_version: str = "", parameter_groups: list | None = None,
                          kpis: list | None = None, ignore_inbound: bool = False) -> dict:
        with self.mutating():
            if service != service_crn():
                raise MockError(f"Service {service} not found.", 404, "NOT_FOUND")

            if any(d["name"] == name for d in self.data["deployments"].values()):
                raise MockError(f"A deployment named {name} already exists.", 409, "ALREADY_EXISTS")

            if cluster_size not in CLUSTER_SIZES:
                raise MockError(f"Unknown cluster size {cluster_size}.")

            if nifi_version and nifi_version != config.NIFI_VERSION:
                raise MockError(f"NiFi version {nifi_version} is not available.")

            version = self.version(flow_version_crn)
            definition = version["definition"]

            if has_listen_components(definition) and not ignore_inbound:
                raise MockError(_INBOUND_MESSAGE)

            groups = groups_from_definition(definition)
            given = {g.get("name") for g in parameter_groups or [] if isinstance(g, dict)}

            # As live: the definition's values are not enough on create.
            for group in groups:
                if group["name"] not in given:
                    raise MockError(f"Missing expected parameter group '{group['name']}' in configuration")

            _merge_parameters(groups, parameter_groups or [])
            ident = uuid.uuid5(uuid.NAMESPACE_URL, f"{name}/{self.now()}")
            crn = _crn("deployment", f"nfh-mock/{ident}")
            now = self.now()
            deployment = {
                "crn": crn,
                "name": name,
                "service_crn": service,
                "flow_crn": flow_version_crn.rsplit("/", 1)[0],
                "flow_version_crn": flow_version_crn,
                "deployed_flow_crn": _crn("deployedFlow", str(uuid.uuid5(ident, "flow"))),
                "cluster_size": cluster_size,
                "nodes": max(1, int(nodes)),
                "nifi_version": nifi_version or config.NIFI_VERSION,
                "created": now,
                "updated": now,
                "configuration_version": 1,
                "deployment_configuration_version": 1,
                "parameter_groups": groups,
                "kpis": [],
                "kpis_dirty": False,
                "parameters_dirty": False,
                "pending": [{"kind": "deploy", "apply_at": now + DEPLOY_SECONDS}],
            }

            if kpis:
                deployment["kpis"] = self._checked_kpis(deployment, kpis,
                                                        FLOW_SCOPES + DEPLOYMENT_SCOPES)

            self._event(deployment, "Deployment Initiated", now)
            self.data["deployments"][crn] = deployment

            return deployment

    def terminate_deployment(self, deployment_crn: str) -> dict:
        with self.mutating():
            deployment = self.data["deployments"].pop(self._deployment(deployment_crn)["crn"])
            self.sims.pop(deployment_crn, None)

            return deployment

    def change_flow_version(self, deployment_crn: str, deployed_flow_crn: str,
                            flow_version_crn: str, strategy: str = "STOP_AND_PROCESS_DATA",
                            ignore_inbound: bool = False) -> dict:
        with self.mutating():
            deployment = self.deployment(deployment_crn, deployed_flow_crn)
            self._require_settled(deployment)
            version = self.version(flow_version_crn)

            if version["crn"].rsplit("/", 1)[0] != deployment["flow_crn"]:
                raise MockError("The flow version belongs to a different flow definition.")

            if version["crn"] == deployment["flow_version_crn"]:
                raise MockError("The deployment already runs this flow version.")

            if has_listen_components(version["definition"]) and not ignore_inbound:
                raise MockError(_INBOUND_MESSAGE)

            change = {"kind": "version", "apply_at": self.now() + VERSION_SECONDS,
                      "flow_version_crn": version["crn"], "strategy": strategy,
                      "empty_queues": strategy == "STOP_AND_EMPTY_QUEUES"}
            deployment["pending"].append(change)
            self._event(deployment, "Change Flow Version Initiated", self.now())

            return change

    def update_flow_configuration(self, deployment_crn: str, deployed_flow_crn: str,
                                  configuration_version: int, kpis: list | None = None,
                                  parameter_groups: list | None = None) -> dict:
        with self.mutating():
            deployment = self.deployment(deployment_crn, deployed_flow_crn)
            self._require_settled(deployment, allow=("configuration",))

            if int(configuration_version) != deployment["configuration_version"]:
                raise MockError(
                    f"Configuration version {configuration_version} is stale; the current "
                    f"version is {deployment['configuration_version']}.", 409, "CONFLICT",
                )

            if kpis is not None:
                deployment["kpis"] = self._checked_kpis(deployment, kpis, FLOW_SCOPES)
                deployment["kpis_dirty"] = True

            if parameter_groups is not None:
                groups = copy.deepcopy(deployment["parameter_groups"])
                _merge_parameters(groups, parameter_groups)
                deployment["parameter_groups"] = groups
                deployment["parameters_dirty"] = True

            deployment["configuration_version"] += 1
            deployment["pending"] = [c for c in deployment["pending"] if c["kind"] != "configuration"]
            deployment["pending"].append({"kind": "configuration",
                                          "apply_at": self.now() + CONFIGURATION_SECONDS})

            return deployment

    def update_deployment(self, deployment_crn: str, configuration_version: int,
                          cluster_size: str | None = None, nodes: int | None = None,
                          kpis: list | None = None) -> dict:
        with self.mutating():
            deployment = self._deployment(deployment_crn)
            self._require_settled(deployment)

            if int(configuration_version) != deployment["deployment_configuration_version"]:
                raise MockError(
                    f"Configuration version {configuration_version} is stale; the current "
                    f"version is {deployment['deployment_configuration_version']}.", 409, "CONFLICT",
                )

            size = cluster_size or deployment["cluster_size"]

            if size not in CLUSTER_SIZES:
                raise MockError(f"Unknown cluster size {size}.")

            count = int(nodes or deployment["nodes"])

            if not 1 <= count <= 32:
                raise MockError("staticNodeCount must be between 1 and 32.")

            if kpis is not None:
                deployment["kpis"] = self._checked_kpis(deployment, kpis, DEPLOYMENT_SCOPES)
                deployment["kpis_dirty"] = True
                deployment["pending"].append({"kind": "configuration",
                                              "apply_at": self.now() + CONFIGURATION_SECONDS})

            deployment["deployment_configuration_version"] += 1

            # KPIs alone restart nothing.
            if cluster_size is None and nodes is None:
                return deployment

            apply_at = self.now() + SIZING_SECONDS
            deployment["pending"].append({"kind": "sizing", "apply_at": apply_at,
                                          "cluster_size": size, "nodes": count})
            sim = self.sims.get(deployment_crn)

            # Nodes restart: nothing is processed until the new size is up.
            if sim:
                sim.pause_until = apply_at

            return deployment

    def _require_settled(self, deployment: dict, allow: tuple = ()) -> None:
        busy = [c["kind"] for c in deployment["pending"] if c["kind"] not in allow]

        if busy:
            raise MockError(
                f"The deployment is busy ({', '.join(busy)} in progress); try again when it "
                "is steady.", 409, "CONFLICT",
            )

    # -----------------------------------------------------------------
    # KPIs
    # -----------------------------------------------------------------

    def kpi_metadata(self, deployment: dict, scope_types: tuple = FLOW_SCOPES) -> dict:
        definition = self._definition(deployment["flow_version_crn"])
        root = definition["flowContents"]
        scopes = []

        for scope in scope_types:
            metric_types = [
                {"id": m.metric_id, "label": m.label, "unitTypeKey": m.unit_id,
                 "defaultUnitId": m.unit_id}
                for m in metrics.table().values() if m.component_type == scope
            ]
            groups = []

            if scope in FLOW_SCOPES:
                for ancestry, group in _groups_with_ids(root):
                    items = group.get("connections" if scope == "NIFI_CONNECTION" else "processors") or []
                    components = [{"id": f"{ancestry};{item['identifier']}",
                                   "name": item.get("name") or ""} for item in items]

                    if components:
                        groups.append({"id": ancestry, "name": group.get("name") or "",
                                       "scopeComponents": components})

            scopes.append({"type": scope, "metricTypes": metric_types, "contextGroups": groups})

        return {"kpiMetaData": {"kpiScopes": scopes}}

    def _checked_kpis(self, deployment: dict, kpis: list, scope_types: tuple) -> list:
        """
        The deployment's KPI list with those in `scope_types` replaced by
        `kpis`; the other command's KPIs are kept, as on the tenant.
        """

        known = {}

        for scope in self.kpi_metadata(deployment, scope_types)["kpiMetaData"]["kpiScopes"]:
            known[scope["type"]] = {
                "metrics": {m["id"] for m in scope["metricTypes"]},
                "components": {c["id"] for g in scope["contextGroups"] for c in g["scopeComponents"]},
            }

        previous = {k.get("id"): k for k in deployment["kpis"]}
        checked = [k for k in deployment["kpis"] if k.get("metricComponentType") not in scope_types]

        for raw in kpis:
            if not isinstance(raw, dict):
                raise MockError("Each KPI must be an object.")

            scope = raw.get("metricComponentType")

            if scope not in known:
                raise MockError(f"metricComponentType {scope!r} is not allowed here; "
                                f"this command takes {', '.join(scope_types)}.")

            if raw.get("metricId") not in known[scope]["metrics"]:
                raise MockError(f"Metric {raw.get('metricId')!r} is not available for {scope}.")

            if scope in FLOW_SCOPES and raw.get("componentId") not in known[scope]["components"]:
                raise MockError(f"Component {raw.get('componentId')!r} not found for {scope}.")

            alert = raw.get("alert")

            if alert is not None:
                unit = ((alert.get("frequencyTolerance") or {}).get("unit") or {}).get("id")

                if not unit:
                    raise MockError("alert.frequencyTolerance.unit.id is required.")

            kpi = {k: v for k, v in raw.items() if not k.startswith("_")}
            kpi_id = kpi.get("id") if kpi.get("id") in previous else None
            kpi["id"] = kpi_id or str(uuid.uuid4())
            kpi["_created"] = previous[kpi_id]["_created"] if kpi_id else self.now()
            checked.append(kpi)

        return checked

    def component_name(self, deployment: dict, component_id: str) -> str:
        """A KPI componentId -> the component's current name ("" if gone)."""

        leaf = (component_id or "").split(";")[-1]
        definition = self._definition(deployment["flow_version_crn"])

        for _, group in walk_groups(definition["flowContents"]):
            for item in (group.get("processors") or []) + (group.get("connections") or []):
                if item["identifier"] == leaf:
                    return item.get("name") or ""

        return ""

    def kpi_key(self, deployment: dict, kpi: dict) -> str:
        scope = kpi.get("metricComponentType")
        logical = metrics.logical_for(kpi.get("metricId"))
        kind = {v: k for k, v in metrics.KIND_SCOPES.items()}.get(scope, "")

        if scope == "SYSTEM":
            return f"system||{logical}"

        return f"{kind}|{self.component_name(deployment, kpi.get('componentId'))}|{logical}"

    def chart_data(self, deployment: dict, key: str, period: str, since: float = 0.0) -> list:
        sim = self.sims.get(deployment["crn"])

        if not sim:
            return []

        cutoff = since * 1000

        return [d for d in sim.chart(key, period if period in PERIODS else "LAST_THIRTY_MINUTES",
                                     self.now())
                if d["timestamp"] >= cutoff]

    def alerts(self, deployment: dict) -> list:
        """[(kpi, first occurrence)] for every KPI breaching past its tolerance."""

        sim = self.sims.get(deployment["crn"])

        if not sim:
            return []

        now = self.now()
        firing = []

        for kpi in deployment["kpis"]:
            alert = kpi.get("alert")

            if not alert:
                continue

            above = (alert.get("thresholdMoreThan") or {}).get("value")
            below = (alert.get("thresholdLessThan") or {}).get("value")

            if above is None and below is None:
                continue

            since = sim.breach_since(self.kpi_key(deployment, kpi), above, below)

            if since is None:
                continue

            since = max(since, kpi.get("_created", since))
            tolerance = tolerance_seconds(alert)

            if now - since >= tolerance:
                firing.append((kpi, since + tolerance))

        return firing

    def state(self, deployment: dict) -> tuple:
        """(state, detailedState, message)."""

        kinds = {c["kind"] for c in deployment["pending"]}

        if "deploy" in kinds:
            return "DEPLOYING", "DEPLOYING_FLOW", "The flow is being deployed."

        if "sizing" in kinds:
            return "UPDATING", "UPDATING_DEPLOYMENT", "Nodes are being resized."

        sim = self.sims.get(deployment["crn"])
        problems = sim.model.problems if sim else []

        if problems:
            return ("CONCERNING_HEALTH", "RUNNING",
                    f"{len(problems)} component(s) invalid: " + "; ".join(problems[:3]))

        if self.alerts(deployment):
            return "CONCERNING_HEALTH", "RUNNING", "One or more KPI alerts are active."

        return "GOOD_HEALTH", "RUNNING", ""


_INBOUND_MESSAGE = (
    "The flow contains listen components but the deployment has no inbound connection "
    "endpoint. Configure one, or pass --ignore-deployment-inbound-configuration-checks."
)


def _groups_with_ids(group: dict, ancestry: str = ""):
    """
    Yield (ancestry, group): the semicolon-joined process-group ids from
    the root down, which prefixes every KPI componentId.
    """

    here = f"{ancestry};{group['identifier']}" if ancestry else group["identifier"]

    yield here, group

    for child in group.get("processGroups") or []:
        yield from _groups_with_ids(child, here)


def _check_definition(definition) -> None:
    if not isinstance(definition, dict) or "flowContents" not in definition:
        raise MockError("The file is not a NiFi flow definition (no flowContents).")

    # Compiling also proves every connection end exists.
    ids = set()

    for _, group in walk_groups(definition["flowContents"]):
        for key in ("processors", "inputPorts", "outputPorts", "funnels"):
            ids.update(item["identifier"] for item in group.get(key) or [])

    for _, _, connection in iter_connections(definition):
        for end in ("source", "destination"):
            if connection[end]["id"] not in ids:
                raise MockError(f"Connection {connection.get('name')!r} has a dangling {end}.")


def _merge_parameters(groups: list, updates: list) -> None:
    """Apply --parameter-groups to the deployment's groups, in place."""

    by_name = {g["name"]: g for g in groups}

    for update in updates:
        if not isinstance(update, dict) or update.get("name") not in by_name:
            raise MockError(f"Parameter group {update.get('name') if isinstance(update, dict) else update!r} "
                            "does not exist in this flow.", 404, "NOT_FOUND")

        parameters = {p["name"]: p for p in by_name[update["name"]]["parameters"]}

        for item in update.get("parameters") or []:
            if item.get("name") not in parameters:
                raise MockError(f"Parameter {item.get('name')!r} does not exist in group "
                                f"{update['name']}.", 404, "NOT_FOUND")

            parameters[item["name"]]["value"] = item.get("value")


_WORLD = None
_WORLD_LOCK = threading.Lock()


def get_world() -> World:
    global _WORLD

    with _WORLD_LOCK:
        if _WORLD is None:
            _WORLD = World()

        return _WORLD
