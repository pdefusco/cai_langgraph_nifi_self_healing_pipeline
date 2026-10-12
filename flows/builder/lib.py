"""
Builders for NiFi versioned flow definitions (the JSON CDF imports).

Ported from SIB self_healing/flow/build_flow_definition.py, which was
verified against a live CDF tenant on NiFi 2.6.0, and generalised:

  * a pack is a root process group with one child group per scenario
  * every connection and processor is created with an explicit name,
    because a KPI chart identifies its component by name only
  * load balancing, expiration, retry and backoff are parameters, since
    the scenarios inject faults through exactly those fields
  * parameter contexts, for the parameter scenarios

Identifiers are uuid5 of a stable label, so rebuilding a pack produces
byte-identical JSON and a diff between two builds is the real change.
They are not the ids a deployment uses: NiFi assigns its own at deploy
time, which is why KPI component ids are discovered after deployment
(provision/03_discover_kpi_metadata.py).
"""

import uuid

import config
from flows.builder.processors import PROCESSORS, spec_for

NAMESPACE = uuid.UUID("6f3c1e2a-9b4d-5f8e-a1c7-2d0b4e6f8a13")

LOAD_BALANCE_STRATEGIES = {
    "DO_NOT_LOAD_BALANCE", "ROUND_ROBIN", "SINGLE_NODE", "PARTITION_BY_ATTRIBUTE",
}

BACKOFF_MECHANISMS = {"PENALIZE_FLOWFILE", "YIELD_PROCESSOR"}

SCHEDULED_STATES = {"ENABLED", "DISABLED"}

# ALL runs a source on every node, so a two-node cluster generates twice
# the data; PRIMARY runs it on one, and only a load-balanced connection
# spreads the work after it.
EXECUTION_NODES = {"ALL", "PRIMARY"}

COLUMN_SPACING = 420.0
ROW_SPACING = 200.0


def component_id(label: str) -> str:
    return str(uuid.uuid5(NAMESPACE, label))


def bundle(artifact: str) -> dict:
    return {"artifact": artifact, "group": "org.apache.nifi", "version": config.NIFI_VERSION}


class ProcessGroup:
    """
    One process group, built up with `processor()`, `connect()` and
    `group()`, then serialised with `to_dict()`.

    Processors are referred to by a short `key` unique within the group
    ("gen", "sink"); the key only feeds the deterministic id.
    """

    def __init__(self, label: str, name: str, parent: "ProcessGroup | None" = None,
                 comments: str = "", parameter_context: str | None = None,
                 position: tuple = (0.0, 0.0)):
        self.label = label
        self.name = name
        self.identifier = component_id(f"pg:{label}")
        self.parent = parent
        self.comments = comments
        self.parameter_context = parameter_context
        self.position = position
        self.processors = {}
        self.connections = []
        self.groups = []

    # -----------------------------------------------------------------
    # Processors
    # -----------------------------------------------------------------

    def processor(self, key: str, name: str, kind: str, properties: dict | None = None, *,
                  period: str = "1 sec", tasks: int = 1, auto_terminate=(),
                  scheduled_state: str = "ENABLED", retried=(), retry_count: int = 10,
                  backoff: str = "PENALIZE_FLOWFILE", max_backoff: str = "10 mins",
                  penalty: str = "30 sec", yield_duration: str = "1 sec",
                  execution_node: str = "ALL",
                  comments: str = "", position: tuple | None = None) -> dict:
        spec = spec_for(kind)

        if key in self.processors:
            raise ValueError(f"{self.name}: duplicate processor key {key!r}")

        if backoff not in BACKOFF_MECHANISMS:
            raise ValueError(f"{name}: unknown backoff mechanism {backoff!r}")

        if scheduled_state not in SCHEDULED_STATES:
            raise ValueError(f"{name}: unknown scheduled state {scheduled_state!r}")

        if execution_node not in EXECUTION_NODES:
            raise ValueError(f"{name}: unknown execution node {execution_node!r}")

        if position is None:
            position = (0.0, len(self.processors) * ROW_SPACING)

        processor = {
            "annotationData": None,
            "autoTerminatedRelationships": sorted(auto_terminate),
            "backoffMechanism": backoff,
            "bulletinLevel": "WARN",
            "bundle": bundle(spec.artifact),
            "comments": comments,
            "componentType": "PROCESSOR",
            "concurrentlySchedulableTaskCount": int(tasks),
            "executionNode": execution_node,
            "groupIdentifier": self.identifier,
            "identifier": component_id(f"proc:{self.label}/{key}"),
            "instanceIdentifier": None,
            "maxBackoffPeriod": max_backoff,
            "name": name,
            "penaltyDuration": penalty,
            "position": {"x": float(position[0]), "y": float(position[1])},
            "properties": dict(properties or {}),
            "propertyDescriptors": {},
            "retriedRelationships": sorted(retried),
            "retryCount": int(retry_count),
            "runDurationMillis": 0,
            "scheduledState": scheduled_state,
            "schedulingPeriod": period,
            "schedulingStrategy": "TIMER_DRIVEN",
            "style": {},
            "type": spec.type,
            "yieldDuration": yield_duration,
        }

        self.processors[key] = processor

        return processor

    # -----------------------------------------------------------------
    # Connections
    # -----------------------------------------------------------------

    def endpoint(self, key: str) -> dict:
        processor = self.processors[key]

        return {
            "comments": None,
            "groupId": self.identifier,
            "id": processor["identifier"],
            "instanceIdentifier": None,
            "name": processor["name"],
            "type": "PROCESSOR",
        }

    def connect(self, source: str, destination: str, relationships, name: str = "", *,
                object_threshold: int = 10000, size_threshold: str = "1 GB",
                expiration: str = "0 sec",
                load_balance: str = "DO_NOT_LOAD_BALANCE") -> dict:
        if load_balance not in LOAD_BALANCE_STRATEGIES:
            raise ValueError(f"{name}: unknown load balance strategy {load_balance!r}")

        relationships = sorted(relationships)
        label = name or f"{source}->{destination}:{','.join(relationships)}"
        bends = []

        # A self-loop with no bends is drawn on top of its processor in
        # the NiFi canvas; two bends make it visible.
        if source == destination:
            at = self.processors[source]["position"]
            bends = [{"x": at["x"] + 420.0, "y": at["y"] - 40.0},
                     {"x": at["x"] + 420.0, "y": at["y"] + 80.0}]

        connection = {
            "backPressureDataSizeThreshold": size_threshold,
            "backPressureObjectThreshold": int(object_threshold),
            "bends": bends,
            "comments": None,
            "componentType": "CONNECTION",
            "destination": self.endpoint(destination),
            "flowFileExpiration": expiration,
            "groupIdentifier": self.identifier,
            "identifier": component_id(f"conn:{self.label}/{label}"),
            "instanceIdentifier": None,
            "labelIndex": 1,
            "loadBalanceCompression": "DO_NOT_COMPRESS",
            "loadBalanceStrategy": load_balance,
            "name": name,
            "partitioningAttribute": None,
            "position": None,
            "prioritizers": [],
            "selectedRelationships": relationships,
            "source": self.endpoint(source),
            "zIndex": 0,
        }

        self.connections.append(connection)

        return connection

    # -----------------------------------------------------------------
    # Nesting
    # -----------------------------------------------------------------

    def group(self, label: str, name: str, comments: str = "",
              parameter_context: str | None = None) -> "ProcessGroup":
        index = len(self.groups)
        child = ProcessGroup(
            f"{self.label}/{label}", name, parent=self, comments=comments,
            parameter_context=parameter_context,
            position=((index % 4) * COLUMN_SPACING, (index // 4) * ROW_SPACING),
        )

        self.groups.append(child)

        return child

    def to_dict(self) -> dict:
        return {
            "comments": self.comments,
            "componentType": "PROCESS_GROUP",
            "connections": self.connections,
            "controllerServices": [],
            "defaultBackPressureDataSizeThreshold": "1 GB",
            "defaultBackPressureObjectThreshold": 10000,
            "defaultFlowFileExpiration": "0 sec",
            "executionEngine": None,
            "flowFileConcurrency": "UNBOUNDED",
            "flowFileOutboundPolicy": "STREAM_WHEN_AVAILABLE",
            "funnels": [],
            "groupIdentifier": self.parent.identifier if self.parent else None,
            "identifier": self.identifier,
            "inputPorts": [],
            "instanceIdentifier": None,
            "labels": [],
            "logFileSuffix": None,
            "maxConcurrentTasks": None,
            "name": self.name,
            "outputPorts": [],
            "parameterContextName": self.parameter_context,
            "position": ({"x": float(self.position[0]), "y": float(self.position[1])}
                         if self.parent else None),
            "processGroups": [g.to_dict() for g in self.groups],
            "processors": list(self.processors.values()),
            "remoteProcessGroups": [],
            "scheduledState": None,
            "statelessFlowTimeout": None,
            "versionedFlowCoordinates": None,
        }


def parameter_context(name: str, parameters: dict, description: str = "") -> dict:
    """
    A parameter context. CDF surfaces each one as a "parameter group" of
    the same name, which `update-flow-in-deployment --parameter-groups`
    then updates in place.
    """

    return {
        "componentType": "PARAMETER_CONTEXT",
        "description": description,
        "identifier": component_id(f"ctx:{name}"),
        "inheritedParameterContexts": [],
        "instanceIdentifier": None,
        "name": name,
        "parameters": [
            {
                "description": "",
                "name": key,
                "provided": False,
                "sensitive": False,
                "value": str(value),
            }
            for key, value in sorted(parameters.items())
        ],
    }


def flow_definition(root: ProcessGroup, contexts: list | None = None) -> dict:
    return {
        "externalControllerServices": {},
        "flowContents": root.to_dict(),
        "flowEncodingVersion": "1.0",
        "latest": False,
        "parameterContexts": {c["name"]: c for c in contexts or []},
        "parameterProviders": {},
    }


# ---------------------------------------------------------------------
# Walking a definition
# ---------------------------------------------------------------------


def walk_groups(group: dict, path: tuple = ()):
    """Yield (path of group names, group) for every group, depth first."""

    here = path + (group.get("name"),)

    yield here, group

    for child in group.get("processGroups") or []:
        yield from walk_groups(child, here)


def iter_processors(definition: dict):
    for path, group in walk_groups(definition["flowContents"]):
        for processor in group.get("processors") or []:
            yield path, group, processor


def iter_connections(definition: dict):
    for path, group in walk_groups(definition["flowContents"]):
        for connection in group.get("connections") or []:
            yield path, group, connection


# ---------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------


def _odd_keys(value, path: str = "") -> list:
    """Keys that are not plain ASCII, which NiFi has rejected on import."""

    found = []

    if isinstance(value, dict):
        for key, item in value.items():
            if not key.isascii() or not key.strip() == key:
                found.append(f"{path}/{key!r}")

            found.extend(_odd_keys(item, f"{path}/{key}"))

    elif isinstance(value, list):
        for index, item in enumerate(value):
            found.extend(_odd_keys(item, f"{path}[{index}]"))

    return found


def check(definition: dict) -> dict:
    """
    Structural checks that do not need NiFi.

    Returns {"problems": [...], "unverified": [...]}: problems make the
    build fail; unverified lists the property keys and relationships not
    yet confirmed against a live NiFi 2.6 (the canary import confirms them).
    """

    problems = []
    unverified = set()
    ids = {}

    for path, group in walk_groups(definition["flowContents"]):
        where = "/".join(path)
        names = {}
        local = {p["identifier"]: p for p in group.get("processors") or []}

        for item in [group] + (group.get("processors") or []) + (group.get("connections") or []):
            identifier = item.get("identifier")

            if identifier in ids and ids[identifier] is not item:
                problems.append(f"{where}: duplicate identifier {identifier}")

            ids[identifier] = item

        for processor in group.get("processors") or []:
            name = processor["name"]
            spec = PROCESSORS.get(processor["type"].rsplit(".", 1)[-1])

            if name in names:
                problems.append(f"{where}: two processors named {name!r}")

            names[name] = processor

            if processor["groupIdentifier"] != group["identifier"]:
                problems.append(f"{where}/{name}: groupIdentifier is not its group")

            if spec is None:
                problems.append(f"{where}/{name}: no spec for {processor['type']}")
                continue

            if processor["bundle"]["artifact"] != spec.artifact:
                problems.append(f"{where}/{name}: bundle artifact is not {spec.artifact}")

            for key, value in processor["properties"].items():
                if key not in spec.properties and not spec.dynamic:
                    problems.append(f"{where}/{name}: unknown property {key!r}")

                if key in spec.properties and not spec.properties[key]:
                    unverified.add(f"{spec.name}.{key}")

                if not isinstance(value, str):
                    problems.append(f"{where}/{name}: property {key!r} is not a string")

            for rel in processor["autoTerminatedRelationships"] + processor["retriedRelationships"]:
                if rel not in spec.relationships:
                    problems.append(f"{where}/{name}: unknown relationship {rel!r}")

            if not spec.verified:
                unverified.add(f"{spec.name} (bundle/type)")

        for connection in group.get("connections") or []:
            label = connection.get("name") or connection["identifier"]

            for end in ("source", "destination"):
                endpoint = connection[end]

                if endpoint["id"] not in local:
                    problems.append(f"{where}/{label}: {end} is not a processor in this group")

                elif endpoint["name"] != local[endpoint["id"]]["name"]:
                    problems.append(f"{where}/{label}: {end} name is stale")

            source = local.get(connection["source"]["id"])

            if source is not None:
                spec = PROCESSORS.get(source["type"].rsplit(".", 1)[-1])

                for rel in connection["selectedRelationships"]:
                    if spec and rel not in spec.relationships:
                        problems.append(f"{where}/{label}: {source['name']} has no {rel!r}")

                    if rel in source["autoTerminatedRelationships"]:
                        problems.append(f"{where}/{label}: {rel!r} is both connected and auto-terminated")

        # Every relationship of every enabled processor must be connected
        # or auto-terminated, or NiFi marks the processor invalid and it
        # never starts.
        for processor in group.get("processors") or []:
            if processor["scheduledState"] == "DISABLED":
                continue

            spec = PROCESSORS.get(processor["type"].rsplit(".", 1)[-1])

            if spec is None:
                continue

            connected = {
                rel
                for c in group.get("connections") or []
                if c["source"]["id"] == processor["identifier"]
                for rel in c["selectedRelationships"]
            }

            for rel in spec.relationships:
                if rel not in connected and rel not in processor["autoTerminatedRelationships"]:
                    problems.append(f"{where}/{processor['name']}: relationship {rel!r} is unhandled")

            if spec.input_required and not any(
                c["destination"]["id"] == processor["identifier"]
                for c in group.get("connections") or []
            ):
                problems.append(f"{where}/{processor['name']}: requires an upstream connection")

        context = group.get("parameterContextName")

        if context and context not in definition.get("parameterContexts", {}):
            problems.append(f"{where}: parameter context {context!r} is not defined")

    problems.extend(f"non-ASCII or padded key {k}" for k in _odd_keys(definition))

    return {"problems": problems, "unverified": sorted(unverified)}
