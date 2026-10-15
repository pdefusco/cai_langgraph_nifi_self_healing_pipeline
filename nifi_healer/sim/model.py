"""
A flow definition, compiled into what the simulator steps.

The simulator reads the same versioned-flow JSON CDF imports, so a fix
the agent makes (a new catalog version) changes the simulation the way
it would change NiFi: through the processor and connection fields, with
`#{parameter}` references resolved from the deployment's parameter
groups.

Validity follows NiFi: an enabled processor with an unhandled
relationship, a required input that is not connected, or a broken
property never starts. It is listed in `problems` and does not run.
"""

import json
import re
from dataclasses import dataclass, field

from flows.builder.lib import iter_connections, walk_groups
from flows.builder.processors import PROCESSORS, kind_of, relationships
from nifi_healer.sim import cron

_DURATION_UNITS = {
    "ms": 0.001, "millis": 0.001, "milliseconds": 0.001,
    "s": 1, "sec": 1, "secs": 1, "second": 1, "seconds": 1,
    "m": 60, "min": 60, "mins": 60, "minute": 60, "minutes": 60,
    "h": 3600, "hr": 3600, "hour": 3600, "hours": 3600,
    "d": 86400, "day": 86400, "days": 86400,
}

_SIZE_UNITS = {"b": 1, "kb": 1024, "mb": 1024 ** 2, "gb": 1024 ** 3, "tb": 1024 ** 4}

_PARAMETER = re.compile(r"#\{([^}]+)\}")


def parse_duration(text, default: float = 0.0) -> float:
    """'500 ms', '10 secs', '5 min' -> seconds. Unparseable -> default."""

    match = re.fullmatch(r"\s*([0-9]*\.?[0-9]+)\s*([A-Za-z]*)\s*", str(text or ""))

    if not match:
        return default

    unit = match.group(2).lower() or "s"

    if unit not in _DURATION_UNITS:
        return default

    return float(match.group(1)) * _DURATION_UNITS[unit]


def parse_size(text, default: float = 0.0) -> float:
    """'1 KB', '10 MB', '1 GB' -> bytes. Unparseable -> default."""

    match = re.fullmatch(r"\s*([0-9]*\.?[0-9]+)\s*([A-Za-z]*)\s*", str(text or ""))

    if not match:
        return default

    unit = match.group(2).lower() or "b"

    if unit not in _SIZE_UNITS:
        return default

    return float(match.group(1)) * _SIZE_UNITS[unit]


def resolve(value, parameters: dict):
    """Substitute #{name} references; an unknown one resolves to ""."""

    if not isinstance(value, str):
        return value

    return _PARAMETER.sub(lambda m: str(parameters.get(m.group(1), "")), value)


@dataclass
class Proc:
    id: str
    name: str
    kind: str
    group: str
    group_name: str
    properties: dict
    period: float
    cron: str | None             # the expression, when CRON_DRIVEN; period is then unused
    tasks: int
    enabled: bool
    execution_node: str
    auto_terminated: set
    retried: set
    retry_count: int
    backoff: str
    max_backoff: float
    penalty: float
    yield_seconds: float
    inputs: list = field(default_factory=list)       # connection ids
    outputs: dict = field(default_factory=dict)      # relationship -> [connection ids]
    problems: list = field(default_factory=list)

    @property
    def runs(self) -> bool:
        return self.enabled and not self.problems

    @property
    def is_source(self) -> bool:
        return self.kind in ("GenerateFlowFile", "ListenHTTP") or not self.inputs


@dataclass
class Conn:
    id: str
    name: str
    group: str
    source: str
    destination: str
    relationships: list
    object_threshold: float
    size_threshold: float
    expiration: float
    load_balanced: bool


@dataclass
class Model:
    processors: dict          # id -> Proc, sources first, then definition order
    connections: dict         # id -> Conn
    problems: list

    def by_name(self, name: str):
        for item in list(self.processors.values()) + list(self.connections.values()):
            if item.name == name:
                return item

        return None


def compile_flow(definition: dict, parameters: dict | None = None) -> Model:
    parameters = parameters or {}
    processors = {}
    group_names = {}

    for _, group in walk_groups(definition["flowContents"]):
        group_names[group["identifier"]] = group.get("name") or ""

        for raw in group.get("processors") or []:
            kind = kind_of(raw)
            processors[raw["identifier"]] = Proc(
                id=raw["identifier"],
                name=raw.get("name") or "",
                kind=kind,
                group=group["identifier"],
                group_name=group.get("name") or "",
                properties={k: resolve(v, parameters) for k, v in (raw.get("properties") or {}).items()},
                period=parse_duration(raw.get("schedulingPeriod"), 1.0),
                cron=(raw.get("schedulingPeriod") or "") if raw.get("schedulingStrategy") == "CRON_DRIVEN" else None,
                tasks=max(1, int(raw.get("concurrentlySchedulableTaskCount") or 1)),
                enabled=raw.get("scheduledState") != "DISABLED",
                execution_node=raw.get("executionNode") or "ALL",
                auto_terminated=set(raw.get("autoTerminatedRelationships") or []),
                retried=set(raw.get("retriedRelationships") or []),
                retry_count=int(raw.get("retryCount") or 0),
                backoff=raw.get("backoffMechanism") or "PENALIZE_FLOWFILE",
                max_backoff=parse_duration(raw.get("maxBackoffPeriod"), 600.0),
                penalty=parse_duration(raw.get("penaltyDuration"), 30.0),
                yield_seconds=parse_duration(raw.get("yieldDuration"), 1.0),
            )

    connections = {}

    for _, group, raw in iter_connections(definition):
        conn = Conn(
            id=raw["identifier"],
            name=raw.get("name") or "",
            group=group["identifier"],
            source=raw["source"]["id"],
            destination=raw["destination"]["id"],
            relationships=list(raw.get("selectedRelationships") or []),
            object_threshold=float(raw.get("backPressureObjectThreshold") or 0) or float("inf"),
            size_threshold=parse_size(raw.get("backPressureDataSizeThreshold"), 0) or float("inf"),
            expiration=parse_duration(raw.get("flowFileExpiration"), 0.0),
            load_balanced=(raw.get("loadBalanceStrategy") or "DO_NOT_LOAD_BALANCE") != "DO_NOT_LOAD_BALANCE",
        )
        connections[conn.id] = conn

        if conn.destination in processors:
            processors[conn.destination].inputs.append(conn.id)

        if conn.source in processors:
            for rel in conn.relationships:
                processors[conn.source].outputs.setdefault(rel, []).append(conn.id)

    problems = []

    for proc in processors.values():
        _validate(proc, processors)
        problems.extend(f"{proc.group_name}/{proc.name}: {p}" for p in proc.problems)

    ordered = sorted(processors.values(), key=lambda p: 0 if p.is_source else 1)

    return Model({p.id: p for p in ordered}, connections, problems)


def _validate(proc: Proc, processors: dict) -> None:
    spec = PROCESSORS.get(proc.kind)

    if spec is None:
        proc.problems.append(f"unknown processor type {proc.kind}")
        return

    if not proc.enabled:
        return

    for rel in relationships(proc.kind, proc.properties):
        if rel not in proc.outputs and rel not in proc.auto_terminated:
            proc.problems.append(f"relationship {rel!r} is not connected or auto-terminated")

    if spec.input_required and not proc.inputs:
        proc.problems.append("requires an upstream connection")

    if proc.kind == "EvaluateJsonPath" and proc.properties.get("Destination") == "flowfile-content":
        if len(dynamic_properties(proc)) != 1:
            proc.problems.append("Destination flowfile-content needs exactly one JSON path")

    if proc.kind == "InvokeHTTP" and not (proc.properties.get("HTTP URL") or "").strip():
        proc.problems.append("HTTP URL is empty")

    if proc.cron is not None:
        try:
            cron.parse(proc.cron)

        except ValueError as error:
            proc.problems.append(f"CRON schedule: {error}")

    if proc.kind == "ControlRate":
        try:
            control_rate(proc)

        except ValueError as error:
            proc.problems.append(str(error))


def control_rate(proc: Proc) -> tuple:
    """
    ControlRate's limits: (FlowFiles, bytes) per Time Duration, None where
    the criteria do not limit it. ValueError when a needed rate is invalid.
    """

    criteria = (proc.properties.get("Rate Control Criteria") or "data rate").strip().lower()
    count = size = None

    def rate(key, parse):
        value = parse(proc.properties.get(key))

        if value <= 0:
            raise ValueError(f"{key} must be a positive {'size' if parse is _size else 'number'}")

        return value

    if criteria == "flowfile count":
        count = rate("Maximum Rate", _number)

    elif criteria == "data rate":
        size = rate("Maximum Rate", _size)

    elif criteria == "data rate or flowfile count":
        count = rate("Maximum FlowFile Rate", _number)
        size = rate("Maximum Data Rate", _size)

    return count, size


def _number(text) -> float:
    try:
        return float(str(text or "").strip())

    except ValueError:
        return 0.0


def _size(text) -> float:
    return parse_size(text, 0.0)


def dynamic_properties(proc: Proc) -> dict:
    spec = PROCESSORS.get(proc.kind)
    known = spec.properties if spec else {}

    return {k: v for k, v in proc.properties.items() if k not in known}


def json_path_lookup(document, path: str):
    """
    A dotted JSON path ($.a.b) against a parsed document. Returns
    (found, value). Only the subset the scenarios use.
    """

    path = (path or "").strip()

    if not path.startswith("$"):
        return False, None

    node = document

    for part in [p for p in path[1:].split(".") if p]:
        if isinstance(node, dict) and part in node:
            node = node[part]

        else:
            return False, None

    return True, node


def parse_json(text):
    try:
        return True, json.loads(text)

    except (TypeError, ValueError):
        return False, None
