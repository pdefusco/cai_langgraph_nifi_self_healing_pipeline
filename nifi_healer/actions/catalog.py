"""
The actions the agent may propose, with their bounds.

A proposal is a list of steps, each a dict in the shape the scenarios
record their known fixes in:

    {"action": "SET_CONCURRENT_TASKS", "target": "S01 Consumer", "value": 4}

The LLM picks from this catalog and never writes flow JSON. Every step
is checked here (types, bounds) before anything looks at a flow, and
again by flow/edits.py against the flow itself (the target exists, the
change changes something).

Each family is one API path, and a proposal stays inside one:

    FLOW_EDIT   get-flow-version -> edit -> import-flow-definition-version
                -> change-flow-version-in-deployment
    PARAMETER   update-flow-in-deployment --parameter-groups
    SIZING      update-deployment --cluster-size --static-node-count
    NONE        nothing

restart-deployment is not here on purpose: a restart does not drain a
backlog (it measured worse), and it is the one lever that looks like it
helps every incident.
"""

import re
from dataclasses import dataclass

from nifi_healer.sim.model import parse_duration, parse_size

FLOW_EDIT = "FLOW_EDIT"
PARAMETER = "PARAMETER"
SIZING = "SIZING"
NONE = "NONE"

RISKS = ("low", "medium", "high")

CLUSTER_SIZES = ("EXTRA_SMALL", "SMALL", "MEDIUM", "LARGE")
BACKOFF_MECHANISMS = ("PENALIZE_FLOWFILE", "YIELD_PROCESSOR")

# A proposal is a small, reviewable change.
MAX_STEPS = 4

MAX_TASKS = 8
MAX_NODES = 4
MAX_RUN_SCHEDULE = 3600
MAX_EXPIRATION = 7 * 86400
MAX_PENALTY = 600
MAX_OBJECT_THRESHOLD = 1_000_000
MAX_SIZE_THRESHOLD = 10 * 1024 ** 3
MAX_RETRY_COUNT = 10
MAX_VALUE_LENGTH = 500

# NiFi's own duration and size grammar, restricted to the units the
# simulator also reads, so a value the mock accepts is one NiFi accepts.
_DURATION = re.compile(
    r"^\d+\s*(ms|millis|milliseconds|s|sec|secs|second|seconds|m|min|mins|minute|minutes"
    r"|h|hr|hour|hours|d|day|days)$")
_SIZE = re.compile(r"^\d+(\.\d+)?\s*(B|KB|MB|GB|TB)$", re.I)


@dataclass(frozen=True)
class ActionType:
    name: str
    family: str
    target: str              # processor | connection | parameter_context | deployment | none
    required: tuple
    optional: tuple
    risk: str
    description: str


CATALOG = {a.name: a for a in (
    ActionType("SET_CONCURRENT_TASKS", FLOW_EDIT, "processor", ("value",), (), "low",
               f"Concurrent tasks of a processor; value is an integer 1-{MAX_TASKS}."),
    ActionType("SET_RUN_SCHEDULE", FLOW_EDIT, "processor", ("value",), (), "low",
               "Run schedule of a timer-driven processor; value is a NiFi duration "
               "such as \"0 sec\", \"250 ms\", \"1 sec\" (at most 1 hour)."),
    ActionType("SET_BACKPRESSURE", FLOW_EDIT, "connection", (), ("object_threshold", "size_threshold"),
               "low",
               f"Back pressure of a connection: object_threshold (integer 1-{MAX_OBJECT_THRESHOLD}) "
               "and/or size_threshold (\"100 MB\", \"1 GB\"; at most 10 GB)."),
    ActionType("SET_FLOWFILE_EXPIRATION", FLOW_EDIT, "connection", ("value",), (), "medium",
               "FlowFile expiration of a connection; value is a duration, \"0 sec\" means never "
               "expire."),
    ActionType("SET_PENALTY_YIELD", FLOW_EDIT, "processor", (), ("penalty", "yield_duration"), "low",
               "Penalty duration and/or yield duration of a processor (durations, at most 10 min)."),
    ActionType("SET_RETRY_POLICY", FLOW_EDIT, "processor", (), ("backoff", "max_backoff", "retry_count"),
               "low",
               "Retry settings of a processor that retries a relationship: backoff "
               "(PENALIZE_FLOWFILE penalizes one FlowFile, YIELD_PROCESSOR stalls the whole "
               f"processor), max_backoff (duration, at most 10 min), retry_count (0-{MAX_RETRY_COUNT})."),
    ActionType("SET_PROCESSOR_PROPERTY", FLOW_EDIT, "processor", ("property", "value"), (), "medium",
               "One property of a processor, from that processor's editable list in the "
               "inventory; value is a string."),
    ActionType("ROUTE_RELATIONSHIP_TO_DLQ", FLOW_EDIT, "processor", ("relationship", "dlq"), (),
               "medium",
               "Route one relationship (or a list of them) of a processor to an existing "
               "dead-letter processor in the same process group (its name contains DLQ). "
               "Self-loops and auto-termination of those relationships are removed, and the DLQ "
               "is enabled."),
    ActionType("SET_SCHEDULED_STATE", FLOW_EDIT, "processor", ("value",), (), "medium",
               "Enable a disabled processor; value must be ENABLED."),
    ActionType("UPDATE_PARAMETER", PARAMETER, "parameter_context", ("parameter", "value"), (), "medium",
               "Change a parameter's value; target is the parameter context name. No new flow "
               "version is made."),
    ActionType("SCALE_DEPLOYMENT", SIZING, "deployment", ("cluster_size", "nodes"), (), "high",
               f"Resize the deployment: cluster_size one of {', '.join(CLUSTER_SIZES)}, nodes "
               f"1-{MAX_NODES}. Target is \"\". Restarts the NiFi nodes."),
    ActionType("NO_ACTION", NONE, "none", (), (), "low",
               "Nothing should change: the flow is healthy, or no catalog action would help. "
               "Must be the only step."),
)}

FIELDS = ("value", "property", "relationship", "dlq", "object_threshold", "size_threshold",
          "parameter", "cluster_size", "nodes", "backoff", "max_backoff", "retry_count",
          "penalty", "yield_duration")


def is_duration(text) -> bool:
    return isinstance(text, str) and bool(_DURATION.match(text.strip()))


def is_size(text) -> bool:
    return isinstance(text, str) and bool(_SIZE.match(text.strip()))


def _int(value):
    if isinstance(value, bool):
        return None

    if isinstance(value, int):
        return value

    if isinstance(value, float) and value.is_integer():
        return int(value)

    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())

    return None


def relationships_of(step: dict) -> list:
    value = step.get("relationship")
    values = value if isinstance(value, (list, tuple)) else [value]

    return sorted({str(v) for v in values if v not in (None, "")})


def normalize(step: dict) -> dict:
    """
    The step with only the fields its action reads, numbers as numbers
    and strings stripped. Unknown actions pass through for check() to
    report.
    """

    kind = str(step.get("action") or "").strip().upper()
    spec = CATALOG.get(kind)
    out = {"action": kind, "target": str(step.get("target") or "").strip()}

    if spec is None:
        return {**out, **{k: step[k] for k in FIELDS if step.get(k) is not None}}

    for name in spec.required + spec.optional:
        value = step.get(name)

        if value is None:
            continue

        if name in ("object_threshold", "nodes", "retry_count") or (
                name == "value" and kind == "SET_CONCURRENT_TASKS"):
            value = _int(value) if _int(value) is not None else value

        elif name == "relationship":
            value = relationships_of(step)
            value = value[0] if len(value) == 1 else value

        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            # A property or parameter value of 1 is the string "1".
            value = str(value)

        if isinstance(value, str):
            value = value.strip()

        out[name] = value

    if kind == "SCALE_DEPLOYMENT":
        out["target"] = ""

    if kind == "NO_ACTION":
        out["target"] = ""

    return out


def check(step: dict) -> list:
    """Problems with one (normalized) step; [] when it is in bounds."""

    kind = step.get("action")
    spec = CATALOG.get(kind)

    if spec is None:
        return [f"unknown action {kind!r}; the catalog has {', '.join(CATALOG)}"]

    problems = []
    label = f"{kind} {step.get('target')!r}" if step.get("target") else kind

    def bad(message):
        problems.append(f"{label}: {message}")

    for name in spec.required:
        if step.get(name) in (None, "", []):
            bad(f"{name} is required")

    extra = sorted(k for k in FIELDS if step.get(k) is not None
                   and k not in spec.required + spec.optional)

    if extra:
        bad(f"does not take {', '.join(extra)}")

    if spec.target in ("processor", "connection", "parameter_context") and not step.get("target"):
        bad(f"target (the {spec.target.replace('_', ' ')} name) is required")

    if problems:
        return problems

    value = step.get("value")

    if kind == "SET_CONCURRENT_TASKS":
        if not isinstance(value, int) or not 1 <= value <= MAX_TASKS:
            bad(f"value must be an integer from 1 to {MAX_TASKS}, got {value!r}")

    elif kind == "SET_RUN_SCHEDULE":
        if not is_duration(value):
            bad(f"value must be a duration like \"250 ms\" or \"1 sec\", got {value!r}")

        elif parse_duration(value) > MAX_RUN_SCHEDULE:
            bad("value must be at most 1 hour")

    elif kind == "SET_BACKPRESSURE":
        objects, size = step.get("object_threshold"), step.get("size_threshold")

        if objects is None and size is None:
            bad("give object_threshold and/or size_threshold")

        if objects is not None and (not isinstance(objects, int)
                                    or not 1 <= objects <= MAX_OBJECT_THRESHOLD):
            bad(f"object_threshold must be an integer from 1 to {MAX_OBJECT_THRESHOLD}")

        if size is not None:
            if not is_size(size):
                bad(f"size_threshold must be a size like \"1 GB\", got {size!r}")

            elif not 0 < parse_size(size) <= MAX_SIZE_THRESHOLD:
                bad("size_threshold must be above 0 and at most 10 GB")

    elif kind == "SET_FLOWFILE_EXPIRATION":
        if not is_duration(value):
            bad(f"value must be a duration like \"0 sec\", got {value!r}")

        elif parse_duration(value) > MAX_EXPIRATION:
            bad("value must be at most 7 days")

    elif kind == "SET_PENALTY_YIELD":
        given = {k: step.get(k) for k in ("penalty", "yield_duration") if step.get(k) is not None}

        if not given:
            bad("give penalty and/or yield_duration")

        for name, duration in given.items():
            if not is_duration(duration) or parse_duration(duration) > MAX_PENALTY:
                bad(f"{name} must be a duration of at most 10 min, got {duration!r}")

    elif kind == "SET_RETRY_POLICY":
        backoff, ceiling, count = step.get("backoff"), step.get("max_backoff"), step.get("retry_count")

        if backoff is None and ceiling is None and count is None:
            bad("give backoff, max_backoff and/or retry_count")

        if backoff is not None and backoff not in BACKOFF_MECHANISMS:
            bad(f"backoff must be one of {', '.join(BACKOFF_MECHANISMS)}")

        if ceiling is not None and (not is_duration(ceiling) or parse_duration(ceiling) > MAX_PENALTY):
            bad(f"max_backoff must be a duration of at most 10 min, got {ceiling!r}")

        if count is not None and (not isinstance(count, int) or not 0 <= count <= MAX_RETRY_COUNT):
            bad(f"retry_count must be an integer from 0 to {MAX_RETRY_COUNT}")

    elif kind == "SET_PROCESSOR_PROPERTY":
        if not isinstance(value, str) or len(value) > MAX_VALUE_LENGTH:
            bad(f"value must be a string of at most {MAX_VALUE_LENGTH} characters")

        elif "#{" in value:
            bad("value may not reference a parameter; change the parameter with UPDATE_PARAMETER")

    elif kind == "ROUTE_RELATIONSHIP_TO_DLQ":
        if not relationships_of(step):
            bad("relationship is required")

        if "DLQ" not in str(step.get("dlq")).upper():
            bad(f"dlq must name a dead-letter processor (its name contains DLQ), got {step.get('dlq')!r}")

        elif step.get("dlq") == step.get("target"):
            bad("a processor cannot be its own DLQ")

    elif kind == "SET_SCHEDULED_STATE":
        if value != "ENABLED":
            bad("value must be ENABLED (disabling or stopping a processor is not in the catalog)")

    elif kind == "UPDATE_PARAMETER":
        if not isinstance(value, str) or len(value) > MAX_VALUE_LENGTH:
            bad(f"value must be a string of at most {MAX_VALUE_LENGTH} characters")

    elif kind == "SCALE_DEPLOYMENT":
        size, nodes = step.get("cluster_size"), step.get("nodes")

        if size not in CLUSTER_SIZES:
            bad(f"cluster_size must be one of {', '.join(CLUSTER_SIZES)}, got {size!r}")

        if not isinstance(nodes, int) or not 1 <= nodes <= MAX_NODES:
            bad(f"nodes must be an integer from 1 to {MAX_NODES}, got {nodes!r}")

    return problems


def family_of(steps: list) -> str:
    families = {CATALOG[s["action"]].family for s in steps if s.get("action") in CATALOG}

    return families.pop() if len(families) == 1 else ""


def check_steps(steps: list) -> list:
    """Problems with a whole proposal (each step, and how they combine)."""

    if not steps:
        return ["a proposal needs at least one step"]

    problems = []

    for step in steps:
        problems.extend(check(step))

    if len(steps) > MAX_STEPS:
        problems.append(f"at most {MAX_STEPS} steps per proposal, got {len(steps)}")

    kinds = [s.get("action") for s in steps]

    if "NO_ACTION" in kinds and len(steps) > 1:
        problems.append("NO_ACTION must be the only step")

    known = [s for s in steps if s.get("action") in CATALOG]
    families = sorted({CATALOG[s["action"]].family for s in known})

    if len(families) > 1:
        problems.append(f"steps mix API paths ({', '.join(families)}); propose one family at a time")

    seen = set()

    for step in known:
        key = (step["action"], step.get("target"), step.get("property"), step.get("parameter"))

        if key in seen:
            problems.append(f"{step['action']} {step.get('target')!r} appears twice")

        seen.add(key)

    return problems


def risk_of(steps: list) -> str:
    levels = [RISKS.index(CATALOG[s["action"]].risk) for s in steps if s.get("action") in CATALOG]

    return RISKS[max(levels)] if levels else "high"


def describe_for_prompt() -> str:
    lines = []

    for spec in CATALOG.values():
        fields = [f"{n}" for n in spec.required] + [f"{n}?" for n in spec.optional]
        target = {"none": "\"\"", "deployment": "\"\""}.get(spec.target, f"<{spec.target} name>")
        lines.append(f"- {spec.name} [{spec.family}] target={target}"
                     + (f" fields: {', '.join(fields)}" if fields else "")
                     + f"\n    {spec.description}")

    return "\n".join(lines)
