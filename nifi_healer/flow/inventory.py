"""
A flow definition as the LLM sees it: one process group, compact.

    inv = inventory(definition, "S01 Slow Consumer", parameter_groups)
    text = render(inv)

The LLM never sees or writes flow JSON. It sees the processors and
connections of the incident's process group by name, with the settings
the action catalog can change, and names targets from this list.
Sensitive parameter values are masked.
"""

import re

from flows.builder.lib import walk_groups
from flows.builder.processors import PROCESSORS, kind_of, relationships

_REFERENCE = re.compile(r"#\{([^}]+)\}")

MASK = "********"


def find_group(definition: dict, group_name: str) -> dict | None:
    for _, group in walk_groups(definition["flowContents"]):
        if group.get("name") == group_name:
            return group

    return None


def references(group: dict) -> set:
    """Parameter names the group's processors reference."""

    found = set()

    for processor in group.get("processors") or []:
        for value in (processor.get("properties") or {}).values():
            found.update(_REFERENCE.findall(str(value or "")))

    return found


def editable_properties(processor: dict) -> list:
    """The keys SET_PROCESSOR_PROPERTY may change on this processor."""

    spec = PROCESSORS.get(kind_of(processor))

    if spec is None:
        return []

    keys = list(spec.editable)

    # EvaluateJsonPath's dynamic properties are its JSON paths: data, not
    # plumbing. Only ones that already exist can be changed.
    if spec.name == "EvaluateJsonPath":
        keys += [k for k in processor.get("properties") or {} if k not in spec.properties]

    return keys


def connection_label(connection: dict) -> str:
    return connection.get("name") or (
        f"{connection['source']['name']} -> {connection['destination']['name']}")


def inventory(definition: dict, group_name: str, parameter_groups: list | None = None) -> dict:
    group = find_group(definition, group_name)

    if group is None:
        return {"group": group_name, "error": f"no process group named {group_name!r}"}

    connections = group.get("connections") or []
    processors = []

    for raw in group.get("processors") or []:
        kind = kind_of(raw)
        spec = PROCESSORS.get(kind)
        routed = {}

        for connection in connections:
            if connection["source"]["id"] == raw["identifier"]:
                for rel in connection.get("selectedRelationships") or []:
                    routed.setdefault(rel, []).append(connection_label(connection))

        processors.append({
            "name": raw.get("name"),
            "kind": kind,
            "state": raw.get("scheduledState"),
            "concurrent_tasks": raw.get("concurrentlySchedulableTaskCount"),
            "run_schedule": raw.get("schedulingPeriod"),
            "scheduling": raw.get("schedulingStrategy"),
            "penalty": raw.get("penaltyDuration"),
            "yield": raw.get("yieldDuration"),
            "retried": list(raw.get("retriedRelationships") or []),
            "retry_count": raw.get("retryCount"),
            "backoff": raw.get("backoffMechanism"),
            "max_backoff": raw.get("maxBackoffPeriod"),
            "relationships": list(relationships(kind, raw.get("properties"))),
            "routed": routed,
            "auto_terminated": list(raw.get("autoTerminatedRelationships") or []),
            "properties": dict(raw.get("properties") or {}),
            "editable_properties": editable_properties(raw),
        })

    names = {p["name"] for p in processors}
    parameters = []
    used = references(group)

    for parameter_group in parameter_groups or []:
        for parameter in parameter_group.get("parameters") or []:
            if parameter.get("name") in used:
                parameters.append({
                    "context": parameter_group.get("name"),
                    "name": parameter.get("name"),
                    "value": MASK if parameter.get("sensitive") else parameter.get("value"),
                    "sensitive": bool(parameter.get("sensitive")),
                })

    return {
        "group": group_name,
        "processors": processors,
        "connections": [{
            "name": connection_label(c),
            "named": bool(c.get("name")),
            "source": c["source"]["name"],
            "destination": c["destination"]["name"],
            "relationships": list(c.get("selectedRelationships") or []),
            "object_threshold": c.get("backPressureObjectThreshold"),
            "size_threshold": c.get("backPressureDataSizeThreshold"),
            "expiration": c.get("flowFileExpiration"),
            "self_loop": c["source"]["id"] == c["destination"]["id"],
        } for c in connections],
        "dlqs": sorted(n for n in names if "DLQ" in n.upper()),
        "parameters": parameters,
    }


def render(inv: dict) -> str:
    """The inventory as plain text, one line per component."""

    if inv.get("error"):
        return inv["error"]

    lines = [f"Process group: {inv['group']}", "", "Processors:"]

    for p in inv["processors"]:
        retry = (f" retry {','.join(p['retried'])} x{p['retry_count']} backoff={p['backoff']}"
                 f" max_backoff={p['max_backoff']}" if p["retried"] else "")
        lines.append(
            f"- {p['name']} ({p['kind']}, {p['state']}) tasks={p['concurrent_tasks']} "
            f"run_schedule={p['run_schedule']} penalty={p['penalty']} yield={p['yield']}{retry}")

        routed = "; ".join(f"{rel} -> {', '.join(targets)}" for rel, targets in sorted(p["routed"].items()))
        unhandled = [r for r in p["relationships"] if r not in p["routed"] and r not in p["auto_terminated"]]
        lines.append(f"    relationships: {routed or 'none routed'}"
                     + (f"; auto-terminated: {', '.join(p['auto_terminated'])}" if p["auto_terminated"] else "")
                     + (f"; UNHANDLED: {', '.join(unhandled)}" if unhandled else ""))

        if p["properties"]:
            lines.append("    properties: " + "; ".join(f"{k}={v}" for k, v in p["properties"].items()))

        if p["editable_properties"]:
            lines.append("    editable properties: " + ", ".join(p["editable_properties"]))

    lines += ["", "Connections:"]

    for c in inv["connections"]:
        loop = " (self-loop)" if c["self_loop"] else ""
        lines.append(
            f"- {c['name']}: {c['source']} -[{', '.join(c['relationships'])}]-> {c['destination']}{loop} "
            f"back_pressure={c['object_threshold']} objects / {c['size_threshold']} "
            f"expiration={c['expiration']}")

    lines += ["", "Dead-letter processors: " + (", ".join(inv["dlqs"]) or "none")]

    if inv["parameters"]:
        lines += ["", "Parameters referenced here:"]
        lines += [f"- {p['name']} = {p['value']!r} (context {p['context']})" for p in inv["parameters"]]

    return "\n".join(lines)
