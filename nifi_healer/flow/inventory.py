"""
A flow definition as the LLM sees it: one process group, compact.

    inv = inventory(definition, "S01 Slow Consumer", parameter_groups)
    text = render(inv)
    text = render(scope(inv, seeds))     # a large group, around what the incident's KPIs read

The LLM never sees or writes flow JSON. It sees the processors and
connections of the incident's process group by name, with the settings
the action catalog can change, and names targets from this list.
Sensitive parameter values are masked.

A group of more than SCOPE_PROCESSORS processors is shown only around
the fault (NFH-97): the flows its KPIs read, and a line saying what else
the group holds. At 250 processors the whole group is about 30k tokens,
almost all of it flows that have nothing to do with the incident.
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
            "execution_node": raw.get("executionNode"),
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


def targets(inv: dict) -> dict:
    """
    The names a reviewer picks a fix's targets from, in the Review Queue's
    form (NFH-58): the whole group's, not the scoped inventory's.
    """

    if "error" in inv:
        return {}

    parameters = {}

    for parameter in inv["parameters"]:
        parameters.setdefault(parameter["context"], []).append(parameter["name"])

    return {"processors": {p["name"]: {"relationships": p["relationships"], "properties": p["editable_properties"]}
                           for p in inv["processors"]},
            "connections": [c["name"] for c in inv["connections"]],
            "dlqs": inv["dlqs"],
            "parameters": parameters}


# A group up to this many processors is shown whole; a larger one only around the fault.
SCOPE_PROCESSORS = 60


def _neighbours(inv: dict) -> dict:
    """{processor: the processors a connection joins it to, either way}; a self-loop joins nothing."""

    near = {p["name"]: set() for p in inv["processors"]}

    for c in inv["connections"]:
        if not c["self_loop"] and c["source"] in near and c["destination"] in near:
            near[c["source"]].add(c["destination"])
            near[c["destination"]].add(c["source"])

    return near


def _flows(names: set, near: dict, dlqs: set) -> list:
    """`names` without their DLQs, in separate flows: a DLQ two flows share does not make them one."""

    left, found = set(names) - dlqs, []

    while left:
        flow, frontier = set(), {left.pop()}

        while frontier:
            flow |= frontier
            frontier = {m for n in frontier for m in near[n] if m in left and m not in flow}
            left -= frontier

        found.append(flow)

    return found


def scope(inv: dict, seeds, budget: int = SCOPE_PROCESSORS) -> dict:
    """
    `inv` with only the fault's neighbourhood, when its group has more than
    `budget` processors: what joins to the `seeds` (the processor and
    connection names the incident's KPIs read) through any number of
    connections, either way, but not through a DLQ, which flows share.
    Hop by hop while that stays within `budget`. A processor nothing
    connects to stays, such as a DLQ nothing is routed to yet: a fix may
    route to it. "outside" counts what is left out. Unchanged without a
    seed in the group.
    """

    if "error" in inv or len(inv["processors"]) <= budget:
        return inv

    near, dlqs, seeds = _neighbours(inv), set(inv["dlqs"]), set(seeds)
    start = {s for s in seeds if s in near}
    start |= {end for c in inv["connections"] if c["name"] in seeds
              for end in (c["source"], c["destination"]) if end in near}

    if not start:
        return inv

    kept, frontier, hops = set(start), set(start), 0

    while frontier:
        reached = {m for n in frontier if n not in dlqs for m in near[n]} - kept

        if not reached or len(kept) + len(reached) > budget:
            break

        kept |= reached
        frontier = reached
        hops += 1

    kept |= {n for n, joined in near.items() if not joined}
    left = set(near) - kept
    flows = _flows(left, near, dlqs)
    shown = set(kept) - dlqs
    farther = [f for f in flows if any(near[n] & shown for n in f)]
    processors = [p for p in inv["processors"] if p["name"] in kept]
    connections = [c for c in inv["connections"] if c["source"] in kept and c["destination"] in kept]
    used = {name for p in processors for value in p["properties"].values()
            for name in _REFERENCE.findall(str(value or ""))}

    return dict(
        inv,
        processors=processors,
        connections=connections,
        dlqs=[n for n in inv["dlqs"] if n in kept],
        parameters=[p for p in inv["parameters"] if p["name"] in used],
        outside={"processors": len(left), "connections": len(inv["connections"]) - len(connections),
                 "flows": len(flows) - len(farther), "farther": len(farther), "hops": hops},
    )


def outside(inv: dict) -> str:
    """What scope() left out of the group, in a sentence; "" when it is whole."""

    out = inv.get("outside")

    if not out:
        return ""

    where = []

    if out["flows"]:
        where.append(f"{out['flows']} flow(s) not connected to the ones above")

    if out["farther"]:
        where.append(f"what lies more than {out['hops']} connection(s) from the components the incident's KPIs read")

    return (f"Also in this group, not shown: {out['processors']} processor(s) and {out['connections']} "
            "connection(s)" + (", in " + " and ".join(where) if where else "") + ".")


def render(inv: dict) -> str:
    """The inventory as plain text, one line per component."""

    if inv.get("error"):
        return inv["error"]

    lines = [f"Process group: {inv['group']}"] + ([outside(inv)] if inv.get("outside") else []) + ["", "Processors:"]

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
