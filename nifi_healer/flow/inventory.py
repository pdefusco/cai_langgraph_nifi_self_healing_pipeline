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
    """Parameter names the processors of the group and the groups inside it reference."""

    found = set()

    for _, inner in walk_groups(group):
        for processor in inner.get("processors") or []:
            for value in (processor.get("properties") or {}).values():
                found.update(_REFERENCE.findall(str(value or "")))

    return found


def subgroups(group: dict):
    """Yield (path below `group`, "" for itself, as "Sub/Sub2"; group) for it and every group inside it."""

    for names, inner in walk_groups(group):
        yield "/".join(names[1:]), inner


def target_path(group_name: str, target) -> str:
    """
    A target as a path below the incident's group: "Sub / Name" is
    "Sub/Name", and one that starts at the group itself, as the
    inventory's "Process group: Top / Sub" headers do, starts below it
    (S98, NFH-105).
    """

    parts = [part.strip() for part in str(target or "").split("/")]

    if len(parts) > 1 and parts[0] == group_name:
        parts = parts[1:]

    return "/".join(parts)


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

    processors, connections, ports = [], [], []

    for path, inner in subgroups(group):
        for raw in (inner.get("inputPorts") or []) + (inner.get("outputPorts") or []):
            ports.append({"name": raw.get("name"), "kind": raw.get("type"), "path": path})

        for c in inner.get("connections") or []:
            connections.append({
                "name": connection_label(c),
                "named": bool(c.get("name")),
                "path": path,
                "source": c["source"]["name"],
                "destination": c["destination"]["name"],
                "relationships": list(c.get("selectedRelationships") or []),
                "object_threshold": c.get("backPressureObjectThreshold"),
                "size_threshold": c.get("backPressureDataSizeThreshold"),
                "expiration": c.get("flowFileExpiration"),
                "self_loop": c["source"]["id"] == c["destination"]["id"],
            })

        processors += _processors(inner, path)

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
        "connections": connections,
        "ports": ports,
        "dlqs": sorted(n for n in names if "DLQ" in n.upper()),
        "parameters": parameters,
    }


def _processors(group: dict, path: str) -> list:
    """The processors directly in `group`, `path` below the incident's group."""

    connections = group.get("connections") or []
    processors = []

    for raw in group.get("processors") or []:
        kind = kind_of(raw)
        routed = {}

        for connection in connections:
            if connection["source"]["id"] == raw["identifier"]:
                for rel in connection.get("selectedRelationships") or []:
                    routed.setdefault(rel, []).append(connection_label(connection))

        processors.append({
            "name": raw.get("name"),
            "path": path,
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

    return processors


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


def _through(inv: dict, name: str, side: str, ports: set, seen=frozenset()) -> set:
    """The processor `name` is, or those a port of `ports` reaches on `side` ("source" upstream, "destination" down)."""

    if name not in ports:
        return {name}

    other = "destination" if side == "source" else "source"

    return {found for c in inv["connections"] if c[other] == name and name not in seen
            for found in _through(inv, c[side], side, ports, seen | {name})}


def _neighbours(inv: dict) -> dict:
    """
    {processor: the processors a connection joins it to, either way, through
    ports}; a self-loop joins nothing.
    """

    near = {p["name"]: set() for p in inv["processors"]}
    ports = {p["name"] for p in inv.get("ports") or []}

    for c in inv["connections"]:
        if c["self_loop"]:
            continue

        for source in _through(inv, c["source"], "source", ports):
            for destination in _through(inv, c["destination"], "destination", ports):
                if source in near and destination in near:
                    near[source].add(destination)
                    near[destination].add(source)

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
    ports = {p["name"] for p in inv.get("ports") or []}
    start = {s for s in seeds if s in near}
    start |= {end for c in inv["connections"] if c["name"] in seeds
              for side in ("source", "destination") for end in _through(inv, c[side], side, ports) if end in near}

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

    def shown_end(name, side):
        return bool(_through(inv, name, side, ports) & kept)

    connections = [c for c in inv["connections"]
                   if shown_end(c["source"], "source") and shown_end(c["destination"], "destination")]
    linked = {end for c in connections for end in (c["source"], c["destination"])}
    used = {name for p in processors for value in p["properties"].values()
            for name in _REFERENCE.findall(str(value or ""))}
    paths = {p.get("path", "") for p in processors}
    hidden = {}

    # The groups inside whose own processors are all left out; the group itself is counted above.
    for p in inv["processors"]:
        if p.get("path", "") not in paths | {""}:
            hidden[p.get("path", "")] = hidden.get(p.get("path", ""), 0) + 1

    return dict(
        inv,
        processors=processors,
        connections=connections,
        ports=[p for p in inv.get("ports") or [] if p["name"] in linked],
        dlqs=[n for n in inv["dlqs"] if n in kept],
        parameters=[p for p in inv["parameters"] if p["name"] in used],
        outside={"processors": len(left), "connections": len(inv["connections"]) - len(connections),
                 "flows": len(flows) - len(farther), "farther": len(farther), "hops": hops}
                | ({"groups": sorted(hidden.items())} if hidden else {}),
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

    groups = "".join(f" {path} ({count} processor(s))," for path, count in out.get("groups") or [])

    return (f"Also in this group, not shown: {out['processors']} processor(s) and {out['connections']} "
            "connection(s)" + (", in " + " and ".join(where) if where else "") + "."
            + (f" Groups inside it with none of their own processors shown:{groups.rstrip(',')}." if groups else ""))


def _end(name: str, ports: dict) -> str:
    return f'{ports[name].lower().replace("_", " ")} "{name}"' if name in ports else name


def render(inv: dict) -> str:
    """
    The inventory as plain text, one line per component; a group with
    groups inside it, one block per group.
    """

    if inv.get("error"):
        return inv["error"]

    lines = [f"Process group: {inv['group']}"] + ([outside(inv)] if inv.get("outside") else [])
    paths = list(dict.fromkeys([p.get("path", "") for p in inv["processors"]] + [c.get("path", "") for c in inv["connections"]]))

    if paths in ([], [""]):
        return "\n".join(lines + _render_block(inv, inv["processors"], inv["connections"]) + _render_tail(inv))

    lines.append("Groups inside it are shown by their path below it, as \"Sub / Sub2\".")

    for path in paths:
        lines += ["", f"Process group: {' / '.join([inv['group']] + ([path.replace('/', ' / ')] if path else []))}"]
        lines += _render_block(inv, [p for p in inv["processors"] if p.get("path", "") == path],
                               [c for c in inv["connections"] if c.get("path", "") == path])

    return "\n".join(lines + _render_tail(inv))


def _render_block(inv: dict, processors: list, connections: list) -> list:
    lines = ["", "Processors:"]
    ports = {p["name"]: p["kind"] for p in inv.get("ports") or []}

    for p in processors:
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

    for c in connections:
        loop = " (self-loop)" if c["self_loop"] else ""
        lines.append(
            f"- {c['name']}: {_end(c['source'], ports)} -[{', '.join(c['relationships'])}]-> "
            f"{_end(c['destination'], ports)}{loop} "
            f"back_pressure={c['object_threshold']} objects / {c['size_threshold']} "
            f"expiration={c['expiration']}")

    return lines


def _render_tail(inv: dict) -> list:
    lines = ["", "Dead-letter processors: " + (", ".join(inv["dlqs"]) or "none")]

    if inv["parameters"]:
        lines += ["", "Parameters referenced here:"]
        lines += [f"- {p['name']} = {p['value']!r} (context {p['context']})" for p in inv["parameters"]]

    return lines
