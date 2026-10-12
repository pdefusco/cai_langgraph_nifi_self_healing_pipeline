"""
Catalog steps applied to a flow definition, deterministically.

    new, changes = apply(definition, "S09 Failure Self-Loop", steps)
    report = validate_edit(definition, new, parameters, "S09 Failure Self-Loop")
    report["ok"], report["problems"], report["diff"]

Only FLOW_EDIT steps are handled here (parameters and sizing are not in
the flow). Every edit works on a copy, touches only the incident's
process group, and must change something: a step that is already true
of the flow is an error, so a proposal cannot pass review by doing
nothing. Component identifiers are kept, so KPIs (keyed by component
id) and queued FlowFiles survive the version change.

validate_edit compares the problems lib.check and the simulator's
compiler find before and after, and reports only the ones the edit
introduced: the faulty baseline is allowed its own faults.
"""

import copy
import json

from flows.builder.lib import check, component_id, walk_groups
from flows.builder.processors import PROCESSORS, kind_of
from nifi_healer.actions import catalog
from nifi_healer.flow.inventory import editable_properties, find_group
from nifi_healer.sim.model import compile_flow


class EditError(ValueError):
    """A step that cannot be applied to this flow."""


def _processor(group: dict, name: str) -> dict:
    for processor in group.get("processors") or []:
        if processor.get("name") == name:
            return processor

    names = ", ".join(sorted(p.get("name") or "" for p in group.get("processors") or []))

    raise EditError(f"no processor named {name!r} in {group.get('name')!r} (processors: {names})")


def _connection(group: dict, name: str) -> dict:
    for connection in group.get("connections") or []:
        if connection.get("name") == name:
            return connection

    names = ", ".join(sorted(c.get("name") for c in group.get("connections") or [] if c.get("name")))

    raise EditError(f"no connection named {name!r} in {group.get('name')!r} (connections: {names})")


def _set(item: dict, key: str, value, label: str, changes: list) -> None:
    if item.get(key) == value:
        raise EditError(f"{label}: {key} is already {value!r}")

    changes.append(f"{label}: {key} {item.get(key)!r} -> {value!r}")
    item[key] = value


def _endpoint(group: dict, processor: dict) -> dict:
    return {
        "comments": None,
        "groupId": group["identifier"],
        "id": processor["identifier"],
        "instanceIdentifier": None,
        "name": processor["name"],
        "type": "PROCESSOR",
    }


# ---------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------


def _route_to_dlq(group: dict, step: dict, changes: list) -> None:
    source = _processor(group, step["target"])
    dlq = _processor(group, step["dlq"])
    wanted = set(catalog.relationships_of(step))
    spec = PROCESSORS.get(kind_of(source))
    label = source["name"]

    if "DLQ" not in dlq["name"].upper():
        raise EditError(f"{dlq['name']!r} is not a dead-letter processor (its name has no DLQ)")

    if dlq is source:
        raise EditError("a processor cannot be its own DLQ")

    unknown = sorted(wanted - set(spec.relationships if spec else ()))

    if unknown:
        raise EditError(f"{label} has no relationship(s) {', '.join(unknown)}; "
                        f"it has {', '.join(spec.relationships) if spec else 'none'}")

    connections = group.setdefault("connections", [])
    outgoing = [c for c in connections if c["source"]["id"] == source["identifier"]]
    target = next((c for c in outgoing if c["destination"]["id"] == dlq["identifier"]), None)

    if target is not None and wanted <= set(target["selectedRelationships"]):
        raise EditError(f"{label}: {', '.join(sorted(wanted))} already go(es) to {dlq['name']}")

    if target is None:
        # Repoint a connection that only carries these relationships (a
        # self-loop, typically). It keeps its id, so its queue and its
        # KPI move with it to the DLQ.
        target = next((c for c in outgoing if set(c["selectedRelationships"]) <= wanted), None)

        if target is not None:
            changes.append(f"{target.get('name') or target['identifier']}: destination "
                           f"{target['destination']['name']!r} -> {dlq['name']!r}")
            target["destination"] = _endpoint(group, dlq)
            target["bends"] = []

    if target is None:
        name = f"{source['name'].split(' ', 1)[0].lower()}-to-dlq"

        if any(c.get("name") == name for c in connections):
            name = f"{name}-{len(connections)}"

        target = {
            "backPressureDataSizeThreshold": group.get("defaultBackPressureDataSizeThreshold") or "1 GB",
            "backPressureObjectThreshold": int(group.get("defaultBackPressureObjectThreshold") or 10000),
            "bends": [],
            "comments": None,
            "componentType": "CONNECTION",
            "destination": _endpoint(group, dlq),
            "flowFileExpiration": "0 sec",
            "groupIdentifier": group["identifier"],
            "identifier": component_id(f"conn:{group['identifier']}/{name}"),
            "instanceIdentifier": None,
            "labelIndex": 1,
            "loadBalanceCompression": "DO_NOT_COMPRESS",
            "loadBalanceStrategy": "DO_NOT_LOAD_BALANCE",
            "name": name,
            "partitioningAttribute": None,
            "position": None,
            "prioritizers": [],
            "selectedRelationships": [],
            "source": _endpoint(group, source),
            "zIndex": 0,
        }
        connections.append(target)
        changes.append(f"{name}: new connection {label} -> {dlq['name']}")

    added = sorted(wanted - set(target["selectedRelationships"]))
    target["selectedRelationships"] = sorted(set(target["selectedRelationships"]) | wanted)

    if added:
        changes.append(f"{target['name'] or target['identifier']}: carries {', '.join(target['selectedRelationships'])}")

    # Nothing else may still carry or drop these relationships.
    for connection in list(connections):
        if connection is target or connection["source"]["id"] != source["identifier"]:
            continue

        moved = wanted & set(connection["selectedRelationships"])

        if not moved:
            continue

        left = sorted(set(connection["selectedRelationships"]) - wanted)
        label_c = connection.get("name") or connection["identifier"]

        if left:
            connection["selectedRelationships"] = left
            changes.append(f"{label_c}: no longer carries {', '.join(sorted(moved))}")

        else:
            connections.remove(connection)
            changes.append(f"{label_c}: removed (it only carried {', '.join(sorted(moved))})")

    terminated = set(source.get("autoTerminatedRelationships") or [])

    if terminated & wanted:
        source["autoTerminatedRelationships"] = sorted(terminated - wanted)
        changes.append(f"{label}: no longer auto-terminates {', '.join(sorted(terminated & wanted))}")

    if dlq.get("scheduledState") == "DISABLED":
        dlq["scheduledState"] = "ENABLED"
        changes.append(f"{dlq['name']}: scheduledState DISABLED -> ENABLED")


def _apply_step(group: dict, step: dict, changes: list) -> None:
    kind = step["action"]
    target = step.get("target")

    if kind == "SET_CONCURRENT_TASKS":
        _set(_processor(group, target), "concurrentlySchedulableTaskCount", int(step["value"]), target, changes)

    elif kind == "SET_RUN_SCHEDULE":
        processor = _processor(group, target)

        if processor.get("schedulingStrategy") != "TIMER_DRIVEN":
            raise EditError(f"{target} is {processor.get('schedulingStrategy')}, not timer driven")

        _set(processor, "schedulingPeriod", step["value"], target, changes)

    elif kind == "SET_BACKPRESSURE":
        connection = _connection(group, target)
        before = len(changes)

        if step.get("object_threshold") is not None and \
                connection.get("backPressureObjectThreshold") != step["object_threshold"]:
            _set(connection, "backPressureObjectThreshold", int(step["object_threshold"]), target, changes)

        if step.get("size_threshold") is not None and \
                connection.get("backPressureDataSizeThreshold") != step["size_threshold"]:
            _set(connection, "backPressureDataSizeThreshold", step["size_threshold"], target, changes)

        if len(changes) == before:
            raise EditError(f"{target}: back pressure is already at those thresholds")

    elif kind == "SET_FLOWFILE_EXPIRATION":
        _set(_connection(group, target), "flowFileExpiration", step["value"], target, changes)

    elif kind == "SET_PENALTY_YIELD":
        processor = _processor(group, target)
        before = len(changes)

        for field, key in (("penalty", "penaltyDuration"), ("yield_duration", "yieldDuration")):
            if step.get(field) is not None and processor.get(key) != step[field]:
                _set(processor, key, step[field], target, changes)

        if len(changes) == before:
            raise EditError(f"{target}: penalty and yield are already those values")

    elif kind == "SET_RETRY_POLICY":
        processor = _processor(group, target)

        if not processor.get("retriedRelationships"):
            raise EditError(f"{target} retries no relationship; its retry settings do nothing")

        before = len(changes)

        for field, key in (("backoff", "backoffMechanism"), ("max_backoff", "maxBackoffPeriod"),
                           ("retry_count", "retryCount")):
            if step.get(field) is not None and processor.get(key) != step[field]:
                _set(processor, key, step[field], target, changes)

        if len(changes) == before:
            raise EditError(f"{target}: the retry policy is already that")

    elif kind == "SET_PROCESSOR_PROPERTY":
        processor = _processor(group, target)
        key = step["property"]
        allowed = editable_properties(processor)

        if key not in allowed:
            raise EditError(f"{target}: {key!r} is not editable; editable: {', '.join(allowed) or 'none'}")

        current = (processor.get("properties") or {}).get(key)

        if current is not None and "#{" in str(current):
            raise EditError(f"{target}: {key!r} is {current!r}, a parameter; use UPDATE_PARAMETER")

        _set(processor.setdefault("properties", {}), key, str(step["value"]), f"{target}.{key}", changes)

    elif kind == "ROUTE_RELATIONSHIP_TO_DLQ":
        _route_to_dlq(group, step, changes)

    elif kind == "SET_SCHEDULED_STATE":
        _set(_processor(group, target), "scheduledState", step["value"], target, changes)

    else:
        raise EditError(f"{kind} is not a flow edit")


def apply(definition: dict, group_name: str, steps: list) -> tuple:
    """(new definition, [change descriptions]). Raises EditError."""

    edited = copy.deepcopy(definition)
    group = find_group(edited, group_name)

    if group is None:
        raise EditError(f"no process group named {group_name!r}")

    changes = []

    for step in steps:
        _apply_step(group, catalog.normalize(step), changes)

    return edited, changes


# ---------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------


def _label(item) -> str:
    if isinstance(item, dict):
        return str(item.get("name") or item.get("identifier"))

    return str(item)


def diff_paths(before, after, path: str = "") -> list:
    """
    [{path, before, after}] for every leaf that differs. Lists of
    components are matched by identifier and labelled by name.
    """

    if isinstance(before, dict) and isinstance(after, dict):
        found = []

        for key in sorted(set(before) | set(after)):
            if before.get(key) != after.get(key):
                found.extend(diff_paths(before.get(key), after.get(key), f"{path}.{key}" if path else key))

        return found

    if isinstance(before, list) and isinstance(after, list) and all(
            isinstance(i, dict) and "identifier" in i for i in before + after) and (before or after):
        old = {i["identifier"]: i for i in before}
        new = {i["identifier"]: i for i in after}
        found = []

        for identifier in list(old) + [i for i in new if i not in old]:
            label = f"{path}[{_label(new.get(identifier) or old.get(identifier))}]"

            if identifier not in new:
                found.append({"path": label, "before": "present", "after": "removed"})

            elif identifier not in old:
                found.append({"path": label, "before": "absent", "after": "added"})

            else:
                found.extend(diff_paths(old[identifier], new[identifier], label))

        return found

    if before == after:
        return []

    return [{"path": path, "before": before, "after": after}]


def _problems(definition: dict, parameters: dict) -> set:
    return set(check(definition)["problems"]) | set(compile_flow(definition, parameters).problems)


def _without(definition: dict, group_name: str) -> str:
    """The definition with one group blanked, for an everything-else comparison."""

    stripped = copy.deepcopy(definition)

    for _, group in walk_groups(stripped["flowContents"]):
        for index, child in enumerate(group.get("processGroups") or []):
            if child.get("name") == group_name:
                group["processGroups"][index] = {"name": group_name}

    return json.dumps(stripped, sort_keys=True)


def validate_edit(before: dict, after: dict, parameters: dict | None = None,
                  group_name: str = "") -> dict:
    parameters = parameters or {}
    problems = sorted(_problems(after, parameters) - _problems(before, parameters))

    if group_name:
        if _without(before, group_name) != _without(after, group_name):
            problems.append(f"the edit changed something outside {group_name!r}")

        diff = diff_paths(find_group(before, group_name) or {}, find_group(after, group_name) or {})

    else:
        diff = diff_paths(before["flowContents"], after["flowContents"])

    if not diff:
        problems.append("the edit changes nothing")

    return {"ok": not problems, "problems": problems, "diff": diff}
