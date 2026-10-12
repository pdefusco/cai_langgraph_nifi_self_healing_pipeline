#!/usr/bin/env python3
"""
Import each pack's flow definition into the DataFlow catalog.

    python provision/01_import_flows.py --canary      # first, once per tenant
    python provision/01_import_flows.py               # all seven packs
    python provision/01_import_flows.py --pack p1-throughput

Each definition is rebuilt and checked first, so nothing stale or broken
is uploaded. Idempotent: a flow already in the catalog under our name is
recorded, not imported twice. Its *first* version is the baseline every
reset returns to.

--canary is the cheapest real test of the generated JSON. It imports one
pack under a throwaway name, reads it back with get-flow-version,
compares what came back with what was sent (processor types, bundle
versions, property keys, connections), and deletes it. A definition the
catalog rewrites or rejects fails here in seconds, not fifteen minutes
into a deployment.
"""

import argparse
import json
import sys
import time

import _common
from _common import config, packs

from flows.build_all import check_pack
from flows.builder.lib import check, iter_connections, iter_processors
from nifi_healer import provisioning
from nifi_healer.cdp.client import require


def build_or_exit(name: str) -> None:
    definition = packs.build(name)
    problems = check(definition)["problems"] + check_pack(packs.PACKS[name], definition)

    if problems:
        raise SystemExit(f"{name} does not build cleanly:\n  " + "\n  ".join(problems)
                         + "\nFix it and run python flows/build_all.py --check.")


def fingerprint(definition: dict) -> dict:
    """What must survive the round trip, keyed by component name."""

    processors = {
        p["name"]: {
            "type": p["type"],
            "bundle": (p.get("bundle") or {}).get("version"),
            "properties": sorted((p.get("properties") or {}).keys()),
            "autoTerminated": sorted(p.get("autoTerminatedRelationships") or []),
            "tasks": p.get("concurrentlySchedulableTaskCount"),
            "schedule": p.get("schedulingPeriod"),
        }
        for _, _, p in iter_processors(definition)
    }
    connections = {
        c.get("name") or c["identifier"]: {
            "relationships": sorted(c.get("selectedRelationships") or []),
            "objects": c.get("backPressureObjectThreshold"),
            "size": c.get("backPressureDataSizeThreshold"),
            "expiration": c.get("flowFileExpiration"),
        }
        for _, _, c in iter_connections(definition)
    }

    return {"processors": processors, "connections": connections}


def canary(client, name: str) -> int:
    build_or_exit(name)
    path = provisioning.definition_file(name)
    sent = json.load(open(path))
    canary_name = f"{config.PREFIX}canary-{name}-{int(time.time())}"

    print(f"[canary] importing {path} as {canary_name}")
    result = require(client.df("import-flow-definition", "--file", path, "--name", canary_name,
                               "--description", "Canary import; deleted by the same script.",
                               "--comments", "canary"), "canary import")
    flow_crn = result["crn"]
    version_crn = result["versions"][0]["crn"]
    failures = []

    try:
        returned = client.flow_definition(version_crn)
        require(returned, "read the canary back")
        a, b = fingerprint(sent), fingerprint(returned)

        for kind in ("processors", "connections"):
            for component in sorted(set(a[kind]) | set(b[kind])):
                if a[kind].get(component) != b[kind].get(component):
                    failures.append(f"{kind[:-1]} {component!r}:\n      sent     "
                                    f"{a[kind].get(component)}\n      returned {b[kind].get(component)}")

        print(f"[canary] read back {len(b['processors'])} processor(s), "
              f"{len(b['connections'])} connection(s)")

    finally:
        deleted = client.df("delete-flow", "--flow-crn", flow_crn)
        print(f"[canary] {'deleted' if 'error' not in deleted else 'COULD NOT DELETE'} {canary_name}")

        if "error" in deleted:
            print(f"  delete it by hand: cdp df delete-flow --flow-crn {flow_crn}")

    if failures:
        print("\n[canary] the catalog changed the definition:\n  " + "\n  ".join(failures))
        return 1

    print("[canary] round trip identical")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    _common.add_pack_argument(parser)
    parser.add_argument("--canary", action="store_true",
                        help="import one pack under a throwaway name, read it back, delete it")
    args = parser.parse_args()

    _common.banner("01 import flows")
    client = _common.Tenant().client

    if args.canary:
        return canary(client, (args.pack or ["p1-throughput"])[0])

    for name in _common.selected(args):
        build_or_exit(name)
        provisioning.import_flow(client, name)

    return 0


if __name__ == "__main__":
    sys.exit(main())
