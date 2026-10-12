#!/usr/bin/env python3
"""
Deploy each imported pack, and wait for the deployments to settle.

    python provision/02_create_deployments.py --dry-run
    python provision/02_create_deployments.py
    python provision/02_create_deployments.py --pack p1-throughput

This is the step that starts billing: six EXTRA_SMALL x1 deployments and
one MEDIUM x2 (p7, deliberately oversized). All requests go out first,
then the script waits for each, since a deployment takes 10-20 minutes
and they provision in parallel.

Idempotent: a deployment already carrying our name in our service is
adopted, not duplicated. One in a different service is refused.
"""

import argparse
import sys

import _common
from _common import config, packs

from nifi_healer import fleet, provisioning
from nifi_healer.cdp.client import command_line


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    _common.add_pack_argument(parser)
    parser.add_argument("--dry-run", action="store_true", help="print the commands, create nothing")
    args = parser.parse_args()

    _common.banner("02 create deployments")
    tenant = _common.Tenant()
    names = _common.selected(args)

    for name in names:
        if not fleet.pack(name).get("baseline_flow_version_crn"):
            raise SystemExit(f"{name}: not imported; run provision/01_import_flows.py first.")

    if args.dry_run:
        for name in names:
            argv = ["df"] + [str(a) for a in provisioning.create_deployment_args(
                name, fleet.pack(name)["baseline_flow_version_crn"])]
            print(command_line(argv) + "\n")

        return 0

    started = {}

    for name in names:
        print(f"[{name}] {packs.PACKS[name].cluster_size} x{packs.PACKS[name].nodes}")
        started[name] = provisioning.start_deployment(tenant.client, name)

    for name, crn in started.items():
        print(f"[{name}] waiting")
        entry = provisioning.create_deployment(tenant.client, name, wait=tenant.deployment_wait(), crn=crn)
        print(f"  deployed flow {entry['deployed_flow_crn']}")

    print("\nnext: provision/03_discover_kpi_metadata.py, then 04_configure_kpis.py")

    if not config.MOCK:
        print("these deployments bill until provision/teardown.py removes them.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
