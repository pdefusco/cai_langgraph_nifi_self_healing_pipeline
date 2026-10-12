#!/usr/bin/env python3
"""
Put a pack back to its faulty baseline, so the same experiment can be
run again.

    python provision/06_reset_pack.py --pack p1-throughput --dry-run
    python provision/06_reset_pack.py --pack p1-throughput

Undoes whatever the agent (or anyone) changed, one step at a time and
only where the deployment differs from the baseline:

  1. flow version: back to the first catalog version, the faulty one
     (df change-flow-version-in-deployment, ONLY_RESTART_AFFECTED_COMPONENTS)
  2. parameters: back to the scenario values
     (dfworkload update-flow-in-deployment --parameter-groups)
  3. sizing: back to the pack's cluster size and node count
     (dfworkload update-deployment)

Each step goes through the same guardrails as the agent's actions and
waits for the deployment to settle before the next. Like the agent, it
needs SELFHEAL_ARMED=true to write; disarmed it behaves as --dry-run.

Queued data is not emptied: a backlog the fix drained has to build up
again, which takes a few minutes for the throughput packs.
"""

import argparse
import sys

import _common
from _common import config

from nifi_healer import fleet, provisioning


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    _common.add_pack_argument(parser, required=True)
    parser.add_argument("--dry-run", action="store_true", help="print the commands, change nothing")
    args = parser.parse_args()

    _common.banner("06 reset pack")
    tenant = _common.Tenant()
    dry_run = args.dry_run or not config.ARMED

    if dry_run and not args.dry_run:
        print("SELFHEAL_ARMED is not true: dry run only\n")

    for name in _common.selected(args):
        entry = fleet.pack(name)

        if not entry.get("deployment_crn"):
            print(f"[{name}] not deployed; skipped")
            continue

        print(f"[{name}]")
        records = provisioning.reset_pack(tenant.client, name, dry_run=dry_run,
                                          wait=tenant.settle_wait(entry["deployment_crn"]))

        if records and not dry_run:
            print(f"  {len(records)} step(s) applied")

    return 0


if __name__ == "__main__":
    sys.exit(main())
