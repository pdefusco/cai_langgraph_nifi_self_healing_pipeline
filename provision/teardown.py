#!/usr/bin/env python3
"""
Remove what 01 and 02 created: terminate each deployment, then delete
its catalog flow.

    python provision/teardown.py --dry-run
    python provision/teardown.py --pack p1-throughput
    python provision/teardown.py --pack p1-throughput --keep-flow
    python provision/teardown.py --pack p1-throughput --deployment-crn crn:cdp:df:...

The order is fixed: a flow a deployment still references cannot be
deleted. Targets are the CRNs the fleet file recorded, never a name
search, and each is re-checked live (our prefix, our service, not on
the denylist) before anything is removed. The fleet entry is cleared
only once its deployment is gone.

--keep-flow keeps the catalog flow and its versions (catalog versions
cannot be deleted one by one, so this is how the agent's fix history
survives a teardown). --deployment-crn names the deployment when the
fleet record of it was lost; the other checks still apply.

Unlike 06, this is not gated by SELFHEAL_ARMED: running it is the
decision. It asks for confirmation against a live tenant unless --yes.
"""

import argparse
import sys

import _common
from _common import config

from nifi_healer import fleet, provisioning


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    _common.add_pack_argument(parser)
    parser.add_argument("--dry-run", action="store_true", help="print what would be removed")
    parser.add_argument("--keep-flow", action="store_true", help="keep the catalog flow")
    parser.add_argument("--deployment-crn", default="",
                        help="the deployment to terminate, if the fleet lost it (needs one --pack)")
    parser.add_argument("--yes", action="store_true", help="do not ask for confirmation")
    args = parser.parse_args()

    _common.banner("teardown")
    names = _common.selected(args)

    if args.deployment_crn and len(names) != 1:
        raise SystemExit("--deployment-crn needs exactly one --pack.")

    names = [n for n in names if fleet.pack(n) or args.deployment_crn]

    if not names:
        print("nothing recorded to tear down")
        return 0

    if not (args.dry_run or args.yes or config.MOCK):
        answer = input(f"terminate and delete {', '.join(names)}? type 'yes': ")

        if answer.strip() != "yes":
            print("cancelled")
            return 1

    client = _common.Tenant().client
    incomplete = []

    for name in names:
        print(f"[{name}]")

        if not provisioning.teardown_pack(client, name, keep_flow=args.keep_flow, dry_run=args.dry_run,
                                          deployment_override=args.deployment_crn):
            incomplete.append(name)

    if incomplete:
        print(f"\nnot finished: {', '.join(incomplete)}; re-run to continue")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
