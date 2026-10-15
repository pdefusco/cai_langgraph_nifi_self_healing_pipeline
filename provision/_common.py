"""
Shared by the provision/ scripts: import path, pack selection, the
client, and how to wait.

Every script runs against whichever tenant SELFHEAL_DF_CLIENT selects.
Against the mock (the default) the waits apply pending changes at once,
so a whole rehearsal takes seconds; against a live tenant they poll.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config  # noqa: E402
from flows import packs  # noqa: E402
from nifi_healer.cdp.client import get_client  # noqa: E402


def add_pack_argument(parser, required: bool = False) -> None:
    parser.add_argument("--pack", action="append", choices=sorted(packs.PACKS), required=required,
                        help="this pack (repeatable)" if required else
                        "only this pack (repeatable; default: the wave-1 packs, p1-p7)")


def selected(args) -> list:
    return args.pack or list(packs.DEFAULT)


def banner(title: str) -> None:
    print(f"{title}\n  client:  {config.DF_CLIENT}"
          f"\n  service: {config.SERVICE_CRN or '(SELFHEAL_SERVICE_CRN unset)'}"
          f"\n  fleet:   {config.FLEET_FILE}\n", flush=True)

    if not config.MOCK and not (config.SERVICE_CRN and config.ENVIRONMENT_CRN):
        raise SystemExit("error: set SELFHEAL_SERVICE_CRN and SELFHEAL_ENVIRONMENT_CRN.")


class Tenant:
    """The client plus the waits that suit it."""

    def __init__(self):
        self.client = get_client()
        self.world = None

        if config.MOCK:
            from nifi_healer.sim.world import get_world

            self.world = get_world()

    def deployment_wait(self):
        """For create_deployment: wait(crn) -> state reached."""

        if not self.world:
            return None

        def settle(crn):
            self.world.settle(crn)

            return self.client.deployment_facts(crn)["state"]

        return settle

    def settle_wait(self, deployment_crn: str):
        """For configure_kpis and reset_pack: wait() -> None."""

        if not self.world:
            return None

        return lambda: self.world.settle(deployment_crn)
