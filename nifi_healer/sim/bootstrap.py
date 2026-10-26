"""
Build the mock tenant: every pack imported, deployed and given its KPIs
through MockClient, with the same provisioning code a live tenant gets,
then an hour of history so the charts are not empty.

    python -m nifi_healer.sim.bootstrap            # build if absent
    python -m nifi_healer.sim.bootstrap --reset    # start over

The app calls `ensure()` at start in mock mode.
"""

import argparse
import os
import sys

import config
from flows import packs
from nifi_healer import fleet, provisioning
from nifi_healer.cdp.mock_client import MockClient
from nifi_healer.sim.world import get_world

HISTORY_SECONDS = 3600


def build(history: float = HISTORY_SECONDS, names=None, reset: bool = False, epoch: float | None = None) -> dict:
    """The packs `names` (all by default) on the mock; `reset` starts over first, the clock at `epoch` or now."""

    if not config.MOCK:
        raise SystemExit("bootstrap builds the mock tenant; SELFHEAL_DF_CLIENT is 'live'.")

    world = get_world()
    client = MockClient(world)

    if reset:
        world.reset(epoch)

        if os.path.exists(config.FLEET_FILE):
            os.remove(config.FLEET_FILE)

    def settle(crn):
        world.settle(crn)

        return client.deployment_facts(crn)["state"]

    for name in names or packs.PACKS:
        provisioning.log(f"[{name}]")
        provisioning.import_flow(client, name)
        entry = provisioning.create_deployment(client, name, wait=settle)
        provisioning.configure_kpis(client, name, wait=lambda: world.settle(entry["deployment_crn"]))

        if history:
            world.backfill(entry["deployment_crn"], history)

    return fleet.read()


def ensure() -> bool:
    """Build the mock tenant if this fleet has no deployments. True if it built."""

    entries = fleet.read()["packs"]
    world = get_world()

    with world.reading():
        known = {d["crn"] for d in world.deployments()}

    if entries and all(e.get("deployment_crn") in known and e.get("kpis_configured")
                       for e in entries.values()):
        return False

    build(reset=True)

    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--reset", action="store_true", help="discard the mock tenant first")
    parser.add_argument("--pack", action="append", help="only this pack (repeatable)")
    parser.add_argument("--history", type=float, default=HISTORY_SECONDS,
                        help="simulated seconds of history to give each deployment")
    args = parser.parse_args()

    build(args.history, args.pack, args.reset)
    print(f"\nmock tenant: {config.MOCK_WORLD_FILE}\nfleet:       {config.FLEET_FILE}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
