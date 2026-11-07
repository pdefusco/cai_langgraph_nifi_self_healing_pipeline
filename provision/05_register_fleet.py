#!/usr/bin/env python3
"""
Confirm the fleet file still describes the tenant, and finish what 04
could not (charts that lagged the KPI write).

    python provision/05_register_fleet.py
    python provision/05_register_fleet.py --pack p1-throughput
    python provision/05_register_fleet.py --pack p1-throughput --adopt

For each pack this reads the recorded deployment back and checks its
name, service, flow and deployed-flow CRN against the record, looks
again for any KPI chart still missing, and prints the fleet. The
guardrails act only on what the fleet file records, so a pack with a
problem here is one the app will refuse to touch.

--adopt rebuilds a lost entry (the fleet file was deleted, or the
project was copied without var/data) from the tenant, by exact name in
our service. It refuses when the name is ambiguous or the pack is still
registered. Run 04 afterwards to record the charts again.

Last, it says whether the running app knows each pack's scenarios. A
pack added after the app started is polled but opens no incident until
the app restarts (NFH-99).
"""

import argparse
import sys
import time

import _common

from nifi_healer import fleet, provisioning


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    _common.add_pack_argument(parser)
    parser.add_argument("--adopt", action="store_true",
                        help="rebuild a lost fleet entry from the tenant, by exact name")
    args = parser.parse_args()

    _common.banner("05 register fleet")
    client = _common.Tenant().client
    names = _common.selected(args)
    failed = []

    if args.adopt:
        for name in names:
            entry = provisioning.adopt(client, name)
            print(f"[{name}] adopted flow {entry.get('flow_crn')}, "
                  f"deployment {entry.get('deployment_crn') or 'none'}")

        print("\nnext: provision/04_configure_kpis.py to record the charts, then this again")
        return 0

    rows = []

    for name in names:
        entry = fleet.pack(name)

        if not entry.get("deployment_crn"):
            rows.append((name, "not deployed", "-", "-", ""))
            continue

        problems = provisioning.verify_registration(client, name)

        if not problems and entry.get("kpis"):
            entry = provisioning.recheck_charts(client, name)

        kpis = entry.get("kpis") or []
        charts = f"{sum(1 for k in kpis if k.get('chart'))}/{len(kpis)}"
        facts = client.deployment_facts(entry["deployment_crn"])
        flow = client.flow_facts(entry["deployment_crn"], entry.get("deployed_flow_crn", ""))
        version = f"v{flow.get('flow_version')}" + (
            " (baseline)" if flow.get("flow_version_crn") == entry.get("baseline_flow_version_crn") else "")
        rows.append((name, facts["state"], version, charts, "; ".join(problems)))

        if problems or not entry.get("kpis_configured"):
            failed.append(name)

    print(f"\n{'pack':22} {'state':18} {'version':15} {'charts':7} problems")

    for name, state, version, charts, problems in rows:
        print(f"{name:22} {state:18} {version:15} {charts:7} {problems}")

    if failed:
        print(f"\nnot ready: {', '.join(failed)}. Missing charts usually appear within a few "
              "minutes of 04; a CRN mismatch means the tenant changed outside these scripts.")
        return 1

    deployed = sum(1 for row in rows if row[1] != "not deployed")
    print(f"\n{deployed} of {len(rows)} pack(s) registered; the app can watch them")
    print(restart_note([name for name, state, *_ in rows if state != "not deployed"]))
    return 0


def restart_note(names: list) -> str:
    """
    Whether the running app knows these packs' scenarios. Its signals
    take their process group from its own build, so a pack newer than the
    app is polled but opens no incident until it restarts (NFH-99).
    """

    app = fleet.running_app()

    if "scenarios" not in app:
        return ("note: the running app does not say which scenarios it knows (no app, or a build before "
                "NFH-99); if a pack here is new to it, restart it (pull_and_restart.py) so it can open incidents")

    started = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(app.get("started", 0)))
    unknown = [fleet.unknown_note(name, found) for name in names
               if (found := fleet.unknown_scenarios(fleet.pack(name), app["scenarios"]))]

    if not unknown:
        return f"the running app (started {started}) knows every scenario of these packs"

    return f"RESTART NEEDED: the running app started {started}. " + "; ".join(unknown)


if __name__ == "__main__":
    sys.exit(main())
