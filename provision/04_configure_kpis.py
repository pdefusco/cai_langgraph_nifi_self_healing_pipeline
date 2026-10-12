#!/usr/bin/env python3
"""
Write each pack's KPIs to its deployment.

    python provision/04_configure_kpis.py
    python provision/04_configure_kpis.py --pack p1-throughput

There is no create-kpi command. The KPI list is replaced whole by
`dfworkload update-flow-in-deployment --kpis`, guarded by
--configuration-version, so this reads the current list, keeps any KPI
someone else configured, replaces ours (reusing their ids), and sends
the lot. A version conflict is re-read once. Then it waits for
kpisDirty=false and finds each KPI's chart, by (componentType,
componentName, chart name), the only identity a chart has.

Charts can lag the write; any not found are reported and
05_register_fleet.py looks again.
"""

import argparse
import sys

import _common

from nifi_healer import fleet, provisioning


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    _common.add_pack_argument(parser)
    args = parser.parse_args()

    _common.banner("04 configure KPIs")
    tenant = _common.Tenant()
    incomplete = []

    for name in _common.selected(args):
        entry = fleet.pack(name)

        if not entry.get("deployed_flow_crn"):
            print(f"[{name}] not deployed; skipped")
            continue

        print(f"[{name}]")
        entry = provisioning.configure_kpis(tenant.client, name,
                                            wait=tenant.settle_wait(entry["deployment_crn"]))
        found = sum(1 for k in entry["kpis"] if k["chart"])
        print(f"  {found}/{len(entry['kpis'])} chart(s) found")

        if not entry["kpis_configured"]:
            incomplete.append(name)

    if incomplete:
        print(f"\ncharts missing for {', '.join(incomplete)}; run provision/05_register_fleet.py "
              "in a few minutes")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
