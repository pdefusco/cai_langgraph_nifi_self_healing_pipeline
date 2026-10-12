#!/usr/bin/env python3
"""
Dump what each deployment offers for KPIs, and check our metric table
against it before 04 writes anything.

    python provision/03_discover_kpi_metadata.py
    python provision/03_discover_kpi_metadata.py --pack p1-throughput

Writes provision/out/kpi_metadata_<pack>.json and reports, for each KPI
the pack declares:

  - whether its metricId is offered for its scope
  - whether its component, looked up by name, has a componentId

The file holds every scope the deployment offers: the flow's, from
get-flow-configuration-metadata-in-deployment (connections, processors,
process groups), and the deployment's, from get-deployment-configuration-
metadata (SYSTEM, NIFI_FLOW).

Every metricId in nifi_healer/kpi/metrics.py was checked against CDF with
NiFi 2.6.0. When a tenant offers different ones, the report lists the
scope's offered ids with names that look close; put the right one in
provision/out/metric_overrides.json:

    {"flowfiles_out": {"metric_id": "processorAmountFlowFilesSent"},
     "cpu": {"metric_id": "nifiMemoryUtilization", "unit_id": "percentage"}}

The table in nifi_healer/kpi/metrics.py picks the file up without a
restart, and so do the mock tenant and 04.
"""

import argparse
import json
import os
import re
import sys

import _common
from _common import config, packs

from nifi_healer import fleet, provisioning
from nifi_healer.kpi import metrics


def words(text: str) -> set:
    return {w.lower() for w in re.findall(r"[A-Z]?[a-z]+|[A-Z]+(?![a-z])", text or "") if len(w) > 2}


def describe(metric_type: dict) -> str:
    label = metric_type.get("label") or metric_type.get("name") or metric_type.get("displayName") or ""
    unit = metric_type.get("unitTypeKey") or metric_type.get("unitType") or metric_type.get("defaultUnitId") or ""

    detail = ", ".join(part for part in (label, unit) if part)

    return f"{metric_type.get('id')}  ({detail})" if detail else str(metric_type.get("id"))


def close_matches(logical: str, wanted: metrics.Metric, offered: list) -> list:
    target = words(wanted.metric_id) | words(wanted.label) | set(logical.split("_"))
    scored = []

    for metric_type in offered:
        overlap = len(target & (words(metric_type.get("id")) | words(describe(metric_type))))

        if overlap:
            scored.append((overlap, describe(metric_type)))

    return [text for _, text in sorted(scored, reverse=True)[:4]]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    _common.add_pack_argument(parser)
    args = parser.parse_args()

    _common.banner("03 discover KPI metadata")
    client = _common.Tenant().client
    os.makedirs(config.PROVISION_OUT_DIR, exist_ok=True)
    problems = 0

    for name in _common.selected(args):
        entry = fleet.pack(name)

        if not entry.get("deployed_flow_crn"):
            print(f"[{name}] not deployed; skipped")
            continue

        metadata = provisioning.kpi_metadata(client, entry["deployment_crn"], entry["deployed_flow_crn"])
        path = os.path.join(config.PROVISION_OUT_DIR, f"kpi_metadata_{name}.json")

        with open(path, "w") as handle:
            json.dump(metadata, handle, indent=2)

        scopes = {s.get("type"): s for s in (metadata.get("kpiMetaData") or {}).get("kpiScopes") or []}
        ids = provisioning.component_ids(metadata)
        print(f"[{name}] {path}")

        for scope, body in scopes.items():
            print(f"    {scope}: {len(body.get('metricTypes') or [])} metric type(s), "
                  f"{sum(len(g.get('scopeComponents') or []) for g in body.get('contextGroups') or [])} component(s)")

        for kpi in packs.PACKS[name].kpis:
            wanted = metrics.metric(kpi.metric)
            scope = metrics.KIND_SCOPES[kpi.kind]
            offered = (scopes.get(scope) or {}).get("metricTypes") or []
            notes = []

            if wanted.metric_id not in {m.get("id") for m in offered}:
                notes.append(f"{wanted.metric_id} NOT OFFERED for {scope}")
                suggestions = close_matches(kpi.metric, wanted, offered)

                if suggestions:
                    notes.append("closest: " + "; ".join(suggestions))

            if scope != "SYSTEM" and (scope, kpi.component) not in ids:
                notes.append(f"no {scope} named {kpi.component!r}")

            status = "ok" if not notes else "FIX"
            problems += bool(notes)
            print(f"  [{status:3}] {kpi.key:46} {wanted.metric_id}"
                  f"{'' if wanted.verified else '  (unverified)'}")

            for note in notes:
                print(f"          {note}")

    if problems:
        print(f"\n{problems} KPI(s) need attention. Correct {config.METRIC_OVERRIDES_FILE} "
              "(or the scenario's component name) and re-run this before 04.")
        return 1

    print("\nall KPIs resolvable; next: provision/04_configure_kpis.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
