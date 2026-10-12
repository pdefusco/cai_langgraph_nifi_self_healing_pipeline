#!/usr/bin/env python3
"""
Prove this runtime can do everything the app needs, before anything is
created or armed.

    python provision/00_selftest.py

Checks, in order:

  1. Python and cdpcli versions (cdpcli's metadata stops at 3.9; this is
     where 3.12 is actually exercised)
  2. the settings: service and environment CRNs, prefix, NiFi version,
     ARMED (credentials are reported as present or absent, never shown)
  3. the control plane (df list-deployments)
  4. the workload plane (dfworkload list-nifi-versions), and that the
     configured NiFi version is offered
  5. the workload endpoint accepts TCP on 443 (it is a private load
     balancer: from outside the environment's VPC every write fails)
  6. the deployments in the fleet file still exist

Exits 1 if any of 3-5 fail.
"""

import argparse
import json
import os
import platform
import sys
from importlib import metadata

import _common
from _common import config

from nifi_healer import fleet
from nifi_healer.cdp.client import CDP_CREDENTIALS_FILE


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--json", action="store_true", help="print the report as JSON")
    args = parser.parse_args()

    tenant = _common.Tenant()
    client = tenant.client

    try:
        cdpcli = metadata.version("cdpcli")

    except metadata.PackageNotFoundError:
        cdpcli = None

    report = {
        "python": platform.python_version(),
        "cdpcli": cdpcli,
        "settings": {
            "client": config.DF_CLIENT,
            "service_crn": config.SERVICE_CRN or None,
            "environment_crn": config.ENVIRONMENT_CRN or None,
            "prefix": config.PREFIX,
            "nifi_version": config.NIFI_VERSION,
            "armed": config.ARMED,
            "credentials": _credentials(),
        },
        "tenant": client.selftest(),
        "workload_reachability": client.workload_unreachable() or "ok",
        "fleet": {},
    }

    for name, entry in fleet.read()["packs"].items():
        crn = entry.get("deployment_crn")
        report["fleet"][name] = client.deployment_facts(crn)["state"] if crn else "not deployed"

    ok = report["tenant"]["ok"] and report["workload_reachability"] == "ok" and cdpcli is not None
    report["ok"] = ok

    if args.json:
        print(json.dumps(report, indent=2))

        return 0 if ok else 1

    print(f"python {report['python']}   cdpcli {cdpcli or 'NOT INSTALLED'}")

    for key, value in report["settings"].items():
        print(f"  {key:16} {value}")

    tenant_report = report["tenant"]

    for plane in ("control_plane", "workload_plane"):
        result = tenant_report[plane]
        status = "ok" if result["ok"] else "FAIL"
        detail = {k: v for k, v in result.items() if k != "ok"}
        print(f"[{status:4}] {plane:15} {json.dumps(detail)}")

    workload = tenant_report["workload_plane"]

    if workload.get("ok") and not workload.get("configured_nifi_version_available"):
        print(f"[warn] NiFi {config.NIFI_VERSION} is not offered; set SELFHEAL_NIFI_VERSION "
              "and rebuild the flows (bundle versions follow it).")

    print(f"[{'ok' if report['workload_reachability'] == 'ok' else 'FAIL':4}] "
          f"workload endpoint {tenant_report.get('workload_endpoint')}: {report['workload_reachability']}")

    for name, state in report["fleet"].items():
        print(f"       fleet {name:22} {state}")

    if not report["fleet"]:
        print("       fleet is empty: run 01, 02 and 04 next")

    print("\nok" if ok else "\nFAILED")

    return 0 if ok else 1


def _credentials() -> str:
    """Where credentials would come from. Never their values."""

    if config.MOCK:
        return "not needed (mock)"

    if os.environ.get("CDP_ACCESS_KEY_ID") and os.environ.get("CDP_PRIVATE_KEY"):
        return "CDP_ACCESS_KEY_ID / CDP_PRIVATE_KEY"

    if os.path.exists(CDP_CREDENTIALS_FILE):
        return CDP_CREDENTIALS_FILE

    return "NONE FOUND"


if __name__ == "__main__":
    sys.exit(main())
