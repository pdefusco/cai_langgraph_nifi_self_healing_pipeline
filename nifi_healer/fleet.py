"""
The fleet registry: which deployments this app owns, and their CRNs.

Written by the provisioning scripts (01, 02, 04, 05) to
`DATA_DIR/fleet.json`. It is the *independent expectation* the guardrails
compare against: a CRN recorded when we created something, not a value
read back from the thing we are about to mutate.

    {
      "packs": {
        "p1-throughput": {
          "flow_name": "nfh-p1-throughput",
          "flow_crn": "...",
          "baseline_flow_version_crn": "...",   # the faulty version, for resets
          "deployment_name": "nfh-p1-throughput",
          "deployment_crn": "...",
          "deployed_flow_crn": "...",
          "kpis": [{"scenario": "S01", "chart": [type, name, label], ...}],
          "kpis_configured": true
        }
      }
    }
"""

import json
import os
import threading

import config

_LOCK = threading.RLock()


def read() -> dict:
    with _LOCK:
        if not os.path.exists(config.FLEET_FILE):
            return {"packs": {}}

        with open(config.FLEET_FILE) as handle:
            data = json.load(handle)

        data.setdefault("packs", {})

        return data


def write(data: dict) -> None:
    with _LOCK:
        os.makedirs(os.path.dirname(config.FLEET_FILE), exist_ok=True)
        tmp = config.FLEET_FILE + ".tmp"

        with open(tmp, "w") as handle:
            json.dump(data, handle, indent=2, sort_keys=True)

        os.replace(tmp, config.FLEET_FILE)


def pack(name: str) -> dict:
    return read()["packs"].get(name) or {}


def update_pack(name: str, **fields) -> dict:
    with _LOCK:
        data = read()
        entry = data["packs"].setdefault(name, {})
        entry.update(fields)
        write(data)

        return entry


def pack_for_deployment(deployment_crn: str) -> str:
    """The pack whose recorded deployment CRN is this one, else ""."""

    for name, entry in read()["packs"].items():
        if deployment_crn and entry.get("deployment_crn") == deployment_crn:
            return name

    return ""


def deployed_packs() -> dict:
    """Packs that have both a deployment and a deployed flow recorded."""

    return {
        name: entry
        for name, entry in read()["packs"].items()
        if entry.get("deployment_crn") and entry.get("deployed_flow_crn")
    }
