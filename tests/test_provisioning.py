"""
Provisioning on a mock tenant: a rebuilt baseline imported as a new
version, and the flow-only reset that deploys it without undoing a fix.
"""

import importlib
import json
import os
import sys

import config
from flows.builder.lib import iter_processors
from flows.scenarios.p5_parameters import CORRECT_URL
from nifi_healer import fleet, provisioning
from nifi_healer.ui import gradio_app


def test_05_says_when_the_running_app_needs_a_restart(tenant, tmp_path):
    # A pack newer than the app is polled but opens no incident until it restarts (NFH-99).
    tenant("p1-throughput", "p23-siblings")
    sys.path.insert(0, os.path.join(config.REPO_ROOT, "provision"))
    register = importlib.import_module("05_register_fleet")
    names = ["p1-throughput", "p23-siblings"]

    assert "does not say which scenarios it knows" in register.restart_note(names)

    gradio_app._claim_data_dir()
    assert "knows every scenario of these packs" in register.restart_note(names)

    owner = json.loads((tmp_path / "app.owner").read_text())
    (tmp_path / "app.owner").write_text(json.dumps(dict(owner, scenarios=[f"S{n:02d}" for n in range(1, 51)])))
    note = register.restart_note(names)

    assert note.startswith("RESTART NEEDED: the running app started ")
    assert "p23-siblings: this build of the app does not know S51 … S77 (27 scenarios)" in note
    assert "p1-throughput" not in note


def test_a_new_baseline_is_deployed_without_undoing_a_parameter_fix(tenant, monkeypatch):
    # Live: S17's InvokeHTTP had its connect timeout under a key NiFi sent as a header,
    # so a rebuilt v2 had to replace v1 without putting the wrong URL back.
    world, client, _ = tenant("p5-parameters")
    monkeypatch.setattr(config, "ARMED", True)
    entry = fleet.pack("p5-parameters")
    deployment, flow, first = entry["deployment_crn"], entry["deployed_flow_crn"], entry["baseline_flow_version_crn"]

    configuration = client.flow_configuration(deployment, flow)
    group = configuration["parameterGroups"][0]["name"]
    client.dfw("update-flow-in-deployment", "--deployment-crn", deployment, "--deployed-flow-crn", flow,
               "--configuration-version", configuration["configurationVersion"], "--parameter-groups",
               json.dumps([{"name": group, "parameters": [{"name": "s17.target.url", "value": CORRECT_URL}]}]))
    world.settle(deployment)

    assert provisioning.reset_steps(client, "p5-parameters") and \
        not provisioning.reset_steps(client, "p5-parameters", flow_only=True)

    baseline = provisioning.import_flow(client, "p5-parameters", new_baseline=True)["baseline_flow_version_crn"]
    assert baseline and baseline != first

    steps = provisioning.reset_steps(client, "p5-parameters", flow_only=True)
    assert [kind for _, _, kind, _ in steps] == ["version"] and steps[0][3] == baseline

    world.skip(31 * 60)  # past the deployment's cooldown
    records = provisioning.reset_pack(client, "p5-parameters", dry_run=False, flow_only=True,
                                      wait=lambda: world.settle(deployment))
    assert [r["step"] for r in records] == ["flow version v1 -> baseline"]
    assert client.flow_facts(deployment, flow)["flow_version_crn"] == baseline

    values = {p["name"]: p["value"] for g in client.flow_configuration(deployment, flow)["parameterGroups"]
              for p in g["parameters"]}
    assert values["s17.target.url"] == CORRECT_URL

    call = next(p for _, _, p in iter_processors(client.flow_definition(baseline))
                if p["name"] == "S17 Partner Call")
    assert "Connection Timeout" in call["properties"] and "Socket Connect Timeout" not in call["properties"]


def test_a_flow_without_a_fleet_record_takes_its_newest_baseline(tenant):
    # Live: p3 and p5 were found in the catalog with no record, took v1 (built before NFH-11) and ran it.
    _, client, _ = tenant("p5-parameters")
    rebuilt = provisioning.import_flow(client, "p5-parameters", new_baseline=True)["baseline_flow_version_crn"]
    crn = fleet.pack("p5-parameters")["flow_crn"]

    # A fix the app imported later is not a baseline.
    client.df("import-flow-definition-version", "--flow-crn", crn,
              "--file", provisioning.definition_file("p5-parameters"), "--comments", "nfh: a fix")
    fleet.update_pack("p5-parameters", baseline_flow_version_crn=None)

    assert provisioning.import_flow(client, "p5-parameters")["baseline_flow_version_crn"] == rebuilt
    assert provisioning._newest_baseline([{"crn": "a", "version": 2, "comments": "nfh"},
                                          {"crn": "b", "version": 1, "comments": ""}])["crn"] == "b"
