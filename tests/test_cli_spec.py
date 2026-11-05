"""Every command line this project builds is one the installed cdpcli accepts."""

import base64
import json

import pytest

from flows import packs
from nifi_healer.cdp import cli_spec, guardrails
from nifi_healer.cdp.client import Client
from nifi_healer.cdp.mock_client import parse_options
from nifi_healer import provisioning


def accepted(argv: list) -> str:
    plane, command, *rest = [str(a) for a in argv]

    return cli_spec.check(plane, command, parse_options(rest))


@pytest.mark.parametrize("pack", sorted(packs.PACKS))
def test_create_deployment(pack):
    argv = ["df"] + provisioning.create_deployment_args(pack, "crn:fv")
    assert accepted(argv) == ""


@pytest.mark.parametrize("listen", [False, True])
def test_change_flow_version(listen):
    argv = ["df"] + guardrails.change_flow_version_args("crn:d", "crn:f", "crn:fv", listen)
    assert accepted(argv) == ""
    assert "--kpis" not in argv and "STOP_AND_EMPTY_QUEUES" not in argv


def test_workload_writes():
    common = ["--environment-crn", "crn:e", "--deployment-crn", "crn:d"]
    assert accepted(["dfworkload", "update-flow-in-deployment", *common, "--deployed-flow-crn", "crn:f",
                     "--configuration-version", 3, "--parameter-groups", json.dumps([])]) == ""
    assert accepted(["dfworkload", "update-deployment", *common, "--configuration-version", 1,
                     "--cluster-size", json.dumps({"name": "SMALL"}), "--static-node-count", 2]) == ""
    assert accepted(["dfworkload", "terminate-deployment", *common]) == ""


def test_rejections():
    assert "dfworkload command" in accepted(["df", "terminate-deployment", "--deployment-crn", "x"])
    assert "unknown option" in accepted(["df", "list-deployments", "--max-itemz", 5])
    assert "required" in accepted(["dfworkload", "update-deployment", "--environment-crn", "e"])
    assert accepted(["df", "list-deployments", "--max-items", 500]) == ""


class Canned(Client):
    def __init__(self, response):
        self.response = response

    def run(self, args):
        return self.response


DEFINITION = {"flowContents": {"name": "nfh-x", "processors": []}, "parameterContexts": {}}


@pytest.mark.parametrize("response", [
    DEFINITION,                                                          # what cdpcli prints
    {"flowDefinition": json.dumps(DEFINITION)},                          # API field as JSON
    {"flowDefinition": base64.b64encode(json.dumps(DEFINITION).encode()).decode()},  # raw API
])
def test_flow_definition_shapes(response):
    assert Canned(response).flow_definition("crn:fv") == DEFINITION


VERSION_CRN = "crn:cdp:df:us-west-1:tenant:flow:nfh-x/v.1"
VERSION_ID = "f049af2a-5e87-44c0-a7d1-2d08efd87d4e"

# What a live get-flow-version printed for p11's 71 KB definition (NFH-56).
CUT = {"error": "The CDP CLI returned an error.", "return_code": 255,
       "stderr": "Unterminated string starting at: line 1718 column 25 (char 65535)"}


class Catalog(Client):
    """get-flow-version answers `flow_version`; the console download answers `console`."""

    def __init__(self, flow_version, console=DEFINITION):
        self.flow_version, self.console, self.calls = flow_version, console, []

    def run(self, args):
        self.calls.append(args)

        if args[1] == "get-flow-version":
            return self.flow_version

        return {"flowVersions": [{"crn": VERSION_CRN, "bucketIdentifier":
                                  f"https://s3.us-west-2.amazonaws.com/dfx-flow-artifacts/tenant/{VERSION_ID}"}]}

    def console_get(self, path):
        self.calls.append(path)
        return self.console


CUT_MID_CHARACTER = dict(CUT, stderr="'utf-8' codec can't decode byte 0xc3 in position 65535: "
                                     "unexpected end of data")


@pytest.mark.parametrize("flow_version", [CUT, CUT_MID_CHARACTER, {"flowDefinition": "eyJmbG93Q29udGVudHMiOiB7"}])
def test_a_definition_cut_short_is_downloaded_from_the_console(flow_version):
    client = Catalog(flow_version)

    assert client.flow_definition(VERSION_CRN) == DEFINITION
    assert client.calls[1] == ["df", "list-flow-definition-versions", "--flow-crn",
                               "crn:cdp:df:us-west-1:tenant:flow:nfh-x"]
    assert client.calls[2] == f"/dfx/api/v1/catalog/flows/versions/{VERSION_ID}"


def test_the_console_is_only_a_fallback():
    client = Catalog(DEFINITION)
    assert client.flow_definition(VERSION_CRN) == DEFINITION
    assert len(client.calls) == 1

    # Any other failure is the answer: no second read can fix a missing version.
    missing = {"error": "The CDP CLI returned an error.", "return_code": 254,
               "stderr": "An error occurred: Flow version not found. (Status Code: 404; Error Code: NOT_FOUND)"}
    client = Catalog(missing)
    assert client.flow_definition(VERSION_CRN) == missing
    assert len(client.calls) == 1


def test_a_failed_fallback_says_why():
    found = Catalog(CUT, console={"error": "The CDP console answered 403.", "detail": "Forbidden"})
    found = found.flow_definition(VERSION_CRN)

    assert found["error"] == CUT["error"] and found["stderr"] == CUT["stderr"]
    assert "NFH-56" in found["hint"] and found["detail"] == "The CDP console answered 403. Forbidden"


def test_the_mock_cuts_and_downloads_as_live(tenant):
    _, client, _ = tenant("p11-payments", "p9-clickstream")
    large = provisioning.fleet.pack("p11-payments")["baseline_flow_version_crn"]
    small = provisioning.fleet.pack("p9-clickstream")["baseline_flow_version_crn"]

    assert "(char 65535)" in client.get_flow_version(large)["stderr"]
    assert "flowContents" in client.get_flow_version(small)
    assert client.flow_definition(large) == client.world.version(large)["definition"]


def test_cdpcli_prints_the_decoded_flow_definition():
    # flow_definition and the mock assume the installed cdpcli decodes
    # getFlowVersion's base64 field client-side and prints the flow JSON.
    import inspect
    import cdpcli.extensions.df as df_extensions

    source = inspect.getsource(df_extensions)
    assert "operation_name == 'getFlowVersion'" in source
    assert "b64decode(response.get('flowDefinition'))" in source


def test_system_kpis_go_through_update_deployment(tenant):
    # update-deployment --kpis takes only deployment-level KPIs and
    # update-flow-in-deployment the rest; each replaces only its own list.
    world, client, _ = tenant("p6-sizing-cpu")
    entry = provisioning.fleet.pack("p6-sizing-cpu")
    deployment, flow = entry["deployment_crn"], entry["deployed_flow_crn"]

    assert [k["metricComponentType"] for k in client.deployment_configuration(deployment)["kpis"]] == ["SYSTEM"]
    # NIFI_PROCESSOR: the chart-only KPIs at S20's edges, for its in/out roll-up.
    assert {k["metricComponentType"] for k in client.flow_configuration(deployment, flow)["kpis"]} == \
        {"NIFI_CONNECTION", "NIFI_PROCESSOR"}
    assert all(k["chart"] for k in entry["kpis"])

    scopes = {s["type"] for s in client.flow_configuration_metadata(deployment, flow)["kpiMetaData"]["kpiScopes"]}
    assert "SYSTEM" not in scopes
    assert "SYSTEM" in {s["type"] for s in provisioning.kpi_metadata(client, deployment, flow)["kpiMetaData"]["kpiScopes"]}

    # Re-running replaces ours in place: no duplicates on either side.
    provisioning.configure_kpis(client, "p6-sizing-cpu", wait=lambda: world.settle(deployment))
    assert len(client.deployment_configuration(deployment)["kpis"]) == 1
    assert len(client.flow_configuration(deployment, flow)["kpis"]) == 3

    # A level already configured as wanted is not written again: update-deployment updates the deployment.
    writes, dfw = [], client.dfw

    def spy(*argv):
        if argv[0].startswith("update-"):
            writes.append(argv[0])

        return dfw(*argv)

    client.dfw = spy
    provisioning.configure_kpis(client, "p6-sizing-cpu", wait=lambda: world.settle(deployment))
    assert writes == []

    configuration = client.flow_configuration(deployment, flow)
    configuration["kpis"] = configuration["kpis"][:2]
    assert not provisioning.already_configured(configuration["kpis"], [p for _, p in provisioning.kpi_payloads(
        "p6-sizing-cpu", provisioning.kpi_metadata(client, deployment, flow))])

    assert accepted(["dfworkload", "update-deployment", "--environment-crn", "crn:e",
                     "--deployment-crn", "crn:d", "--configuration-version", 1,
                     "--kpis", json.dumps([{"metricId": "cpuUtilization",
                                            "metricComponentType": "SYSTEM"}])]) == ""
    assert accepted(["dfworkload", "get-deployment-configuration-metadata", "--environment-crn", "crn:e",
                     "--deployment-crn", "crn:d"]) == ""
