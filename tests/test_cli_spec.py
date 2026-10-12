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


def test_cdpcli_prints_the_decoded_flow_definition():
    # flow_definition and the mock assume the installed cdpcli decodes
    # getFlowVersion's base64 field client-side and prints the flow JSON.
    import inspect
    import cdpcli.extensions.df as df_extensions

    source = inspect.getsource(df_extensions)
    assert "operation_name == 'getFlowVersion'" in source
    assert "b64decode(response.get('flowDefinition'))" in source
