"""Every command line this project builds is one the installed cdpcli accepts."""

import json

import pytest

from flows import packs
from nifi_healer.cdp import cli_spec, guardrails
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
