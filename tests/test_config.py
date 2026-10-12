import os
import subprocess
import sys

import config

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

LIVE_CRNS = {
    "SELFHEAL_SERVICE_CRN": "crn:cdp:df:us-west-1:acct:service:real",
    "SELFHEAL_ENVIRONMENT_CRN": "crn:cdp:environments:us-west-1:acct:environment:real",
}


def crns_with(env: dict) -> list[str]:
    # config reads the environment once at import, so import it fresh.
    out = subprocess.check_output(
        [sys.executable, "-c", "import config; print(config.SERVICE_CRN); print(config.ENVIRONMENT_CRN)"],
        cwd=REPO, env={**os.environ, **env}, text=True)
    return out.split()


def test_mock_ignores_live_crns_in_the_environment():
    assert crns_with({**LIVE_CRNS, "SELFHEAL_DF_CLIENT": "mock"}) == [
        config.MOCK_SERVICE_CRN, config.MOCK_ENVIRONMENT_CRN]


def test_live_uses_the_environment_crns():
    assert crns_with({**LIVE_CRNS, "SELFHEAL_DF_CLIENT": "live"}) == [
        LIVE_CRNS["SELFHEAL_SERVICE_CRN"], LIVE_CRNS["SELFHEAL_ENVIRONMENT_CRN"]]
