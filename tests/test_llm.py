"""
The chat model's token: minted with the endpoint tenant's key, renewed
before it expires, the client rebuilt with it; and probe() against the
endpoint. No CLI or network: both are faked.
"""

import base64
import io
import json
import subprocess
import urllib.error

import pytest

import config
from nifi_healer import llm

HOUR = 3600.0


class Clock:
    def __init__(self, now=1_000_000.0):
        self.now = now

    def __call__(self):
        return self.now


def iso(seconds: float) -> str:
    import datetime

    return datetime.datetime.fromtimestamp(seconds, datetime.timezone.utc).isoformat()


@pytest.fixture
def minted(monkeypatch):
    """(clock, mints): llm's token minted by a fake CLI, each mint an hour long."""

    clock, mints = Clock(), []

    def mint():
        mints.append(clock.now)

        return {"token": f"tok-{len(mints)}", "expireAt": iso(clock.now + HOUR)}

    monkeypatch.setattr(config, "LLM_PROVIDER", "cai")
    monkeypatch.setattr(config, "LLM_MODEL_ID", "test-model")
    monkeypatch.setattr(config, "LLM_ENDPOINT_BASE_URL", "https://endpoint.example/v1")
    monkeypatch.setattr(config, "LLM_CDP_TOKEN", "stale-pasted-token")
    monkeypatch.setattr(config, "LLM_CDP_ACCESS_KEY_ID", "key-id")
    monkeypatch.setattr(config, "LLM_CDP_PRIVATE_KEY", "private-key")
    monkeypatch.setattr(config, "LLM_TOKEN_RENEW_SECONDS", 300)
    monkeypatch.setattr(llm, "_minted", llm.MintedToken(mint, clock))
    monkeypatch.setattr(llm.time, "time", clock)
    llm.set_chat_model(None)

    yield clock, mints

    llm.set_chat_model(None)


def test_a_token_is_reused_until_shortly_before_it_expires(minted):
    clock, mints = minted

    assert llm.chat_token() == "tok-1"
    clock.now += HOUR - 301
    assert llm.chat_token() == "tok-1" and len(mints) == 1

    clock.now += 2
    assert llm.chat_token() == "tok-2" and len(mints) == 2


def test_the_minted_token_wins_over_a_pasted_one(minted, monkeypatch):
    assert llm.chat_token() == "tok-1"

    monkeypatch.setattr(config, "LLM_CDP_ACCESS_KEY_ID", "")
    monkeypatch.setattr(config, "LLM_CDP_PRIVATE_KEY", "")
    assert llm.chat_token() == "stale-pasted-token"


def test_half_a_key_is_named(minted, monkeypatch):
    monkeypatch.setattr(config, "LLM_CDP_PRIVATE_KEY", "")

    ok, reason = llm.chat_available()
    assert not ok and "LLM_CDP_PRIVATE_KEY" in reason


def test_a_failed_mint_is_not_retried_for_a_minute(monkeypatch):
    clock, calls = Clock(), []

    def mint():
        calls.append(clock.now)

        raise llm.LLMUnavailable("could not mint a token: Access denied")

    token = llm.MintedToken(mint, clock)

    for _ in range(3):
        with pytest.raises(llm.LLMUnavailable, match="Access denied"):
            token.get()

    assert len(calls) == 1

    clock.now += 61
    with pytest.raises(llm.LLMUnavailable):
        token.get()

    assert len(calls) == 2


def test_expiry_falls_back_to_the_jwt_exp_claim():
    claims = base64.urlsafe_b64encode(json.dumps({"exp": 1_234_567}).encode()).decode().rstrip("=")

    assert llm._expiry({"token": f"h.{claims}.s"}, 0) == 1_234_567
    assert llm._expiry({"token": "not-a-jwt"}, 100) == 100 + 1800
    assert llm._expiry({"expireAt": "2026-10-12T17:03:46.356000+00:00"}, 0) == pytest.approx(1791824626.356)


def test_the_client_is_rebuilt_with_each_new_token(minted, monkeypatch):
    clock, _ = minted
    built = []
    monkeypatch.setattr(llm, "_build_chat", lambda token: built.append(token) or object())

    first = llm.chat_model()
    assert llm.chat_model() is first and built == ["tok-1"]

    clock.now += HOUR
    assert llm.chat_model() is not first and built == ["tok-1", "tok-2"]

    # An installed model is used as is, whatever the token.
    fixed = object()
    llm.set_chat_model(fixed)
    clock.now += HOUR
    assert llm.chat_model() is fixed and built == ["tok-1", "tok-2"]


def test_mint_runs_the_cli_with_the_endpoint_tenants_key(monkeypatch):
    seen = {}

    def run(command, capture_output, text, timeout, env):
        seen.update(command=command, env=env)

        return subprocess.CompletedProcess(command, 0, stdout=json.dumps(
            {"token": "tok", "expireAt": "2026-10-12T17:03:46+00:00"}), stderr="")

    monkeypatch.setattr(config, "LLM_CDP_ACCESS_KEY_ID", "key-id")
    monkeypatch.setattr(config, "LLM_CDP_PRIVATE_KEY", "private-key")
    monkeypatch.setattr(config, "LLM_CDP_REGION", "us-west-1")
    monkeypatch.setenv("CDP_ACCESS_KEY_ID", "the-apps-own-key")
    monkeypatch.setenv("CDP_PROFILE", "default")
    monkeypatch.setattr(llm.subprocess, "run", run)

    assert llm._run_mint()["token"] == "tok"
    assert seen["command"][3:] == ["iam", "generate-workload-auth-token", "--workload-name", "DE",
                                   "--output", "json", "--cdp-region", "us-west-1"]
    assert seen["env"]["CDP_ACCESS_KEY_ID"] == "key-id" and seen["env"]["CDP_PRIVATE_KEY"] == "private-key"
    assert "CDP_PROFILE" not in seen["env"]


def test_a_cli_error_reports_its_last_line(monkeypatch):
    monkeypatch.setattr(llm.subprocess, "run", lambda command, **_: subprocess.CompletedProcess(
        command, 255, stdout="", stderr="a warning\nAn error occurred: Access denied (Status Code: 403)\n"))

    with pytest.raises(llm.LLMUnavailable, match="mint a token: An error occurred: Access denied"):
        llm._run_mint()


def _http_error(code, body):
    return urllib.error.HTTPError("https://endpoint.example/v1/models", code, "", {}, io.BytesIO(body))


def test_probe_reports_a_rejected_token_and_rechecks_after_a_minute(minted, monkeypatch):
    clock, _ = minted
    answers, seen = [_http_error(401, b'{"error": "Token has expired"}'), _http_error(404, b"")], []
    monkeypatch.setattr(llm, "_build_chat", lambda token: object())

    def urlopen(request, timeout):
        seen.append((request.full_url, request.get_header("Authorization")))

        raise answers.pop(0)

    monkeypatch.setattr(llm.urllib.request, "urlopen", urlopen)

    ok, reason = llm.probe()
    assert not ok and "401" in reason and "Token has expired" in reason
    assert seen == [("https://endpoint.example/v1/models", "Bearer tok-1")]

    assert llm.probe() == (ok, reason) and len(seen) == 1

    # 404: no /models route, but the token got past the endpoint's auth.
    clock.now += 61
    assert llm.probe() == (True, "") and len(seen) == 2


def test_probe_skips_an_installed_model():
    llm.set_chat_model(object())

    try:
        assert llm.probe() == (True, "")

    finally:
        llm.set_chat_model(None)
