"""
The one place that knows which model serves the agent and the retriever.

    llm = chat_model()                  # a LangChain chat model
    vectors = embedder().passages(texts)
    vector = embedder().query(text)

SELFHEAL_LLM_PROVIDER picks the chat model:

  cai     CAI Inference Service, OpenAI-compatible (LLM_MODEL_ID,
          LLM_ENDPOINT_BASE_URL, and a token: see below)

SELFHEAL_EMBEDDING_PROVIDER picks the embedder:

  cai     the CAI embedding endpoint (EMBEDDING_*). E5-style models embed
          documents and questions differently, so passages are sent with
          input_type "passage" and queries with "query".
  hash    a local hashed bag-of-words, for tests and offline mock runs.

Another provider is another branch here; nothing else imports a client.
Tests install their own with set_chat_model() / set_embedder().

The chat model's token, first match wins:

  1. LLM_CDP_ACCESS_KEY_ID + LLM_CDP_PRIVATE_KEY, a CDP API key from the
     endpoint's tenant: tokens are minted with it and renewed before
     they expire, and the client is rebuilt with each new one.
  2. LLM_CDP_TOKEN, as pasted. It lasts an hour.
  3. The CAI session's /tmp/jwt, which only works when the endpoint is
     in the workbench's own tenant.

probe() asks the endpoint whether it accepts the token. Tokens are only
ever handed to the client, never logged.
"""

import base64
import datetime
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

import config
from nifi_healer.runtime import telemetry

_JWT_FILE = "/tmp/jwt"

# Seconds a probe result, or a failed mint, is reused before trying again.
_RECHECK_SECONDS = 60

_lock = threading.Lock()
_chat = None
_chat_token = None
_chat_fixed = False
_embedder = None
_probe = (0.0, None)


class LLMUnavailable(RuntimeError):
    """The configured provider cannot be used (missing endpoint, model or token)."""


def log(msg: str) -> None:
    print(f"[llm] {msg}", flush=True)


def _token(value: str) -> str:
    if value:
        return value

    try:
        with open(_JWT_FILE) as handle:
            return json.load(handle).get("access_token", "")

    except (OSError, ValueError):
        return ""


def _require(**settings) -> None:
    missing = [name for name, value in settings.items() if not value]

    if missing:
        raise LLMUnavailable(f"not configured: {', '.join(missing)}")


# ---------------------------------------------------------------------
# Minted tokens
# ---------------------------------------------------------------------


def _expiry(data: dict, now: float) -> float:
    """expireAt from the CLI, else the JWT's exp claim, else half an hour."""

    try:
        return datetime.datetime.fromisoformat(data["expireAt"].replace("Z", "+00:00")).timestamp()

    except (KeyError, TypeError, ValueError, AttributeError):
        pass

    try:
        claims = data["token"].split(".")[1]
        claims += "=" * (-len(claims) % 4)

        return float(json.loads(base64.urlsafe_b64decode(claims))["exp"])

    except (KeyError, IndexError, TypeError, ValueError, AttributeError):
        return now + 1800


def _run_mint() -> dict:
    """`cdp iam generate-workload-auth-token` with the endpoint tenant's key."""

    # Through `-c`, as the DataFlow client does: cdpcli.clidriver has no
    # __main__ guard.
    command = [sys.executable, "-c",
               "import sys; from cdpcli.clidriver import main; sys.exit(main())",
               "iam", "generate-workload-auth-token",
               "--workload-name", config.LLM_TOKEN_WORKLOAD, "--output", "json"]

    if config.LLM_CDP_REGION:
        command += ["--cdp-region", config.LLM_CDP_REGION]

    # The key goes in the subprocess env only. It overrides the app's own
    # CDP key (another tenant) and any profile.
    environment = dict(os.environ, CDP_ACCESS_KEY_ID=config.LLM_CDP_ACCESS_KEY_ID,
                       CDP_PRIVATE_KEY=config.LLM_CDP_PRIVATE_KEY)
    environment.pop("CDP_PROFILE", None)

    try:
        completed = subprocess.run(command, capture_output=True, text=True,
                                   timeout=config.CDP_TIMEOUT_SECONDS, env=environment)

    except (subprocess.TimeoutExpired, OSError) as exc:
        raise LLMUnavailable(f"could not mint a token: {type(exc).__name__}") from None

    if completed.returncode != 0:
        lines = [line for line in (completed.stderr or "").strip().splitlines() if line.strip()]

        raise LLMUnavailable("could not mint a token: " + (lines[-1][:200] if lines else
                                                           f"exit {completed.returncode}"))

    try:
        data = json.loads(completed.stdout)

    except ValueError:
        raise LLMUnavailable("could not mint a token: the CLI did not print JSON") from None

    if not isinstance(data, dict) or not data.get("token"):
        raise LLMUnavailable("could not mint a token: no token in the CLI's reply")

    return data


class MintedToken:
    """
    The current workload token, minted on first use and again once it is
    within LLM_TOKEN_RENEW_SECONDS of expiring. A failed mint is reused
    for a minute, so a bad key doesn't spawn a CLI per call.
    """

    def __init__(self, mint=None, clock=time.time):
        self._mint = mint or _run_mint
        self._clock = clock
        self._lock = threading.Lock()
        self._value = ""
        self._renew_at = 0.0
        self._failure = None
        self._retry_at = 0.0

    def get(self) -> str:
        with self._lock:
            now = self._clock()

            if self._value and now < self._renew_at:
                return self._value

            if self._failure is not None and now < self._retry_at:
                raise self._failure

            try:
                data = self._mint()

            except LLMUnavailable as exc:
                self._failure, self._retry_at = exc, now + _RECHECK_SECONDS
                raise

            expires = _expiry(data, now)
            self._value, self._failure = data["token"], None
            # Renew ahead of expiry; a short-lived token is renewed halfway.
            self._renew_at = max(expires - config.LLM_TOKEN_RENEW_SECONDS, now + (expires - now) / 2)
            log(f"minted a workload token, valid until "
                f"{datetime.datetime.fromtimestamp(expires, datetime.timezone.utc):%H:%M:%S} UTC")

            return self._value


_minted = MintedToken()


def minting() -> bool:
    return bool(config.LLM_CDP_ACCESS_KEY_ID or config.LLM_CDP_PRIVATE_KEY)


def chat_token() -> str:
    """The chat endpoint's token, per the order in the module docstring."""

    if minting():
        _require(LLM_CDP_ACCESS_KEY_ID=config.LLM_CDP_ACCESS_KEY_ID,
                 LLM_CDP_PRIVATE_KEY=config.LLM_CDP_PRIVATE_KEY)

        return _minted.get()

    return _token(config.LLM_CDP_TOKEN)


# ---------------------------------------------------------------------
# Chat model
# ---------------------------------------------------------------------


def describe() -> dict:
    """What serves the agent, for the UI and for each proposal's agent_version."""

    return {"provider": config.LLM_PROVIDER, "model": config.LLM_MODEL_ID,
            "endpoint": config.LLM_ENDPOINT_BASE_URL,
            "embedder": embedder_id()}


def _cai_settings() -> str:
    """The CAI endpoint's token, after checking the endpoint is configured."""

    if config.LLM_PROVIDER != "cai":
        raise LLMUnavailable(f"unknown SELFHEAL_LLM_PROVIDER {config.LLM_PROVIDER!r}")

    _require(LLM_MODEL_ID=config.LLM_MODEL_ID, LLM_ENDPOINT_BASE_URL=config.LLM_ENDPOINT_BASE_URL)
    token = chat_token()
    _require(LLM_CDP_TOKEN=token)

    return token


def _build_chat(token: str):
    from langchain_openai import ChatOpenAI

    log(f"chat model {config.LLM_MODEL_ID} at {config.LLM_ENDPOINT_BASE_URL}")

    return ChatOpenAI(model=config.LLM_MODEL_ID, base_url=config.LLM_ENDPOINT_BASE_URL,
                      api_key=token, temperature=config.LLM_TEMPERATURE,
                      timeout=120, max_retries=2)


def chat_model():
    """
    The chat model, rebuilt when its token has been renewed. A run keeps
    the instance it was handed; the old token outlives the renewal by
    LLM_TOKEN_RENEW_SECONDS.
    """

    global _chat, _chat_token

    if _chat_fixed:
        return _chat

    # Outside _lock: a mint runs the CLI, and the embedder shares the lock.
    token = _cai_settings()

    with _lock:
        if _chat_fixed:
            return _chat

        if _chat is None or token != _chat_token:
            _chat, _chat_token = _build_chat(token), token

        return _chat


def set_chat_model(model) -> None:
    """Install a model (tests), used as is; None goes back to the configured one."""

    global _chat, _chat_token, _chat_fixed, _probe

    with _lock:
        _chat, _chat_token, _chat_fixed = model, None, model is not None
        _probe = (0.0, None)


def invoke(model, messages: list):
    """model.invoke(messages), timed for App health (how long, and whether it raised)."""

    started, ok = time.monotonic(), False

    try:
        reply = model.invoke(messages)
        ok = True

        return reply

    finally:
        telemetry.record("llm", time.monotonic() - started, ok)


def chat_available() -> tuple:
    """(ok, reason), without raising. Configured and a token in hand; see probe()."""

    try:
        chat_model()

    except LLMUnavailable as exc:
        return False, str(exc)

    return True, ""


def probe() -> tuple:
    """
    (ok, reason): does the endpoint accept the token? One GET of
    {LLM_ENDPOINT_BASE_URL}/models, reused for a minute. A route the
    endpoint doesn't serve (404, 405) still means the token got past its
    auth. An installed model (tests) is not probed.
    """

    global _probe

    ok, reason = chat_available()

    if not ok or _chat_fixed:
        return ok, reason

    checked_at, result = _probe

    if result is not None and time.time() - checked_at < _RECHECK_SECONDS:
        return result

    try:
        token = chat_token()

    except LLMUnavailable as exc:
        return False, str(exc)

    request = urllib.request.Request(config.LLM_ENDPOINT_BASE_URL.rstrip("/") + "/models",
                                     headers={"Authorization": f"Bearer {token}"})

    try:
        with urllib.request.urlopen(request, timeout=15):
            result = (True, "")

    except urllib.error.HTTPError as exc:
        if exc.code in (404, 405):
            result = (True, "")

        else:
            # A gateway may answer with an HTML page: its text, on one line, fits a table cell.
            body = re.sub(r"<[^>]*>", " ", exc.read().decode("utf-8", "replace"))
            body = " ".join(body.replace("|", "/").split())[:160]
            result = (False, f"the endpoint answered {exc.code}" + (f": {body}" if body else ""))

    except (urllib.error.URLError, OSError) as exc:
        result = (False, f"the endpoint is unreachable: {getattr(exc, 'reason', exc)}")

    _probe = (time.time(), result)

    return result


# ---------------------------------------------------------------------
# Embeddings
# ---------------------------------------------------------------------


class CaiEmbedder:
    # Requests per call; the endpoint takes a list of inputs.
    BATCH = 32

    def __init__(self):
        self.token = _token(config.EMBEDDING_CDP_TOKEN)
        _require(EMBEDDING_MODEL_ID=config.EMBEDDING_MODEL_ID,
                 EMBEDDING_ENDPOINT_BASE_URL=config.EMBEDDING_ENDPOINT_BASE_URL,
                 EMBEDDING_CDP_TOKEN=self.token)

        from openai import OpenAI

        self.client = OpenAI(base_url=config.EMBEDDING_ENDPOINT_BASE_URL, api_key=self.token)
        self.id = f"cai:{config.EMBEDDING_MODEL_ID}"

    def _embed(self, texts: list, input_type: str) -> list:
        vectors = []

        for start in range(0, len(texts), self.BATCH):
            response = self.client.embeddings.create(
                input=texts[start:start + self.BATCH], model=config.EMBEDDING_MODEL_ID,
                extra_body={"input_type": input_type, "truncate": "END"})
            vectors.extend(item.embedding for item in sorted(response.data, key=lambda d: d.index))

        return vectors

    def passages(self, texts: list) -> list:
        return self._embed(list(texts), "passage")

    def query(self, text: str) -> list:
        return self._embed([text], "query")[0]


class HashEmbedder:
    """
    Signed feature hashing of words and word pairs, L2-normalized. No
    semantics beyond shared vocabulary, which is enough to tell a backlog
    section from a sizing one. md5, not hash(): Python salts str hashes
    per process, and stored vectors must survive a restart.
    """

    DIMS = 512
    _WORD = re.compile(r"[a-z][a-z0-9_]+")
    _STOP = frozenset("""
        the and for that this with are was not but its from into when then than
        each any all can has have been will what which they them their there only
        also more most such one two out off per via does did how why our you your
    """.split())

    def __init__(self):
        self.id = f"hash:v1:{self.DIMS}"

    def _terms(self, text: str) -> list:
        words = [w.rstrip("s") if len(w) > 4 else w
                 for w in self._WORD.findall(text.lower()) if w not in self._STOP]

        return words + [f"{a} {b}" for a, b in zip(words, words[1:])]

    def _vector(self, text: str) -> list:
        vector = [0.0] * self.DIMS

        for term in self._terms(text):
            digest = hashlib.md5(term.encode()).digest()
            slot = int.from_bytes(digest[:4], "little") % self.DIMS
            vector[slot] += 1.0 if digest[4] & 1 else -1.0

        norm = math.sqrt(sum(v * v for v in vector)) or 1.0

        return [v / norm for v in vector]

    def passages(self, texts: list) -> list:
        return [self._vector(t) for t in texts]

    def query(self, text: str) -> list:
        return self._vector(text)


def _build_embedder():
    if config.EMBEDDING_PROVIDER == "cai":
        return CaiEmbedder()

    if config.EMBEDDING_PROVIDER == "hash":
        return HashEmbedder()

    raise LLMUnavailable(f"unknown SELFHEAL_EMBEDDING_PROVIDER {config.EMBEDDING_PROVIDER!r}")


def embedder():
    global _embedder

    with _lock:
        if _embedder is None:
            _embedder = _build_embedder()
            log(f"embedder {_embedder.id}")

        return _embedder


def set_embedder(instance) -> None:
    global _embedder

    with _lock:
        _embedder = instance


def embedder_id() -> str:
    """The embedder's identity, without building it."""

    if _embedder is not None:
        return _embedder.id

    if config.EMBEDDING_PROVIDER == "hash":
        return HashEmbedder().id

    return f"{config.EMBEDDING_PROVIDER}:{config.EMBEDDING_MODEL_ID}"


def endpoint_env() -> dict:
    """The endpoint settings that are present, as names only, for the System tab."""

    names = ("LLM_MODEL_ID", "LLM_ENDPOINT_BASE_URL", "LLM_CDP_TOKEN",
             "LLM_CDP_ACCESS_KEY_ID", "LLM_CDP_PRIVATE_KEY", "LLM_CDP_REGION",
             "EMBEDDING_MODEL_ID", "EMBEDDING_ENDPOINT_BASE_URL", "EMBEDDING_CDP_TOKEN")

    return {name: bool(os.environ.get(name)) for name in names}
