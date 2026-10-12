"""
The one place that knows which model serves the agent and the retriever.

    llm = chat_model()                  # a LangChain chat model
    vectors = embedder().passages(texts)
    vector = embedder().query(text)

SELFHEAL_LLM_PROVIDER picks the chat model:

  cai     CAI Inference Service, OpenAI-compatible (LLM_MODEL_ID,
          LLM_ENDPOINT_BASE_URL, LLM_CDP_TOKEN)

SELFHEAL_EMBEDDING_PROVIDER picks the embedder:

  cai     the CAI embedding endpoint (EMBEDDING_*). E5-style models embed
          documents and questions differently, so passages are sent with
          input_type "passage" and queries with "query".
  hash    a local hashed bag-of-words, for tests and offline mock runs.

Another provider is another branch here; nothing else imports a client.
Tests install their own with set_chat_model() / set_embedder().

When a CDP token is not in the environment, the CAI session's /tmp/jwt
is used. Tokens are only ever handed to the client, never logged.
"""

import hashlib
import json
import math
import os
import re
import threading

import config

_JWT_FILE = "/tmp/jwt"

_lock = threading.Lock()
_chat = None
_embedder = None


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
# Chat model
# ---------------------------------------------------------------------


def describe() -> dict:
    """What serves the agent, for the UI and for each proposal's agent_version."""

    return {"provider": config.LLM_PROVIDER, "model": config.LLM_MODEL_ID,
            "endpoint": config.LLM_ENDPOINT_BASE_URL,
            "embedder": embedder_id()}


def _build_chat():
    if config.LLM_PROVIDER == "cai":
        token = _token(config.LLM_CDP_TOKEN)
        _require(LLM_MODEL_ID=config.LLM_MODEL_ID,
                 LLM_ENDPOINT_BASE_URL=config.LLM_ENDPOINT_BASE_URL,
                 LLM_CDP_TOKEN=token)

        from langchain_openai import ChatOpenAI

        log(f"chat model {config.LLM_MODEL_ID} at {config.LLM_ENDPOINT_BASE_URL}")

        return ChatOpenAI(model=config.LLM_MODEL_ID, base_url=config.LLM_ENDPOINT_BASE_URL,
                          api_key=token, temperature=config.LLM_TEMPERATURE,
                          timeout=120, max_retries=2)

    raise LLMUnavailable(f"unknown SELFHEAL_LLM_PROVIDER {config.LLM_PROVIDER!r}")


def chat_model():
    global _chat

    with _lock:
        if _chat is None:
            _chat = _build_chat()

        return _chat


def set_chat_model(model) -> None:
    global _chat

    with _lock:
        _chat = model


def chat_available() -> tuple:
    """(ok, reason), without raising."""

    try:
        chat_model()

    except LLMUnavailable as exc:
        return False, str(exc)

    return True, ""


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
             "EMBEDDING_MODEL_ID", "EMBEDDING_ENDPOINT_BASE_URL", "EMBEDDING_CDP_TOKEN")

    return {name: bool(os.environ.get(name)) for name in names}
