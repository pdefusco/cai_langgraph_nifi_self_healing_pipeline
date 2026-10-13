"""Playbook chunking, Chroma ingest and retrieval, with the local hash embedder."""

import pytest

import config
from nifi_healer import llm
from nifi_healer.rag import ingest, retriever


@pytest.fixture
def chroma(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CHROMA_DIR", str(tmp_path / "chroma"))
    ingest.reset_client()
    retriever.invalidate()
    llm.set_embedder(llm.HashEmbedder())
    yield
    ingest.reset_client()
    retriever.invalidate()
    llm.set_embedder(None)


def test_every_playbook_parses():
    chunks = ingest.load()
    sources = {c["metadata"]["source"] for c in chunks}

    assert "parameter-misconfiguration.md" in sources and "cdf-platform-rules.md" in sources
    assert {c["collection"] for c in chunks} == set(ingest.COLLECTIONS)
    # Small enough for a 512-token E5 window.
    assert max(len(c["text"].split()) for c in chunks) < 350


def test_section_tags_narrow_the_front_matter():
    chunks = {c["id"]: c for c in ingest.load()}
    merge = chunks["latency-batching-stuck#mergecontent-bin-that-never-fills"]["metadata"]

    assert ingest.untag(merge["symptoms"]) == ["latency"]
    assert ingest.untag(merge["actions"]) == ["SET_PROCESSOR_PROPERTY"]
    assert "<!--" not in chunks["latency-batching-stuck#disabled-processor"]["text"]


def test_runbook_carries_the_correct_values():
    text = " ".join(c["text"] for c in ingest.load() if c["metadata"]["source"] == "parameter-misconfiguration.md")

    assert "http://localhost:9876/ingest" in text and "`s18.copies` | `1`" in text


def test_ingest_is_idempotent_and_rebuilds_for_a_new_embedder(chroma):
    first = ingest.ingest()
    assert first[ingest.PLAYBOOKS] > 0 and first[ingest.BEST_PRACTICES] > 0

    assert ingest.ingest() == {name: 0 for name in ingest.COLLECTIONS}

    class Other(llm.HashEmbedder):
        def __init__(self):
            self.id = "hash:other"

    llm.set_embedder(Other())
    assert ingest.ingest()[ingest.PLAYBOOKS] == first[ingest.PLAYBOOKS]
    assert ingest.collection(ingest.PLAYBOOKS).metadata["embedder"] == "hash:other"


@pytest.mark.parametrize("query, expected", [
    ("Percent Full on s05-buffer is 100, back pressure object threshold 10", "back-pressure#"),
    ("MergeContent FlowFiles Out is 0, bin never fills, Max Bin Age not set", "latency-batching-stuck#mergecontent"),
    ("FlowFiles dropped, no queue growing, data lost, expiration", "data-loss#"),
    ("CPU Utilization 97 above 85, CompressContent level 9", "sizing-and-cost#cpu-bound"),
])
def test_search_finds_the_right_playbook(chroma, query, expected):
    hits = retriever.search(query, k=4)

    assert any(h["id"].startswith(expected) for h in hits[:2]), [h["id"] for h in hits]


def test_search_keeps_a_platform_rule(chroma):
    hits = retriever.search("Queued Count on s01-backlog above 500, Concurrent Tasks 1", k=3)

    assert len(hits) == 3
    assert any(h["collection"] == ingest.BEST_PRACTICES for h in hits)
    assert len({h["id"] for h in hits}) == 3


def test_a_section_naming_a_parameter_in_use_is_kept(chroma):
    query = "Flow Files Queued on s17-requests is 594, above 50"
    runbook = "parameter-misconfiguration#runbook-correct-values-for-the-nfh-p5-parameters-context"

    assert runbook not in [h["id"] for h in retriever.search(query, k=3)]

    hits = retriever.search(query, k=3, terms=["s17.target.url", "not.in.any.playbook"])
    assert runbook in [h["id"] for h in hits] and len(hits) == 3
    assert any(h["collection"] == ingest.BEST_PRACTICES for h in hits)

    used = [{"context": "nfh-p5-parameters", "name": "s17.target.url", "value": "http://10.255.255.1:8080/ingest"},
            {"context": "nfh-p5-parameters", "name": "s17.token", "value": "********", "sensitive": True}]
    asked = retriever.query_for([], "S17 Wrong Endpoint Parameter", used)
    assert "s17.target.url = http://10.255.255.1:8080/ingest" in asked and "s17.token" in asked
    assert "********" not in asked
    assert retriever.hint_symptoms([], used) == ["misconfiguration"]


def test_hint_symptoms():
    imbalance = {"type": "flow_imbalance", "metric": "flowfiles_in"}
    failures = {"type": "kpi_breach", "metric": "queued_count", "component": "s10-failures",
                "evidence": {"threshold": {"above": 50, "below": None}}}
    idle = {"type": "kpi_breach", "metric": "cpu", "evidence": {"threshold": {"above": None, "below": 10}}}

    assert retriever.hint_symptoms([imbalance]) == ["data_loss"]
    assert retriever.hint_symptoms([failures])[0] == "failure_loop"
    assert retriever.hint_symptoms([idle]) == ["sizing_cost"]


def test_cai_embedder_uses_passage_and_query(monkeypatch):
    monkeypatch.setattr(config, "EMBEDDING_MODEL_ID", "e5")
    monkeypatch.setattr(config, "EMBEDDING_ENDPOINT_BASE_URL", "https://example.invalid/v1")
    monkeypatch.setattr(config, "EMBEDDING_CDP_TOKEN", "t")
    calls = []

    class Item:
        def __init__(self, index):
            self.index, self.embedding = index, [float(index)]

    class Embeddings:
        def create(self, input, model, extra_body):
            calls.append(extra_body["input_type"])
            return type("R", (), {"data": [Item(i) for i in range(len(input))]})

    embedder = llm.CaiEmbedder()
    embedder.client = type("C", (), {"embeddings": Embeddings()})()

    assert embedder.passages(["a", "b"]) == [[0.0], [1.0]]
    assert embedder.query("q") == [0.0]
    assert calls == ["passage", "query"]


def test_unconfigured_chat_model_says_what_is_missing(monkeypatch):
    monkeypatch.setattr(config, "LLM_PROVIDER", "cai")
    monkeypatch.setattr(config, "LLM_MODEL_ID", "")
    monkeypatch.setattr(llm, "_JWT_FILE", "/nonexistent/jwt")
    llm.set_chat_model(None)

    ok, reason = llm.chat_available()
    assert not ok and "LLM_MODEL_ID" in reason
