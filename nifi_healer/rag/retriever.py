"""
Playbook sections for an incident.

    hits = for_incident(signals, context, parameters=inventory["parameters"])
    hits[0]  # {id, collection, title, heading, text, score, symptoms, actions}

The query is embedded with input_type "query" (llm.embedder().query).
Candidates come from both collections; a section tagged with a symptom
the signals suggest gets a small boost, and at least one platform rule
is always kept, since those apply to every remediation. A section that
names a parameter the process group uses (a runbook row with its correct
value) is kept too: the signals of a wrong value look like a backlog or
an outage, and similarity alone ranks those playbooks first. Collections
are (re)built on first use if the playbooks or the embedder changed.

`id` ("back-pressure#runaway-producer-on-a-0-sec-schedule") is what a
proposal cites.
"""

import threading

import config
from nifi_healer import llm
from nifi_healer.rag import ingest

# Added to a section's similarity per symptom tag it shares with the
# signals: enough to break near-ties, not to outrank a clearly better text.
SYMPTOM_BOOST = 0.05

_ready = threading.Event()
_lock = threading.Lock()


def ensure() -> dict:
    """Build the collections once per process if they are missing or stale."""

    if _ready.is_set():
        return {}

    with _lock:
        if _ready.is_set():
            return {}

        written = ingest.ingest()
        _ready.set()

        return written


def invalidate() -> None:
    _ready.clear()


def edition() -> str:
    """The approved playbook edits the search reads (overlay.edition); "" for the files alone."""

    return ingest.edition()


def rebuild() -> None:
    """After a playbook edit is approved or reverted: build again now, so no diagnosis waits for it."""

    invalidate()

    def run():
        try:
            ensure()

        except Exception as exc:  # the next search tries again
            ingest.log(f"rebuild failed: {exc}")

    threading.Thread(target=run, name="playbook-rebuild", daemon=True).start()


# ---------------------------------------------------------------------
# Query building
# ---------------------------------------------------------------------


def hint_symptoms(signals: list, parameters: list = ()) -> list:
    """
    Playbook symptom tags the signals point at, most likely first. A hint
    for ranking only: the LLM still decides what the evidence means. A
    process group that uses parameters may be running a wrong value.
    """

    hints = ["misconfiguration"] if parameters else []

    def add(*names):
        for name in names:
            if name not in hints:
                hints.append(name)

    for signal in signals:
        kind, metric = signal.get("type"), signal.get("metric")
        component = (signal.get("component") or "").lower()
        threshold = (signal.get("evidence") or {}).get("threshold") or {}
        below = threshold.get("below") is not None and threshold.get("above") is None

        if kind == "flow_imbalance":
            add("data_loss")

        elif metric == "cpu":
            add("sizing_cost" if below else "sizing_cpu")

        elif metric == "percent_full":
            add("backpressure", "backlog")

        elif metric in ("queued_count", "queued_bytes"):
            if any(word in component for word in ("fail", "retry", "error")):
                add("failure_loop", "misconfiguration")

            add("backlog", "backpressure")

        elif metric in ("flowfiles_out", "flowfiles_in", "bytes_out"):
            add("latency", "stuck", "misconfiguration")

    return hints


def query_for(signals: list, context: str = "", parameters: list = ()) -> str:
    """The retrieval question: what the signals say, plus any flow context."""

    lines = [s.get("summary", "") for s in signals if s.get("summary")]

    if context:
        lines.append(context)

    if parameters:
        lines.append("Parameters it uses: " + ", ".join(
            p["name"] if p.get("sensitive") else f"{p['name']} = {p.get('value')}" for p in parameters))

    return "\n".join(lines) or "NiFi flow health check"


# ---------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------


def _hits(name: str, vector: list, n: int, contains: str = "") -> list:
    found = ingest.collection(name)

    if found is None or not n:
        return []

    count = found.count()

    if not count:
        return []

    result = found.query(query_embeddings=[vector], n_results=min(n, count),
                         where_document={"$contains": contains} if contains else None,
                         include=["documents", "metadatas", "distances"])
    hits = []

    for chunk_id, text, meta, distance in zip(result["ids"][0], result["documents"][0],
                                              result["metadatas"][0], result["distances"][0]):
        hits.append({"id": chunk_id, "collection": name, "title": meta.get("title", ""),
                     "heading": meta.get("heading", ""), "source": meta.get("source", ""),
                     "text": text, "similarity": 1.0 - float(distance),
                     "symptoms": ingest.untag(meta.get("symptoms")),
                     "actions": ingest.untag(meta.get("actions"))})

    return hits


def search(query: str, k: int = 0, symptoms: list = (), min_practices: int = 1, terms: list = ()) -> list:
    """
    The top k sections, best first, with at least `min_practices` platform
    rules and the best section naming each of `terms` verbatim (up to k/2).
    """

    ensure()
    k = k or config.RAG_TOP_K
    vector = llm.embedder().query(query)
    wanted = set(symptoms)
    candidates = _hits(ingest.PLAYBOOKS, vector, k * 2) + _hits(ingest.BEST_PRACTICES, vector, k)
    named = []

    for term in terms:
        for hit in _hits(ingest.PLAYBOOKS, vector, 1, contains=term):
            if hit["id"] not in named:
                named.append(hit["id"])

            if hit["id"] not in {h["id"] for h in candidates}:
                candidates.append(hit)

    named = set(named[:max(1, k // 2)])

    for hit in candidates:
        # Platform rules are tagged with every symptom; only a specific
        # tag says something about this incident.
        shared = 0 if hit["collection"] == ingest.BEST_PRACTICES else len(wanted & set(hit["symptoms"]))
        hit["score"] = round(hit["similarity"] + SYMPTOM_BOOST * shared, 4)

    ranked = sorted(candidates, key=lambda h: -h["score"])
    chosen = ranked[:k]

    def make_room():
        # Drop the weakest playbook section that nothing requires.
        playbooks = [h for h in chosen if h["collection"] == ingest.PLAYBOOKS and h["id"] not in named]

        if len(chosen) >= k and playbooks:
            chosen.remove(playbooks[-1])

    for hit in ranked[k:]:
        if hit["id"] in named:
            make_room()
            chosen.append(hit)

    practices = [h for h in chosen if h["collection"] == ingest.BEST_PRACTICES]
    spare = [h for h in ranked[k:] if h["collection"] == ingest.BEST_PRACTICES]

    while len(practices) < min_practices and spare:
        make_room()
        best = spare.pop(0)
        chosen.append(best)
        practices.append(best)

    return chosen


def for_incident(signals: list, context: str = "", k: int = 0, parameters: list = ()) -> list:
    """`parameters`: the inventory's parameters the process group references."""

    return search(query_for(signals, context, parameters), k=k, symptoms=hint_symptoms(signals, parameters),
                  terms=[p["name"] for p in parameters if p.get("name")])
