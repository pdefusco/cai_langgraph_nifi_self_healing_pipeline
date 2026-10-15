"""
Playbooks into Chroma: one document per `##` section of playbooks/*.md.

    python -m nifi_healer.rag.ingest            # only if the playbooks changed
    python -m nifi_healer.rag.ingest --force

Each playbook starts with YAML front matter naming its collection
(nifi_playbooks or nifi_best_practices), title, symptom tags and action
types. A section can narrow them with `<!-- symptoms: a, b -->` and
`<!-- actions: X, Y -->` lines under its heading. The text before the
first `##` is a section of its own, under the playbook's title.

Embeddings are computed here (llm.embedder(), input_type "passage") and
handed to Chroma, which never embeds anything itself. Every collection
records a fingerprint of the chunks and the embedder that built it; a
change to either rebuilds the collection, because vectors from two
embedders cannot be compared.
"""

import argparse
import glob
import hashlib
import json
import os
import re
import threading

import yaml

import config
from nifi_healer import llm
from nifi_healer.rag import overlay
from nifi_healer.store import db

PLAYBOOKS = "nifi_playbooks"
BEST_PRACTICES = "nifi_best_practices"
COLLECTIONS = (PLAYBOOKS, BEST_PRACTICES)

_FRONT = re.compile(r"\A---\n(.*?)\n---\n", re.S)
_TAG = re.compile(r"<!--\s*(symptoms|actions)\s*:(.*?)-->", re.I)
_COMMENT = re.compile(r"<!--.*?-->", re.S)

_lock = threading.Lock()
_client = None
_edition = {"value": ""}
# The replay reads the app's approved edits, not its own store's (use_edits).
_override = {"edits": None}


def log(msg: str) -> None:
    print(f"[rag] {msg}", flush=True)


slug = overlay.slug


def tags(values) -> str:
    """Tags as one metadata string, comma-delimited at both ends so
    `",backlog,"` matches whole tags only."""

    return "," + ",".join(values) + "," if values else ""


def untag(value: str) -> list:
    return [v for v in (value or "").split(",") if v]


def _split_list(raw: str) -> list:
    return [v.strip() for v in raw.split(",") if v.strip()]


# ---------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------


def parse(path: str) -> list:
    """The chunks of one playbook: [{id, collection, text, metadata}]."""

    with open(path, encoding="utf-8") as handle:
        return parse_text(os.path.basename(path), handle.read())


def front_matter(raw: str) -> dict:
    match = _FRONT.match(raw)

    return (yaml.safe_load(match.group(1)) if match else None) or {}


def parse_text(source: str, raw: str) -> list:
    """The chunks of a playbook's markdown, named `source` (its file name)."""

    match = _FRONT.match(raw)
    front = front_matter(raw)
    body = raw[match.end():] if match else raw
    title = front.get("title") or os.path.splitext(source)[0]
    collection = front.get("collection") or PLAYBOOKS

    if collection not in COLLECTIONS:
        raise ValueError(f"{source}: unknown collection {collection!r}")

    head, rest = overlay.split(body)
    chunks = []

    for heading, text in [(title, head)] + rest:
        text = "\n".join(line for line in text.splitlines() if not line.startswith("# "))
        found = {"symptoms": front.get("symptoms") or [], "actions": front.get("actions") or []}

        for name, values in _TAG.findall(text):
            found[name.lower()] = _split_list(values)

        text = _COMMENT.sub("", text).strip()

        if not text:
            continue

        chunk_id = f"{os.path.splitext(source)[0]}#{slug(heading)}"
        chunks.append({
            "id": chunk_id,
            "collection": collection,
            # The title rides along so a section reads on its own, to the
            # embedder and to the LLM.
            "text": f"{title}: {heading}\n\n{text}" if heading != title else f"{title}\n\n{text}",
            "metadata": {"source": source, "title": title, "heading": heading,
                         "symptoms": tags(found["symptoms"]), "actions": tags(found["actions"])},
        })

    return chunks


def use_edits(edits: list | None) -> None:
    """Read these approved edits instead of the store's (None: the store's again)."""

    _override["edits"] = None if edits is None else list(edits)


def applied_edits() -> list:
    """The approved playbook edits (rag/overlay.py); none if the store can't be read."""

    if _override["edits"] is not None:
        return list(_override["edits"])

    try:
        return db.get_store().applied_playbook_edits()

    except Exception as exc:  # the files alone are still the playbooks
        log(f"playbook edits unavailable: {exc}")

        return []


def raw_playbooks(playbooks_dir: str = "", edits: list | None = None, states: dict | None = None) -> dict:
    """{file name: its markdown with the approved edits applied}; `states` collects each edit's."""

    edits = applied_edits() if edits is None else edits
    out = {}

    for path in sorted(glob.glob(os.path.join(playbooks_dir or config.PLAYBOOKS_DIR, "*.md"))):
        source = os.path.basename(path)

        with open(path, encoding="utf-8") as handle:
            raw = handle.read()

        mine = [e for e in edits if e.get("playbook") == source]

        if mine:
            raw, found = overlay.apply(raw, mine)

            if states is not None:
                states.update(found)

        out[source] = raw

    if states is not None:
        # An edit for a playbook that is gone has nothing to apply to.
        states.update({e["id"]: overlay.STALE for e in edits if e["id"] not in states})

    return out


def load(playbooks_dir: str = "", edits: list | None = None, states: dict | None = None) -> list:
    chunks = []

    for source, raw in raw_playbooks(playbooks_dir, edits, states).items():
        chunks.extend(parse_text(source, raw))

    seen = set()

    for chunk in chunks:
        if chunk["id"] in seen:
            raise ValueError(f"duplicate section id {chunk['id']}: headings must be unique per playbook")

        seen.add(chunk["id"])

    return chunks


def fingerprint(chunks: list, embedder_id: str) -> str:
    payload = json.dumps([embedder_id] + [[c["id"], c["text"], c["metadata"]] for c in chunks],
                         sort_keys=True)

    return hashlib.sha256(payload.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------
# Chroma
# ---------------------------------------------------------------------


def client():
    global _client

    with _lock:
        if _client is None:
            import chromadb
            from chromadb.config import Settings

            os.makedirs(config.CHROMA_DIR, exist_ok=True)
            _client = chromadb.PersistentClient(path=config.CHROMA_DIR,
                                                settings=Settings(anonymized_telemetry=False))

        return _client


def reset_client() -> None:
    """Forget the cached client (tests point CHROMA_DIR somewhere new)."""

    global _client

    with _lock:
        _client = None


def collection(name: str):
    """The collection, or None if it was never built."""

    try:
        return client().get_collection(name)

    except Exception:
        return None


def current(name: str, chunks: list, embedder_id: str) -> bool:
    found = collection(name)

    return bool(found is not None and (found.metadata or {}).get("fingerprint")
                == fingerprint(chunks, embedder_id))


def edition() -> str:
    """The approved edits in the collections last built (overlay.edition): "" for the files alone."""

    return _edition["value"]


def _note(states: dict) -> None:
    """An edit git now has (MERGED) or that no longer fits its section (STALE) stops applying."""

    if _override["edits"] is not None:  # another store's edits: theirs to record
        return

    try:
        store = db.get_store()

        for edit_id, state in states.items():
            if state != overlay.APPLIED:
                store.update_playbook_edit(edit_id, status=state)
                log(f"playbook edit {edit_id}: {state.lower()}")

    except Exception as exc:  # best effort: the overlay already skipped it
        log(f"could not record playbook edit states: {exc}")


def ingest(force: bool = False, playbooks_dir: str = "") -> dict:
    """{collection: sections written, or 0 when it was already current}."""

    edits, states = applied_edits(), {}
    chunks = load(playbooks_dir, edits, states)
    _note(states)
    _edition["value"] = overlay.edition(edits, states)
    embedder = llm.embedder()
    written = {}

    for name in COLLECTIONS:
        mine = [c for c in chunks if c["collection"] == name]

        if not force and current(name, mine, embedder.id):
            written[name] = 0
            continue

        if collection(name) is not None:
            client().delete_collection(name)

        built = client().create_collection(name, metadata={
            "hnsw:space": "cosine",
            "fingerprint": fingerprint(mine, embedder.id),
            "embedder": embedder.id,
        })

        if mine:
            built.add(ids=[c["id"] for c in mine],
                      documents=[c["text"] for c in mine],
                      metadatas=[c["metadata"] for c in mine],
                      embeddings=embedder.passages([c["text"] for c in mine]))

        written[name] = len(mine)
        log(f"{name}: {len(mine)} section(s) with {embedder.id}")

    return written


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--force", action="store_true", help="rebuild even if nothing changed")
    args = parser.parse_args()

    result = ingest(force=args.force)
    print(json.dumps({"chroma_dir": config.CHROMA_DIR, "embedder": llm.embedder_id(),
                      "written": result}, indent=2))


if __name__ == "__main__":
    main()
