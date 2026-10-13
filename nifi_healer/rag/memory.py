"""
Remediation memory: the fleet's own past cases, shown to the agent with
the playbooks.

    hits = recall(incident)
    hits[0]  # {id, headline, text, similarity, score, outcome}

A case is an incident whose fix was judged, by the KPIs or a reviewer,
or whose proposal a reviewer rejected: what the agent diagnosed and
proposed, what the reviewer did with it (and why), and what was measured
after. Incidents with nothing judged yet (pending, dry runs, NO_ACTION)
teach nothing and are left out, as are the replay's oracle runs, whose
fixes are the design labels.

The store is the record; the remediation_memory collection is an index
of it, brought up to date before every recall (only new or changed cases
are embedded). Each case is embedded by its situation, the same text the
playbook search asks with (retriever.query_for), so a recall compares
one incident's signals with another's. A case of the same process group
gets a boost: the same fault coming back is what memory is most for.

`id` ("case:INC-20261012-...") is what a proposal cites.
"""

import hashlib
import threading
import time

import config
from nifi_healer import llm
from nifi_healer.eval import scorecard as sc
from nifi_healer.rag import ingest, retriever
from nifi_healer.store.db import get_store

COLLECTION = "remediation_memory"

# Added to a case's similarity when it is of the incident's own process group.
SAME_GROUP_BOOST = 0.1

# Results worth learning from; "unclear" and "pending" say nothing yet.
_LEARNED = (sc.WORKED, sc.NO_HELP, sc.WORSE)
_TEXT_CAP = 300

_lock = threading.Lock()


def log(msg: str) -> None:
    print(f"[memory] {msg}", flush=True)


def situation(incident: dict) -> str:
    """What the incident looked like, as the playbook search asks it."""

    return retriever.query_for(incident.get("signals") or [],
                               context=f"{incident.get('process_group') or ''}: {incident.get('summary') or ''}")


# ---------------------------------------------------------------------
# Cases from the store's record
# ---------------------------------------------------------------------


def _cap(text, limit: int = _TEXT_CAP) -> str:
    text = " ".join(str(text or "").split())

    return text if len(text) <= limit else text[:limit - 3] + "..."


def step_text(step: dict, values: bool = True) -> str:
    """ "SET_CONCURRENT_TASKS 'S01 Consumer' value=4"; without values, only what names the change."""

    rest = {k: v for k, v in step.items() if k not in ("action", "target")
            and (values or k in ("property", "parameter", "relationship"))}

    return " ".join([str(step.get("action") or "?")] + ([repr(step["target"])] if step.get("target") else [])
                    + [f"{k}={v}" for k, v in rest.items()])


def _steps(proposal: dict | None) -> str:
    # A value is shown only once validation passed it: validation refuses
    # sensitive parameters, so a shown value is never a secret.
    proposal = proposal or {}
    shown = bool((proposal.get("validation") or {}).get("ok"))
    steps = [s for s in proposal.get("action") or [] if isinstance(s, dict)]

    return "; ".join(step_text(s, shown) for s in steps) or "no steps"


def _window(seconds) -> str:
    seconds = int(seconds or 0)

    return f"{seconds // 60} min" if seconds < 3600 else f"{seconds // 3600} h"


def _measured(found: list) -> str:
    parts = []

    for o in sorted(found, key=lambda o: o["window_seconds"] or 0):
        details = o.get("details") or {}
        deciding = [k for k in details.get("kpis") or [] if k.get("primary") or k.get("signalled")]
        why = "; ".join(f"{(k.get('kpi') or '').split(':')[2] or k.get('kpi')}: {k.get('reason')}"
                        for k in deciding if k.get("reason") and len((k.get("kpi") or "").split(":")) == 4)
        parts.append(f"{_window(o['window_seconds'])} {o['verdict']}" + (f" ({_cap(why, 160)})" if why else ""))

    return ", ".join(parts)


def lessons(record: dict) -> list:
    """The cases worth recalling in a store.record(): [{id, incident, situation, text, headline, outcome, ...}]."""

    by = {name: {} for name in ("proposals", "reviews", "actions", "outcomes", "action_labels")}

    for name, rows in by.items():
        for row in record.get(name) or []:
            rows.setdefault(row["incident_id"], []).append(row)

    incidents = {i["id"]: i for i in record.get("incidents") or []}
    out = []

    for case in sc.cases(record):
        iid = case["incident"]

        if "/model:oracle" in case["agent_version"]:
            continue

        rejected = case["review"] == sc.REJECTED
        learned = case["applied"] and case["result"] in _LEARNED

        if not (rejected or learned):
            continue

        incident = incidents[iid]
        proposals = by["proposals"].get(iid, [])
        agents = [p for p in proposals if not (p.get("agent_version") or "").endswith("+review")]
        pick = next((p for p in reversed(agents) if (p.get("validation") or {}).get("ok")),
                    agents[-1] if agents else None)
        reviews = by["reviews"].get(iid, [])
        applied = [a for a in by["actions"].get(iid, []) if a["status"] == "APPLIED" and not a["dry_run"]]
        action = applied[-1] if applied else None
        lines = [f"Signals: {_cap(situation(incident), 400)}"]

        if pick:
            lines += [f"Agent's diagnosis: {_cap(pick.get('diagnosis'))}", f"Agent proposed: {_steps(pick)}"]

        if rejected:
            reason = next((r["reason"] for r in reversed(reviews) if r["decision"] == "reject"), "")
            outcome = "rejected"
            lines.append(f"Reviewer rejected it: {_cap(reason) or '(no reason given)'}")

        else:
            chosen = next((p for p in proposals if p["id"] == action.get("proposal_id")), None)
            outcome = case["result"]

            if chosen and (chosen.get("agent_version") or "").endswith("+review"):
                lines.append(f"Reviewer changed it to: {_steps(chosen)}; approved")

            else:
                lines.append("Reviewer approved it as proposed")

            label = {r["action_id"]: r for r in by["action_labels"].get(iid, [])}.get(action["id"])
            measured = _measured([o for o in by["outcomes"].get(iid, []) if o["action_id"] == action["id"]])
            lines.append(f"Applied. Result: {outcome}" + (f"; measured {measured}" if measured else ""))

            if label:
                lines.append(f"A reviewer labelled the fix {label['label']}: {_cap(label.get('reason'))}")

            if case["regressed"]:
                lines.append("A later window measured it worse (a regression)")

            if case["rolled_back"]:
                lines.append("The fix was later rolled back")

        when = time.strftime("%Y-%m-%d", time.gmtime(incident["opened_at"]))
        fix = case["action"] or "no proposal"
        head = f"[case:{iid}] {when} · {incident['pack']} · \"{incident['process_group']}\" · {fix}: {outcome}"
        out.append({"id": f"case:{iid}", "incident": iid, "pack": incident["pack"],
                    "process_group": incident["process_group"], "situation": situation(incident),
                    "headline": f"{incident['process_group']}: {fix} → {outcome}", "outcome": outcome,
                    "text": "\n".join([head] + lines)})

    return out


# ---------------------------------------------------------------------
# The index
# ---------------------------------------------------------------------


def _digest(lesson: dict, embedder_id: str) -> str:
    payload = "\n".join([embedder_id, lesson["situation"], lesson["text"]])

    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _collection(embedder_id: str):
    found = ingest.collection(COLLECTION)

    if found is not None and (found.metadata or {}).get("embedder") != embedder_id:
        # Vectors from two embedders cannot be compared: start over.
        ingest.client().delete_collection(COLLECTION)
        found = None

    if found is None:
        found = ingest.client().create_collection(COLLECTION, metadata={"hnsw:space": "cosine",
                                                                        "embedder": embedder_id})

    return found


def sync(store=None) -> int:
    """Bring the collection up to the store's cases; how many were (re)embedded."""

    with _lock:
        embedder = llm.embedder()
        found = lessons((store or get_store()).record())
        index = _collection(embedder.id)
        have = index.get(include=["metadatas"])
        digests = {i: (m or {}).get("digest") for i, m in zip(have["ids"], have["metadatas"])}
        changed = [l for l in found if digests.get(l["id"]) != _digest(l, embedder.id)]
        gone = sorted(set(digests) - {l["id"] for l in found})

        if gone:
            index.delete(ids=gone)

        if changed:
            index.upsert(ids=[l["id"] for l in changed], documents=[l["text"] for l in changed],
                         embeddings=embedder.passages([l["situation"] for l in changed]),
                         metadatas=[{"incident": l["incident"], "pack": l["pack"],
                                     "process_group": l["process_group"], "headline": l["headline"],
                                     "outcome": l["outcome"], "digest": _digest(l, embedder.id)}
                                    for l in changed])
            log(f"{len(changed)} case(s) indexed, {len(found)} in all")

        return len(changed)


def recall(incident: dict, k: int = 0, store=None) -> list:
    """The k past cases most like this incident, best first; none when memory is off."""

    k = k or config.MEMORY_TOP_K

    if not config.MEMORY or k <= 0:
        return []

    sync(store)
    index = ingest.collection(COLLECTION)
    count = index.count() if index is not None else 0

    if not count:
        return []

    result = index.query(query_embeddings=[llm.embedder().query(situation(incident))],
                         n_results=min(count, 3 * k + 1), include=["documents", "metadatas", "distances"])
    hits = []

    for case_id, text, meta, distance in zip(result["ids"][0], result["documents"][0],
                                             result["metadatas"][0], result["distances"][0]):
        if meta.get("incident") == incident.get("id"):
            continue

        same = meta.get("pack") == incident.get("pack") and meta.get("process_group") == incident.get("process_group")
        similarity = 1.0 - float(distance)
        hits.append({"id": case_id, "headline": meta.get("headline", ""), "text": text,
                     "outcome": meta.get("outcome", ""), "similarity": similarity,
                     "score": round(similarity + (SAME_GROUP_BOOST if same else 0.0), 4)})

    return sorted(hits, key=lambda h: -h["score"])[:k]
