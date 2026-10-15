"""
A reviewer's feedback, drafted by the LLM into a playbook edit for a
person to approve on the Playbooks tab.

    future = queue("reject", incident_id, review_id)      # nodes.record_feedback
    future = queue("wrong_fix", incident_id, label_id)    # views.label_fix
    future.result()                                       # the playbook_edits row

A rejection or a WRONG_FIX label already carries a reason. The drafter
shows the LLM the incident, what the agent proposed, what the reviewer
did and said, and the sections the agent cited as it read them (with
the approved edits, rag/overlay.py), and asks for the smallest edit of
one section that would have led the agent right, or a skip when the
feedback is about the incident only. check() refuses an edit the agent
must not read (the platform rules, an action outside the catalog, an
emptied queue, a heading, a CRN); the LLM gets one turn to fix it.

A draft is PROPOSED (with any problems left), SKIPPED (the LLM's reason)
or FAILED (no LLM, or no usable reply). A second draft of the same
section supersedes an open one and carries its feedback along, so each
section has one open draft. Nothing here changes what the agent reads:
only an approval (ui/views.approve_playbook_edit) does.
"""

import difflib
import json
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from pydantic import BaseModel, ValidationError

import config
from nifi_healer import llm
from nifi_healer.actions import catalog, rollback
from nifi_healer.agent import prompts
from nifi_healer.cdp.client import get_client
from nifi_healer.rag import ingest, overlay
from nifi_healer.store.db import get_store

REJECT, WRONG_FIX = "reject", "wrong_fix"
KINDS = (REJECT, WRONG_FIX)

MIN_TEXT, MAX_TEXT = 40, 4000

# The share of an existing section's words an edit keeps, in order.
KEEP = 0.5

# What no playbook may newly advise (counted against the section's text,
# so an existing "never do X" line still passes).
FORBIDDEN = ("stop_and_empty_queues", "restart-deployment", "restart the deployment")

_lock = threading.Lock()
_pool = None
_waiting = {}


def log(message: str) -> None:
    print(f"[drafter] {message}", flush=True)


def _now() -> float:
    return get_client().now()


class PlaybookEdit(BaseModel):
    skip: bool = False
    playbook: str = ""
    section_id: str = ""
    heading: str = ""
    text: str = ""
    rationale: str = ""


# ---------------------------------------------------------------------
# Queueing
# ---------------------------------------------------------------------


def _executor() -> ThreadPoolExecutor:
    global _pool

    with _lock:
        if _pool is None:
            # One at a time: a draft reads the open drafts the last one wrote.
            _pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="drafter")

        return _pool


def _submit(edit_id: int):
    pool = _executor()

    with _lock:
        future = _waiting.get(edit_id)

        if future is None:
            future = _waiting[edit_id] = pool.submit(draft, edit_id)

    future.add_done_callback(lambda done: _forget(edit_id, done))

    return future


def _forget(edit_id: int, future) -> None:
    with _lock:
        if _waiting.get(edit_id) is future:
            del _waiting[edit_id]


def drafting(edit_id: int) -> bool:
    """Queued or running in this process (a DRAFTING row that is not was cut off by a restart)."""

    with _lock:
        return edit_id in _waiting


def queue(kind: str, incident_id: str, source_id: int):
    """
    Draft an edit from one review (reject) or action label (wrong_fix) in
    the background. Returns the Future, or None when drafting is off, the
    incident is a rollback (nothing it did came from a playbook) or this
    feedback was drafted already.
    """

    if not config.DRAFT_PLAYBOOK_EDITS or kind not in KINDS or not source_id:
        return None

    store = get_store()

    if rollback.is_rollback(store.incident(incident_id)):
        return None

    edit_id = store.add_playbook_edit(kind, incident_id, int(source_id), _now(),
                                      sources=[{"kind": kind, "incident_id": incident_id, "id": int(source_id)}])

    if edit_id is None:
        return None

    log(f"PE-{edit_id}: drafting from the {kind.replace('_', ' ')} of {incident_id}")

    return _submit(edit_id)


def redraftable(row: dict) -> bool:
    """FAILED, STALE, PROPOSED with checks it fails (which the text box may not fix), or cut off by a restart."""

    status = row["status"]

    return status in ("FAILED", "STALE") or (status == "PROPOSED" and bool(row.get("problems"))) or \
        (status == "DRAFTING" and not drafting(row["id"]))


def redraft(edit_id: int):
    """Draft an edit again from its feedback (redraftable() says which). Returns the Future."""

    store = get_store()
    row = store.playbook_edit(edit_id)

    if not row:
        raise ValueError(f"no playbook edit PE-{edit_id}")

    if not redraftable(row):
        raise ValueError(f"PE-{edit_id} is {row['status']}: only a failed or stale draft, or one that fails "
                         "its checks, is drafted again")

    store.update_playbook_edit(edit_id, status="DRAFTING", error="", decided_at=None, reviewer="", reason="")

    return _submit(edit_id)


def reset() -> None:
    """Forget queued drafts (tests)."""

    with _lock:
        _waiting.clear()


# ---------------------------------------------------------------------
# What the feedback was
# ---------------------------------------------------------------------


def sources_of(row: dict) -> list:
    return row.get("sources") or [{"kind": row["trigger_kind"], "incident_id": row["incident_id"],
                                   "id": row["source_id"]}]


def _edited(proposal: dict | None) -> bool:
    return bool(proposal) and (proposal.get("agent_version") or "").endswith("+review")


def evidence(store, source: dict) -> dict:
    """
    One piece of feedback: {kind, incident, proposal (the agent's), edited
    (the reviewer's steps, if they changed them), reason, reviewer, action,
    verdicts [(window seconds, verdict)]}.
    """

    kind, incident_id = source["kind"], source["incident_id"]
    proposals = store.proposals(incident_id)
    by_id = {p["id"]: p for p in proposals}
    found = {"kind": kind, "incident": store.incident(incident_id) or {"id": incident_id}, "proposal": None,
             "edited": None, "reason": "", "reviewer": "", "action": None, "verdicts": []}

    if kind == REJECT:
        row = next((r for r in store.reviews(incident_id) if r["id"] == source["id"]), None) or {}
        judged = by_id.get(row.get("proposal_id"))

    else:
        row = store.label(source["id"]) or {}
        action = store.action(row.get("action_id")) or {}
        judged = by_id.get(action.get("proposal_id"))
        found["action"] = action or None
        found["verdicts"] = [(o["window_seconds"], o["verdict"]) for o in store.outcomes(incident_id)
                             if o["action_id"] == action.get("id")]

    if _edited(judged):
        found["edited"] = judged.get("action") or []
        judged = next((p for p in reversed(proposals) if p["id"] < judged["id"] and not _edited(p)), None)

    agents = [p for p in proposals if not _edited(p)]
    found.update(proposal=judged or (agents[-1] if agents else None), reason=row.get("reason") or "",
                 reviewer=row.get("reviewer") or "")

    return found


def window_name(seconds) -> str:
    seconds = int(seconds or 0)

    return f"{seconds // 3600} h" if seconds >= 3600 and not seconds % 3600 else f"{seconds // 60} min"


# ---------------------------------------------------------------------
# The sections, as the agent reads them
# ---------------------------------------------------------------------


def sections(raws: dict) -> dict:
    """{section id: {playbook, heading, text, collection}} of every `## ` section of {file name: markdown}."""

    out = {}

    for source, raw in raws.items():
        collection = ingest.front_matter(raw).get("collection") or ingest.PLAYBOOKS
        head, rest = overlay.split(overlay.front(raw)[1])

        for heading, text in rest:
            out[f"{os.path.splitext(source)[0]}#{overlay.slug(heading)}"] = {
                "playbook": source, "heading": heading, "text": overlay.clean(text), "collection": collection}

    return out


def _title_slug(source: str, raw: str) -> str:
    return overlay.slug(ingest.front_matter(raw).get("title") or os.path.splitext(source)[0])


def resolve(section_id: str, playbook: str, heading: str, found: dict) -> str:
    """
    The section id the LLM meant: its own, or (as nemotron writes them) its
    slug without the playbook in front, or the section of that heading.
    An id that matches none comes back as it was, for check() to refuse.
    """

    if not section_id or section_id in found:
        return section_id

    tail = overlay.slug(section_id.rsplit("#", 1)[-1])
    base = os.path.splitext(playbook)[0] if playbook else section_id.split("#", 1)[0] if "#" in section_id else ""

    for candidate in (f"{base}#{tail}", f"{base}#{overlay.slug(heading)}" if heading else ""):
        if base and candidate in found:
            return candidate

    # No playbook to go by: the one section anywhere with that slug.
    matches = [c for c in found if c.split("#", 1)[1] == tail and (not base or c.startswith(base + "#"))]

    return matches[0] if len(matches) == 1 else section_id


def normalize(edit: PlaybookEdit, found: dict) -> PlaybookEdit:
    """The section id as the index writes it, the playbook from it when it was left out, and its own heading."""

    playbook = edit.playbook.strip()
    section_id = resolve(edit.section_id.strip(), playbook, edit.heading.strip(), found)

    if section_id in found and not playbook:
        playbook = found[section_id]["playbook"]

    if section_id and not playbook:
        playbook = section_id.split("#", 1)[0] + ".md"

    if playbook and not playbook.endswith(".md"):
        playbook += ".md"

    heading = found[section_id]["heading"] if section_id in found else edit.heading.strip()

    return edit.model_copy(update={"section_id": section_id, "playbook": playbook, "heading": heading,
                                   "text": overlay.clean(edit.text)})


def check(edit: dict, raws: dict, found: dict | None = None) -> list:
    """
    Why the agent must not read this edit ([] when it may). `edit`:
    {playbook, section_id, heading, text}; `raws`: {file name: markdown},
    with the approved edits applied (ingest.raw_playbooks).
    """

    found = sections(raws) if found is None else found
    playbook, section_id = edit.get("playbook") or "", edit.get("section_id") or ""
    text = overlay.clean(edit.get("text") or edit.get("new_text") or "")

    if playbook not in raws:
        return [f"there is no playbook {playbook!r}; edit one of: "
                + ", ".join(s for s in raws if _editable(raws[s]))]

    if not _editable(raws[playbook]):
        return [f"{playbook} holds the platform rules, which are not edited here"]

    problems, old = [], ""

    if section_id:
        if found.get(section_id, {}).get("playbook") != playbook:
            ids = [c for c, s in found.items() if s["playbook"] == playbook]
            problems.append(f"{playbook} has no section {section_id!r}; its sections are "
                            + ", ".join(ids) + " (or leave section_id empty and give a heading for a new one)")

        else:
            old = found[section_id]["text"]

    else:
        wanted = overlay.slug(edit.get("heading") or "")
        base = os.path.splitext(playbook)[0]

        if not wanted:
            problems.append("a new section needs a heading")

        elif f"{base}#{wanted}" in found or wanted == _title_slug(playbook, raws[playbook]):
            problems.append(f"{playbook} already has a section headed {edit.get('heading')!r}: "
                            f"rewrite it by its id, {base}#{wanted}")

    if text.startswith("---"):
        problems.append("the text starts with front matter (---); give only the section's body")

    if re.search(r"^#{1,2}\s", text, re.M):
        problems.append("the text has a # or ## heading line; the section keeps its heading, so give only its body")

    if len(text) < MIN_TEXT:
        problems.append(f"the text is {len(text)} characters; a section needs at least {MIN_TEXT}")

    elif len(text) > MAX_TEXT:
        problems.append(f"the text is {len(text)} characters; keep a section under {MAX_TEXT}")

    if section_id and old and text == old:
        problems.append("the text is the section as it already reads: nothing would change")

    for name, values in ingest._TAG.findall(text):
        unknown = [v for v in ingest._split_list(values) if v not in catalog.CATALOG] \
            if name.lower() == "actions" else []

        if unknown:
            problems.append(f"the actions line names {', '.join(unknown)}, which the catalog does not have")

        if not ingest._split_list(values):
            problems.append(f"the {name.lower()} line is empty; name what it lists, or leave the line out")

    if old:
        problems += _dropped(old, text)

    for phrase in FORBIDDEN:
        if text.lower().count(phrase) > old.lower().count(phrase):
            problems.append(f"the text adds {phrase!r}, which the agent must never do")

    if "crn:" in text.lower():
        problems.append("the text names a CRN; write guidance for any deployment")

    if re.search(r"\bINC-\d", text):
        problems.append("the text names an incident; write guidance for any incident")

    return problems


def _tags(text: str) -> dict:
    return {name.lower(): set(ingest._split_list(values)) for name, values in ingest._TAG.findall(text)}


def _rows(text: str) -> list:
    """A section's table rows, without the header separator."""

    return [line for line in text.splitlines() if line.strip().startswith("|") and set(line.strip()) - set("|-: ")]


def _dropped(old: str, new: str) -> list:
    """
    What a rewrite of an existing section loses that it must keep: what its
    tag lines list, its table rows (owners' values, which a row may change
    but not lose), and most of its lines (an edit changes what the feedback
    contradicts, not the whole section).
    """

    problems, tags = [], _tags(new)

    for name, values in _tags(old).items():
        lost = sorted(values - tags.get(name, set()))

        if lost:
            problems.append(f"the text drops {', '.join(lost)} from the section's {name} line; keep it")

    if len(_rows(new)) < len(_rows(old)):
        problems.append(f"the text keeps {len(_rows(new))} of the section's {len(_rows(old))} table rows; "
                        "keep every row (change a value in its row if the feedback shows it is wrong)")

    # Words, in order, so rewrapping a paragraph is not a rewrite.
    words = lambda text: ingest._TAG.sub(" ", text).split()  # noqa: E731
    before = words(old)
    kept = sum(b.size for b in difflib.SequenceMatcher(a=before, b=words(new), autojunk=False).get_matching_blocks())

    if before and kept < len(before) * KEEP:
        problems.append(f"the text keeps {kept * 100 // len(before)}% of the section's words; change only what "
                        "the feedback contradicts and keep the rest as it reads")

    return problems


def _editable(raw: str) -> bool:
    return (ingest.front_matter(raw).get("collection") or ingest.PLAYBOOKS) == ingest.PLAYBOOKS


def same_target(a: dict, b: dict) -> bool:
    """Two edits of the same section (or two new sections with the same heading)."""

    if a.get("playbook") != b.get("playbook"):
        return False

    if a.get("section_id") or b.get("section_id"):
        return target_id(a) == target_id(b)

    return overlay.slug(a.get("heading") or "") == overlay.slug(b.get("heading") or "")


def target_id(edit: dict) -> str:
    """The section id an edit writes: its own, or the one a new section gets."""

    return edit.get("section_id") or \
        f"{os.path.splitext(edit.get('playbook') or '')[0]}#{overlay.slug(edit.get('heading') or '')}"


# ---------------------------------------------------------------------
# The prompt
# ---------------------------------------------------------------------


def _steps(steps) -> str:
    return json.dumps(steps or [], sort_keys=True, default=str)


def _feedback_block(number: int, fact: dict) -> str:
    incident, proposal = fact["incident"], fact["proposal"] or {}
    lines = [f"Feedback {number}: incident {incident.get('id')} on process group \"{incident.get('process_group', '')}\".",
             f"Summary: {incident.get('summary') or ''}", "Signals:"]
    lines += [prompts._signal_line(s) for s in incident.get("signals") or []] or ["- none recorded"]
    lines += [f"The agent diagnosed: {proposal.get('diagnosis') or '(no diagnosis)'}",
              f"It proposed: {_steps(proposal.get('action'))}",
              f"Its rationale: {proposal.get('rationale') or ''}",
              f"It cited: {', '.join(proposal.get('citations') or []) or 'nothing'}"]

    if fact["edited"] is not None:
        lines.append(f"The reviewer changed the steps to: {_steps(fact['edited'])}")

    if fact["kind"] == REJECT:
        lines.append("The reviewer rejected it before it ran.")

    else:
        measured = ", ".join(f"{window_name(w)} {v}" for w, v in fact["verdicts"]) or "no verdict yet"
        lines.append(f"It was applied (measured: {measured}), and the reviewer labelled it a wrong fix.")

    lines.append(f"The reviewer's reason: \"{fact['reason']}\"")

    return "\n".join(lines)


def user_prompt(facts: list, found: dict, open_drafts: list) -> str:
    cited = []

    for fact in facts:
        for chunk_id in (fact["proposal"] or {}).get("citations") or []:
            if chunk_id not in cited:
                cited.append(chunk_id)

    blocks = []

    for chunk_id in cited:
        section = found.get(chunk_id)

        if section is None:
            continue

        note = "" if section["collection"] == ingest.PLAYBOOKS else " (platform rules: not editable)"
        blocks.append(f"[{chunk_id}] {section['heading']}{note}\n{section['text']}")

    open_text = ""

    if open_drafts:
        open_text = "\nOpen drafts of these sections, not yet decided (keep what they fix if it still holds):\n" + \
            "\n\n".join(f"[{target_id(d)}] drafted from: "
                        + "; ".join(f"\"{s['reason']}\"" for s in d["reasons"]) + f"\n{d['new_text']}"
                        for d in open_drafts) + "\n"

    toc = "\n".join(f"- {chunk_id}: {s['heading']} ({s['playbook']})" for chunk_id, s in found.items()
                    if s["collection"] == ingest.PLAYBOOKS)

    return prompts.DRAFT_USER.format(
        feedback="\n\n".join(_feedback_block(n, f) for n, f in enumerate(facts, 1)),
        cited="\n\n".join(blocks) or "(none of its citations is a playbook section)",
        open=open_text, toc=toc)


def system_prompt() -> str:
    return prompts.DRAFT_SYSTEM.format(
        actions=", ".join(name for name, spec in catalog.CATALOG.items() if not spec.hidden))


# ---------------------------------------------------------------------
# Drafting
# ---------------------------------------------------------------------


def _parse(raw: str) -> tuple:
    """(PlaybookEdit | None, error)."""

    try:
        return PlaybookEdit.model_validate(prompts.extract_json(raw)), ""

    except ValidationError as exc:
        return None, "; ".join(f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors())

    except ValueError as exc:
        return None, str(exc)


def draft(edit_id: int) -> dict:
    """Draft one DRAFTING row; returns it as it ends (PROPOSED, SKIPPED or FAILED)."""

    store = get_store()

    try:
        _draft(store, store.playbook_edit(edit_id))

    except Exception as exc:  # a draft must never take the worker down
        log(f"PE-{edit_id}: failed: {type(exc).__name__}: {exc}")
        store.update_playbook_edit(edit_id, status="FAILED", error=f"{type(exc).__name__}: {exc}")

    return store.playbook_edit(edit_id)


def _draft(store, row: dict) -> None:
    edit_id = row["id"]
    ok, why = llm.chat_available()

    if not ok:
        store.update_playbook_edit(edit_id, status="FAILED", error=f"LLM unavailable: {why}")
        log(f"PE-{edit_id}: no LLM: {why}")

        return

    sources = sources_of(row)
    facts = [evidence(store, s) for s in sources]
    raws = ingest.raw_playbooks()
    found = sections(raws)
    cited = {c for f in facts for c in (f["proposal"] or {}).get("citations") or []}
    open_drafts = [dict(d, reasons=[evidence(store, s) for s in sources_of(d)])
                   for d in store.playbook_edits(("PROPOSED",))
                   if d["id"] != edit_id and target_id(d) in cited]
    model = llm.chat_model()
    drafted_by = f"draft:{prompts.DRAFT_VERSION}/model:{getattr(model, 'model_name', '') or config.LLM_MODEL_ID}"
    messages = [SystemMessage(content=system_prompt()),
                HumanMessage(content=user_prompt(facts, found, open_drafts))]
    edit, problems, error, retried = None, [], "", False

    # A reply that does not parse gets one repair turn, an edit check() refuses one more.
    for _ in range(3):
        raw = prompts.text_of(llm.invoke(model, messages))
        parsed, error = _parse(raw)

        if parsed is None:
            if edit is not None:
                break

            messages = messages + [AIMessage(content=raw), HumanMessage(content=prompts.REPAIR.format(error=error))]
            continue

        edit = normalize(parsed, found)

        if edit.skip:
            break

        problems = check(edit.model_dump(), raws, found)

        if not problems or retried:
            break

        retried = True
        messages = messages + [AIMessage(content=raw), HumanMessage(content=prompts.DRAFT_FEEDBACK.format(
            problems="\n".join(f"- {p}" for p in problems)))]

    if edit is None:
        store.update_playbook_edit(edit_id, status="FAILED", drafted_by=drafted_by,
                                   error=f"the reply was not a usable edit: {error}")
        log(f"PE-{edit_id}: unusable reply: {error}")

        return

    if edit.skip:
        store.update_playbook_edit(edit_id, status="SKIPPED", drafted_by=drafted_by, rationale=edit.rationale,
                                   error="")
        log(f"PE-{edit_id}: skipped: {edit.rationale[:120]}")

        return

    target = {"playbook": edit.playbook, "section_id": edit.section_id, "heading": edit.heading}

    for older in store.playbook_edits(("PROPOSED",)):
        if older["id"] != edit_id and same_target(older, target):
            store.update_playbook_edit(older["id"], status="SUPERSEDED", decided_at=_now(),
                                       reason=f"drafted again as PE-{edit_id}")
            sources += [s for s in sources_of(older) if s not in sources]

    store.update_playbook_edit(
        edit_id, status="PROPOSED", playbook=edit.playbook, section_id=edit.section_id, heading=edit.heading,
        old_text=found.get(edit.section_id, {}).get("text", "") if edit.section_id else "",
        new_text=edit.text, rationale=edit.rationale, problems=problems, sources=sources,
        drafted_by=drafted_by, error="")
    log(f"PE-{edit_id}: proposed for {target_id(target)}" + (f" ({len(problems)} problem(s))" if problems else ""))
