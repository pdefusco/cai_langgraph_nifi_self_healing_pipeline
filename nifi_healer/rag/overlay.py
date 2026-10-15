"""
Approved playbook edits over playbooks/*.md: the text the agent reads.

    raw, states = apply(raw, edits)     # one playbook's markdown, edited
    states[edit_id]                     # APPLIED | MERGED | STALE

An edit (store.playbook_edits, drafted by rag/drafter.py) replaces the
body of one `## ` section, or adds a section. The files are never
written: the git clone stays clean, so a pull can always fast-forward.

An edit applies only while its section still reads as the drafter saw
it (old_text). If the section now reads as the edit does, git has it
(MERGED, from patch() committed); any other text means the file changed
under it (STALE), and the file wins. Edits apply in id order, so a later
edit of a section reads the earlier one's text.
"""

import difflib
import hashlib
import os
import re

APPLIED, MERGED, STALE = "APPLIED", "MERGED", "STALE"

_FRONT = re.compile(r"\A---\n(.*?)\n---\n", re.S)


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def clean(text: str) -> str:
    """A section body as compared and stored: trimmed, no trailing spaces."""

    return "\n".join(line.rstrip() for line in (text or "").strip().splitlines())


def split(body: str) -> tuple:
    """(the text before the first `## ` heading, [(heading, the text under it)])."""

    head, sections, heading, lines = None, [], None, []

    for line in body.splitlines():
        if line.startswith("## "):
            if heading is None:
                head = lines

            else:
                sections.append((heading, "\n".join(lines)))

            heading, lines = line[3:].strip(), []

        else:
            lines.append(line)

    if heading is None:
        head = lines

    else:
        sections.append((heading, "\n".join(lines)))

    return "\n".join(head), sections


def front(raw: str) -> tuple:
    """(the front matter block with its fences, the rest)."""

    match = _FRONT.match(raw)

    return (raw[:match.end()], raw[match.end():]) if match else ("", raw)


def section_slug(section_id: str) -> str:
    return (section_id or "").split("#", 1)[-1]


def apply(raw: str, edits: list) -> tuple:
    """(the playbook's markdown with its edits, {edit id: APPLIED | MERGED | STALE})."""

    top, body = front(raw)
    head, sections = split(body)
    sections = [[heading, text] for heading, text in sections]
    states = {}

    for edit in sorted(edits, key=lambda e: e["id"]):
        new, old = clean(edit.get("new_text")), clean(edit.get("old_text"))
        wanted = section_slug(edit["section_id"]) if edit.get("section_id") else slug(edit.get("heading") or "")
        found = next((s for s in sections if slug(s[0]) == wanted), None)

        if found is not None and clean(found[1]) == new:
            states[edit["id"]] = MERGED

        elif found is None and not edit.get("section_id") and wanted:
            sections.append([edit["heading"].strip(), new + "\n"])
            states[edit["id"]] = APPLIED

        elif found is not None and edit.get("section_id") and clean(found[1]) == old:
            found[1] = new + "\n"
            states[edit["id"]] = APPLIED

        else:
            states[edit["id"]] = STALE

    if APPLIED not in states.values():
        return raw, states

    lines = head.split("\n")

    for heading, text in sections:
        if lines and lines[-1].strip():
            lines.append("")

        lines += [f"## {heading}"] + text.rstrip("\n").split("\n")

    return top + "\n".join(lines).rstrip("\n") + "\n", states


def edition(edits: list, states: dict) -> str:
    """A fingerprint of the edits the agent reads; "" when it reads the files as they are."""

    applied = sorted((e["id"], clean(e.get("new_text"))) for e in edits if states.get(e["id"]) == APPLIED)

    return hashlib.sha256(repr(applied).encode()).hexdigest()[:6] if applied else ""


def patch(playbooks_dir: str, edits: list) -> str:
    """The applied edits as a unified diff against the files, for `git apply` from the repo root."""

    out = []

    for source in sorted({e["playbook"] for e in edits}):
        path = os.path.join(playbooks_dir, source)

        if not os.path.exists(path):
            continue

        with open(path, encoding="utf-8") as handle:
            raw = handle.read()

        edited, states = apply(raw, [e for e in edits if e["playbook"] == source])

        if edited != raw:
            out += difflib.unified_diff(raw.splitlines(keepends=True), edited.splitlines(keepends=True),
                                        fromfile=f"a/playbooks/{source}", tofile=f"b/playbooks/{source}")

    return "".join(out)
