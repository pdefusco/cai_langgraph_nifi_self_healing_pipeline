"""
The Playbooks tab: the edits rag/drafter.py drafted from reviewers'
feedback, each with why it was drafted and its diff, for a person to
approve, change, reject or revert (ui/views.py decides).

    choices(store, view)     the dropdown, newest first
    panel(store, edit_id)    {html, text, can_approve, ..., key}

Every piece of text a reviewer or the LLM wrote is escaped (monitor._e).
"""

import difflib
import json
import os

from nifi_healer.rag import drafter, overlay
from nifi_healer.ui.monitor import _e
from nifi_healer.ui.views import clock

VIEWS = {"To review": ("DRAFTING", "PROPOSED", "FAILED", "STALE"), "Applied": ("APPLIED", "MERGED"), "All": None}
DEFAULT_VIEW = "To review"

_KIND = {drafter.REJECT: "a rejection", drafter.WRONG_FIX: "a wrong-fix label"}

# What each status means, beside it on the panel.
_MEANING = {
    "DRAFTING": "the LLM is drafting it",
    "PROPOSED": "waiting for a person; the agent does not read it yet",
    "FAILED": "the draft did not come through; Draft again retries it",
    "SKIPPED": "the LLM judged the feedback to be about the incident, not the playbook",
    "APPLIED": "the agent reads it",
    "REJECTED": "a person turned it down; the playbook stays as it was",
    "SUPERSEDED": "a newer draft of the same section took over its feedback",
    "REVERTED": "the agent no longer reads it",
    "STALE": "its section changed after it was drafted; Draft again rewrites it as the section reads now",
    "MERGED": "the playbook file in git now has it, so the overlay has nothing to apply",
}


def _source_line(row: dict) -> str:
    source = drafter.sources_of(row)[0]
    more = len(drafter.sources_of(row)) - 1

    return f"from {_KIND.get(source['kind'], source['kind'])} of {source['incident_id']}" \
        + (f" and {more} more" if more else "")


def label(row: dict) -> str:
    """PE-7 · backlog-slow-consumer › Raise concurrency first · from a rejection of INC-… · proposed"""

    where = (f"{os.path.splitext(row['playbook'])[0]} › {row.get('heading') or '?'}" if row.get("playbook")
             else "not drafted yet")

    return f"PE-{row['id']} · {where} · {_source_line(row)} · {row['status'].lower()}"


def choices(store, view: str = DEFAULT_VIEW, keep=None) -> list:
    """[(label, id)] of the view's edits, newest first; `keep` stays listed (the edit just decided)."""

    rows = store.playbook_edits(VIEWS.get(view or DEFAULT_VIEW, VIEWS[DEFAULT_VIEW]))
    ids = {r["id"] for r in rows}

    if keep and str(keep).isdigit() and int(keep) not in ids:
        kept = store.playbook_edit(int(keep))
        rows = sorted(rows + ([kept] if kept else []), key=lambda r: -r["id"])

    return [(label(r), str(r["id"])) for r in rows]


def _row(store, edit_id) -> dict | None:
    try:
        return store.playbook_edit(int(edit_id))

    except (TypeError, ValueError):
        return None


def key(store, edit_id) -> str:
    """Changes when the panel would: a draft finishing, a decision, the text."""

    row = _row(store, edit_id)

    if not row:
        return ""

    return f"{row['id']}|{row['status']}|{row.get('decided_at') or ''}|{hash(row.get('new_text') or '')}"


def panel(store, edit_id) -> dict:
    row = _row(store, edit_id)

    if not row:
        return {"html": '<div class="mx pb"><p class="mx-empty">No playbook edits here yet. A rejection or a '
                        "wrong-fix label drafts one from its reason.</p></div>",
                "text": "", "can_edit": False, "can_approve": False, "can_reject": False, "can_revert": False,
                "can_redraft": False, "key": ""}

    status = row["status"]
    cut_off = status == "DRAFTING" and not drafter.drafting(row["id"])

    return {"html": render(store, row), "text": row.get("new_text") or "",
            "can_edit": status == "PROPOSED", "can_approve": status == "PROPOSED",
            "can_reject": status in ("PROPOSED", "FAILED", "STALE") or cut_off,
            "can_revert": status == "APPLIED",
            "can_redraft": drafter.redraftable(row), "key": key(store, row["id"])}


# ---------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------


def _step(step: dict) -> str:
    rest = ", ".join(f"{k} {json.dumps(v, default=str)}" for k, v in step.items() if k not in ("action", "target"))

    return " ".join(p for p in (step.get("action") or "?", f"“{step['target']}”" if step.get("target") else "",
                                rest) if p)


def _steps(steps) -> str:
    if not steps:
        return '<span class="pb-muted">no steps</span>'

    return "<br>".join(f"<code>{_e(_step(s))}</code>" for s in steps if isinstance(s, dict))


def _why(store, source: dict) -> str:
    fact = drafter.evidence(store, source)
    incident, proposal = fact["incident"], fact["proposal"] or {}
    what = "Rejected before it ran" if fact["kind"] == drafter.REJECT else "Applied, then labelled a wrong fix"
    lines = [f'<div class="pb-src-h"><b>{_e(incident.get("id"))}</b>'
             f'<span>{_e(incident.get("process_group") or "")} · {_e(what)}'
             + (f" by {_e(fact['reviewer'])}" if fact["reviewer"] else "") + "</span></div>",
             f'<p class="pb-reason">“{_e(fact["reason"])}”</p>',
             '<dl class="pb-dl">',
             f"<dt>Agent diagnosed</dt><dd>{_e(proposal.get('diagnosis') or '(no diagnosis)')}</dd>",
             f"<dt>Agent proposed</dt><dd>{_steps(proposal.get('action'))}</dd>",
             f"<dt>Agent cited</dt><dd>{_e(', '.join(proposal.get('citations') or []) or 'nothing')}</dd>"]

    if fact["edited"] is not None:
        lines.append(f"<dt>Reviewer changed it to</dt><dd>{_steps(fact['edited'])}</dd>")

    if fact["kind"] == drafter.WRONG_FIX:
        measured = ", ".join(f"{drafter.window_name(w)} {v}" for w, v in fact["verdicts"]) or "no verdict yet"
        lines.append(f"<dt>Measured</dt><dd>{_e(measured)}</dd>")

    return '<div class="pb-src">' + "".join(lines) + "</dl></div>"


def diff_html(old: str, new: str) -> str:
    """The section's body, line by line: removed lines struck, added ones marked."""

    old_lines, new_lines = overlay.clean(old).splitlines(), overlay.clean(new).splitlines()
    out = []

    for op, a1, a2, b1, b2 in difflib.SequenceMatcher(a=old_lines, b=new_lines, autojunk=False).get_opcodes():
        if op == "equal":
            out += [f'<div class="pb-ln"><i> </i>{_e(line) or "&nbsp;"}</div>' for line in old_lines[a1:a2]]
            continue

        out += [f'<div class="pb-ln pb-del"><i>−</i>{_e(line) or "&nbsp;"}</div>' for line in old_lines[a1:a2]]
        out += [f'<div class="pb-ln pb-add"><i>+</i>{_e(line) or "&nbsp;"}</div>' for line in new_lines[b1:b2]]

    return '<div class="pb-diff" role="table" aria-label="Section diff">' + "".join(out) + "</div>"


def render(store, row: dict) -> str:
    status = row["status"]
    where = (f'<span class="mx-name">PE-{row["id"]}</span>'
             f'<span class="pb-pill pb-{status.lower()}">{_e(status)}</span>'
             f'<span class="pb-meaning">{_e(_MEANING.get(status, ""))}</span>')
    target = ""

    if row.get("playbook"):
        new = "new section in " if not row.get("section_id") else ""
        target = (f'<div class="pb-target">{_e(new)}<code>{_e(row["playbook"])}</code> › '
                  f'<b>{_e(row.get("heading") or "")}</b>'
                  + (f' <span class="pb-muted">{_e(row["section_id"])}</span>' if row.get("section_id") else "")
                  + "</div>")

    sources = "".join(_why(store, s) for s in drafter.sources_of(row))
    parts = [f'<div class="mx-head">{where}</div>', target,
             f'<section><h4>Why it was drafted</h4>{sources}</section>']

    if row.get("rationale"):
        parts.append(f'<section><h4>The drafter’s reasoning</h4><p>{_e(row["rationale"])}</p></section>')

    if row.get("new_text"):
        parts.append(f'<section><h4>{"The new section" if not row.get("section_id") else "The change"}</h4>'
                     f'{diff_html(row.get("old_text") or "", row["new_text"])}</section>')

    if row.get("problems"):
        parts.append('<section class="pb-problems"><h4>Checks it fails</h4><ul>'
                     + "".join(f"<li>{_e(p)}</li>" for p in row["problems"])
                     + "</ul><p>Fix them in the text below before approving, or Draft again (a section that "
                       "is not there can only be fixed by drafting again).</p></section>")

    if row.get("error"):
        parts.append(f'<section class="pb-problems"><h4>Error</h4><p>{_e(row["error"])}</p></section>')

    decided = []

    if row.get("decided_at"):
        decided.append(f"{status.lower()} {_e(clock(row['decided_at']))}"
                       + (f" by {_e(row['reviewer'])}" if row.get("reviewer") else ""))

    if row.get("reason"):
        decided.append(f"reason: “{_e(row['reason'])}”")

    foot = [f"drafted {_e(clock(row.get('created_at')))}"] \
        + ([f"by {_e(row['drafted_by'])}"] if row.get("drafted_by") else []) + decided

    parts.append(f'<div class="pb-foot">{" · ".join(foot)}</div>')

    return '<div class="mx pb">' + "".join(parts) + "</div>"


CSS = """
.pb section{padding:12px 16px;border-bottom:1px solid var(--mx-line)}
.pb h4{margin:0 0 8px;font:600 11px var(--font);letter-spacing:.06em;text-transform:uppercase;color:var(--mx-muted)}
.pb p{margin:0;max-width:75ch}
.pb code{font:12px var(--mx-mono)}
.pb-muted{color:var(--mx-muted)}
.pb-meaning{color:var(--mx-muted);font-size:12px}
.pb-target{padding:10px 16px;border-bottom:1px solid var(--mx-line)}
.pb-pill{font:600 11px var(--mx-mono);padding:2px 8px;border-radius:999px;background:var(--mx-neutral-tint)}
.pb-proposed,.pb-drafting,.pb-stale{background:var(--mx-operator-tint);color:var(--mx-operator)}
.pb-applied,.pb-merged{background:var(--mx-good-tint);color:var(--mx-good)}
.pb-failed,.pb-rejected{background:var(--mx-bad-tint);color:var(--mx-bad)}
.pb-src{display:grid;gap:6px}
.pb-src+.pb-src{margin-top:12px;padding-top:12px;border-top:1px dashed var(--mx-grid)}
.pb-src-h{display:flex;flex-wrap:wrap;gap:4px 12px;align-items:baseline}
.pb-src-h b{font:500 12px var(--mx-mono)}
.pb-src-h span{color:var(--mx-muted);font-size:12px}
.pb-reason{font-size:14px;padding-left:10px;border-left:3px solid var(--mx-operator)}
.pb-dl{display:grid;grid-template-columns:max-content 1fr;gap:4px 14px;margin:0}
.pb-dl dt{color:var(--mx-muted);font-size:12px}
.pb-dl dd{margin:0;min-width:0;overflow-wrap:anywhere}
.pb-diff{font:12px/1.55 var(--mx-mono);overflow-x:auto;border:1px solid var(--mx-line);border-radius:6px;background:var(--mx-surface-2)}
.pb-ln{display:flex;white-space:pre-wrap;overflow-wrap:anywhere;padding:0 10px 0 0}
.pb-ln i{flex:none;width:22px;text-align:center;font-style:normal;color:var(--mx-muted);user-select:none}
.pb-add{background:var(--mx-good-tint)}.pb-add i{color:var(--mx-good)}
.pb-del{background:var(--mx-bad-tint);text-decoration:line-through;text-decoration-color:color-mix(in srgb,var(--mx-bad) 60%,transparent)}
.pb-del i{color:var(--mx-bad);text-decoration:none}
.pb-problems ul{margin:0 0 6px;padding-left:18px}
.pb-problems{background:var(--mx-bad-tint)}
.pb-foot{padding:8px 16px;font-size:11.5px;color:var(--mx-muted)}
"""
