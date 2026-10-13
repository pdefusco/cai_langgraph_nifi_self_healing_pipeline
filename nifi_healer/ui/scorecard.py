"""
The Scorecard tab: how often the agent picks the right fix, how often a
reviewer takes it as proposed, and what the applied fixes did. Server-
rendered HTML in the Monitor's style (its tokens and chips, under .mx).

Counts come from eval/scorecard.py; a reviewer's label on a fix counts
over the verdict measured for it.
"""

from nifi_healer.eval import scorecard as sc
from nifi_healer.ui.monitor import _e, _when

SINCE = {"24 h": 86400, "7 d": 7 * 86400, "All": None}
DEFAULT_SINCE = "7 d"
GROUPINGS = tuple(sc.GROUPINGS)
_RECENT = 50

_MATCH_CHIP = {sc.EXPECTED: ("mx-v-IMPROVED", "design fix"), sc.ACCEPTABLE: ("mx-v-IMPROVED", "acceptable"),
               sc.OTHER: ("mx-operator", "other fix"), sc.UNKNOWN: ("", "no label")}
_REVIEW_CHIP = {sc.APPROVED: "mx-agent", sc.EDITED: "mx-operator", sc.REJECTED: "mx-v-WORSENED",
                sc.UNDECIDED: "mx-v-pending", sc.NO_REVIEW: ""}
_RESULT_CHIP = {sc.WORKED: "mx-v-IMPROVED", sc.NO_HELP: "mx-v-NO_CHANGE", sc.WORSE: "mx-v-WORSENED",
                sc.UNCLEAR: "mx-v-INCONCLUSIVE", sc.PENDING: "mx-v-pending"}
_BAR = {sc.APPROVED: "approved", sc.EDITED: "edited", sc.REJECTED: "rejected", sc.UNDECIDED: "undecided",
        sc.WORKED: "worked", sc.NO_HELP: "nohelp", sc.WORSE: "worse", sc.UNCLEAR: "unclear",
        sc.PENDING: "pending"}


def model(store, now: float, since: str = DEFAULT_SINCE, grouping: str = GROUPINGS[0]) -> dict:
    span = SINCE.get(since, SINCE[DEFAULT_SINCE])
    found = sc.cases(store.record(now - span if span else 0.0))

    return {"since": since if since in SINCE else DEFAULT_SINCE, "grouping": grouping if grouping in sc.GROUPINGS
            else GROUPINGS[0], "now": now, "cases": found, "overall": sc.summarize(found),
            "rows": sc.table(found, grouping)}


def pct(rate) -> str:
    return "—" if rate is None else f"{rate * 100:.0f}%"


def duration(seconds) -> str:
    if seconds is None:
        return "—"

    if seconds < 60:
        return "<1 min"

    if seconds < 5400:
        return f"{seconds / 60:.0f} min"

    if seconds < 2 * 86400:
        return f"{seconds / 3600:.1f} h"

    return f"{seconds / 86400:.1f} d"


def _chip(css: str, text: str) -> str:
    return f'<span class="mx-chip {css}">{_e(text)}</span>' if css or text else ""


def _tile(label: str, value: str, sub: str) -> str:
    return (f'<div class="sc-tile"><span class="mx-eyebrow">{_e(label)}</span>'
            f'<b class="sc-big">{_e(value)}</b><small>{_e(sub)}</small></div>')


def _of(hits: int, total: int, what: str) -> str:
    return f"{hits} of {total} {what}" if total else f"no {what} yet"


def _tiles(s: dict) -> str:
    matched = s["match"][sc.EXPECTED] + s["match"][sc.ACCEPTABLE]
    labelled = sum(s["match"].values())
    decided = s["review"][sc.APPROVED] + s["review"][sc.EDITED] + s["review"][sc.REJECTED]
    worked = s["results"][sc.WORKED]

    return '<div class="sc-tiles">' + "".join((
        _tile("Incidents", str(s["incidents"]),
              f"{s['controls']} on the healthy control" if s["controls"] else "none on the healthy control"),
        _tile("Valid first try", pct(s["valid_first"]),
              _of(s["first_valid"], s["proposed"], "incidents proposed for")),
        _tile("Picked the design fix", pct(s["matched"]), _of(matched, labelled, "with a design label")),
        _tile("Approved as proposed", pct(s["approved"]), _of(s["review"][sc.APPROVED], decided, "decided")),
        _tile("Fix worked", pct(s["worked"]),
              _of(worked, s["judged"], "judged") + (f" · {s['labelled']} by a reviewer" if s["labelled"] else "")),
        _tile("Regressions", str(s["regressed"]), "worse in a later window"
              + (f" · {s['rolled_back']} fix(es) rolled back" if s["rolled_back"] else "")),
        _tile("Time to resolve", duration(s["to_resolve"]), "median, opened to resolved"),
    )) + "</div>"


def _bar(counts: dict, order: tuple) -> str:
    total = sum(counts.get(k, 0) for k in order)

    if not total:
        return '<span class="mx-none">—</span>'

    parts = "".join(f'<i class="sc-b-{_BAR[k]}" style="flex:{counts[k]}" title="{_e(k)}: {counts[k]}"></i>'
                    for k in order if counts.get(k))
    text = " · ".join(f"{counts[k]} {k}" for k in order if counts.get(k))

    return f'<div class="sc-bar" role="img" aria-label="{_e(text)}">{parts}</div><span class="sc-bar-t">{_e(text)}</span>'


_REVIEW_BARS = (sc.APPROVED, sc.EDITED, sc.REJECTED, sc.UNDECIDED)
_RESULT_BARS = (sc.WORKED, sc.NO_HELP, sc.WORSE, sc.UNCLEAR, sc.PENDING)


def _groups(m: dict) -> str:
    head = ("<tr><th>{}</th><th>Incidents</th><th>Valid first try</th><th>Design fix</th>"
            "<th>Review</th><th>Result</th><th>Worked</th><th>Regressions</th><th>To resolve</th></tr>"
            ).format(_e(m["grouping"]))
    rows = "".join(
        f"<tr><td class=\"sc-g\">{_e(name)}</td><td>{s['incidents']}</td><td>{pct(s['valid_first'])}</td>"
        f"<td>{pct(s['matched'])}</td><td class=\"sc-stack\">{_bar(s['review'], _REVIEW_BARS)}</td>"
        f"<td class=\"sc-stack\">{_bar(s['results'], _RESULT_BARS)}</td><td>{pct(s['worked'])}</td>"
        f"<td>{s['regressed'] or ''}</td><td>{duration(s['to_resolve'])}</td></tr>"
        for name, s in m["rows"])

    return (f'<div class="mx-ledger-head"><span class="mx-eyebrow">By {_e(m["grouping"].lower())}</span></div>'
            f'<div class="mx-scroll"><table class="mx-ledger sc-table"><thead>{head}</thead>'
            f"<tbody>{rows}</tbody></table></div>")


def _short(incident_id: str) -> str:
    return incident_id.split("-", 3)[-1]


def _case(c: dict, now: float) -> str:
    css, text = _MATCH_CHIP[c["match"]]
    result = ""

    if c["result"]:
        result = _chip(_RESULT_CHIP[c["result"]],
                       f"reviewer: {c['result']}" if c["result_from"] == "reviewer" else c["result"])

    elif c["dry_run"]:
        result = _chip("", "dry run")

    elif c["apply_failed"]:
        result = _chip("mx-v-WORSENED", "apply failed")

    flags = (_chip("mx-v-WORSENED", "regressed") if c["regressed"] else "") + \
        (_chip("mx-v-WORSENED", "rolled back") if c.get("rolled_back") else "") + \
        (_chip("", "healthy control") if c["control"] else "")
    first = "" if c["valid_first"] is None else "valid" if c["valid_first"] else \
        f"valid on try {c['repairs'] + 1}" if c["valid"] else "never valid"

    return (f"<tr><td><span class=\"sc-id\">{_e(_short(c['incident']))}</span>"
            f"<span class=\"mx-when\">{_e(_when(c['opened_at'], now - c['opened_at'] > 20 * 3600))}</span></td>"
            f"<td>{_e(c['scenario_title'])}</td><td class=\"sc-fix\">{_e(c['action'] or '—')}</td>"
            f"<td>{_e(first)}</td><td>{_chip(css, text) if c['proposed'] else ''}</td>"
            f"<td>{_chip(_REVIEW_CHIP[c['review']], c['review'])}</td>"
            f"<td>{result}</td><td><span class=\"mx-chips\">{flags}</span></td></tr>")


def _recent(m: dict) -> str:
    shown = m["cases"][::-1][:_RECENT]
    more = len(m["cases"]) - len(shown)
    head = ("<tr><th>Incident</th><th>Scenario</th><th>Agent's fix</th><th>First proposal</th>"
            "<th>Against the design</th><th>Review</th><th>Result</th><th></th></tr>")

    return (f'<div class="mx-ledger-head"><span class="mx-eyebrow">Incidents, newest first</span>'
            + (f"<p>The {_RECENT} newest of {len(m['cases'])}.</p>" if more > 0 else "") + "</div>"
            f'<div class="mx-scroll"><table class="mx-ledger sc-table sc-cases"><thead>{head}</thead><tbody>'
            + "".join(_case(c, m["now"]) for c in shown) + "</tbody></table></div>")


def render(m: dict) -> str:
    s = m["overall"]
    window = "all time" if m["since"] == "All" else f"the last {m['since']}"

    if not m["cases"]:
        return (f'<div class="mx sc"><p class="mx-empty">No incidents over {_e(window)}. '
                "The scorecard fills in as the agent handles them.</p></div>")

    return (f'<div class="mx sc"><div class="mx-head"><span class="mx-name">Agent scorecard</span>'
            f'<span class="mx-facts"><span>over <b>{_e(window)}</b></span>'
            f"<span><b>{s['applied']}</b> fixes applied</span><span><b>{s['dry_runs']}</b> dry runs</span>"
            + (f"<span><b>{s['apply_failed']}</b> failed to apply</span>" if s["apply_failed"] else "")
            + f"<span><b>{s['repairs']}</b> re-proposals</span></span></div>"
            + _tiles(s) + _groups(m) + _recent(m)
            + '<p class="mx-hint">The design fix is each scenario\'s known-correct lever, kept from the agent; '
              "a fix matches on its action and target, not its values. A reviewer's label on a fix (History → "
              "Correct the outcome) counts over its measured verdict. A healthy-control incident is a false "
              "alarm: the right proposal there is NO_ACTION.</p></div>")


CSS = """
.sc-tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:1px;background:var(--mx-line);border-bottom:1px solid var(--mx-line)}
.sc-tile{display:grid;gap:3px;padding:12px 16px;background:var(--mx-surface);align-content:start}
.sc-big{font:600 24px/1.1 var(--mx-mono);font-variant-numeric:tabular-nums}
.sc-tile small{font-size:11.5px;color:var(--mx-muted)}
.sc .sc-table td,.sc .sc-table th{text-align:left;vertical-align:middle}
.sc .sc-table td:nth-child(n+2):not(.sc-stack){font-variant-numeric:tabular-nums}
.sc .sc-table th:first-child,.sc .sc-table td:first-child{min-width:180px}
.sc-g{font:500 12px var(--mx-mono);white-space:normal;overflow-wrap:anywhere;max-width:360px}
.sc-stack{min-width:200px}
.sc-bar{display:flex;height:8px;border-radius:2px;overflow:hidden;gap:1px;background:var(--mx-surface-2)}
.sc-bar i{display:block;min-width:3px}
.sc-bar-t{display:block;margin-top:3px;font-size:11px;color:var(--mx-muted)}
.sc-b-approved{background:var(--mx-accent)}.sc-b-edited{background:var(--mx-operator)}
.sc-b-rejected{background:var(--mx-bad)}.sc-b-undecided{background:var(--mx-neutral-tint)}
.sc-b-worked{background:var(--mx-good)}.sc-b-worse{background:var(--mx-bad)}
.sc-b-nohelp{background:color-mix(in srgb,var(--mx-muted) 55%,transparent)}
.sc-b-unclear{background:var(--mx-neutral-tint)}
.sc-b-pending{background:repeating-linear-gradient(45deg,var(--mx-neutral-tint) 0 3px,transparent 3px 6px)}
.sc-id{display:block;font:500 12px var(--mx-mono)}
.sc-fix{font:500 12px var(--mx-mono)}
"""
