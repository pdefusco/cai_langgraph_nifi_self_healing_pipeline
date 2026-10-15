"""
The Scorecard tab: how often the agent picks the right fix, how often a
reviewer takes it as proposed, and what the applied fixes did. Server-
rendered HTML in the Monitor's style (its tokens and chips, under .mx).

Counts come from eval/scorecard.py; a reviewer's label on a fix counts
over the verdict measured for it.
"""

import re

from nifi_healer.eval import replay, scorecard as sc
from nifi_healer.kpi import metrics
from nifi_healer.ui.monitor import _e, _when, short
from nifi_healer.ui.views import window_label

SINCE = {"24 h": 86400, "7 d": 7 * 86400, "All": None}
DEFAULT_SINCE = "7 d"
GROUPINGS = tuple(sc.GROUPINGS)
LIVE, REPLAY = "Live", "Replay"
SOURCES = (LIVE, REPLAY)
_RECENT = 50

_MATCH_CHIP = {sc.EXPECTED: ("mx-v-IMPROVED", "design fix"), sc.ACCEPTABLE: ("mx-v-IMPROVED", "acceptable"),
               sc.OTHER: ("mx-operator", "other fix"), sc.UNKNOWN: ("", "no label")}
_REVIEW_CHIP = {sc.APPROVED: "mx-agent", sc.EDITED: "mx-operator", sc.REJECTED: "mx-v-WORSENED",
                sc.AUTO: "mx-v-IMPROVED", sc.UNDECIDED: "mx-v-pending", sc.NO_REVIEW: ""}
_RESULT_CHIP = {sc.WORKED: "mx-v-IMPROVED", sc.NO_HELP: "mx-v-NO_CHANGE", sc.WORSE: "mx-v-WORSENED",
                sc.UNCLEAR: "mx-v-INCONCLUSIVE", sc.PENDING: "mx-v-pending"}
_BAR = {sc.APPROVED: "approved", sc.EDITED: "edited", sc.REJECTED: "rejected", sc.AUTO: "auto",
        sc.UNDECIDED: "undecided"}


def model(store, now: float, since: str = DEFAULT_SINCE, grouping: str = GROUPINGS[0], source: str = LIVE) -> dict:
    """
    The live agent's incidents from the store, or with source REPLAY the
    offline replay's trials from its record (eval/replay.py), timed on
    the replay's own clock.
    """

    span = SINCE.get(since, SINCE[DEFAULT_SINCE])
    seen = None

    if source == REPLAY:
        snapshot = replay.load()
        now = (snapshot or {}).get("now") or now
        found = replay.cases(snapshot, now - span if span else 0.0) if snapshot else []
        seen = replay.detection(snapshot, now - span if span else 0.0) if snapshot else None

    else:
        found = sc.cases(store.record(now - span if span else 0.0))

    return {"since": since if since in SINCE else DEFAULT_SINCE, "grouping": grouping if grouping in sc.GROUPINGS
            else GROUPINGS[0], "now": now, "cases": found, "overall": sc.summarize(found),
            "rows": sc.table(found, grouping), "source": REPLAY if source == REPLAY else LIVE, "detection": seen}


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


def lift_text(lift) -> str:
    """+0.86 -> "+86%"."""

    if lift is None:
        return "—"

    return "0%" if abs(lift) < 0.005 else f"{lift * 100:+.0f}%".replace("-", "−")


def _tone(lift) -> str:
    return "" if lift is None or abs(lift) < 0.05 else "sc-up" if lift > 0 else "sc-down"


def _level(value) -> str:
    if abs(value) >= 1000:
        return short(abs(value))

    return f"{value:.1f}" if abs(value) < 10 and not float(value).is_integer() else f"{value:.0f}"


def _kpi_name(key: str) -> tuple:
    """("S01 Queue", "Flow Files Queued") from a KPI key, scenario:kind:component:metric."""

    parts = (key or "").split(":")

    if len(parts) != 4:
        return key or "?", ""

    try:
        label = metrics.metric(parts[3]).label

    except KeyError:
        label = parts[3]

    return parts[2] or label, label if parts[2] else ""


def _chip(css: str, text: str) -> str:
    return f'<span class="mx-chip {css}">{_e(text)}</span>' if css or text else ""


def _tile(label: str, value: str, sub: str) -> str:
    return (f'<div class="sc-tile"><span class="mx-eyebrow">{_e(label)}</span>'
            f'<b class="sc-big">{_e(value)}</b><small>{_e(sub)}</small></div>')


def _of(hits: int, total: int, what: str) -> str:
    return f"{hits} of {total} {what}" if total else f"no {what} yet"


def _tiles(s: dict, seen: dict | None = None) -> str:
    matched = s["match"][sc.EXPECTED] + s["match"][sc.ACCEPTABLE]
    labelled = sum(s["match"].values())
    decided = s["review"][sc.APPROVED] + s["review"][sc.EDITED] + s["review"][sc.REJECTED]
    worked = s["results"][sc.WORKED]

    if seen is None:
        approved = _tile("Approved as proposed", pct(s["approved"]), _of(s["review"][sc.APPROVED], decided, "decided"))

    else:   # the replay approves every valid proposal; what it can count instead is detection
        approved = _tile("Faults detected", pct(seen["detected"] / seen["faults"] if seen["faults"] else None),
                         _of(seen["detected"], seen["faults"], "fault trials")
                         + (f" · control quiet in {seen['quiet']} of {seen['controls']}" if seen["controls"] else ""))

    return '<div class="sc-tiles">' + "".join((
        _tile("Incidents", str(s["incidents"]),
              f"{s['controls']} on the healthy control" if s["controls"] else "none on the healthy control"),
        _tile("Valid first try", pct(s["valid_first"]),
              _of(s["first_valid"], s["proposed"], "incidents proposed for")),
        _tile("Picked the design fix", pct(s["matched"]), _of(matched, labelled, "with a design label")),
        approved,
        _tile("Fix worked", pct(s["worked"]),
              _of(worked, s["judged"], "judged") + (f" · {s['labelled']} by a reviewer" if s["labelled"] else "")),
        _tile("Median lift", lift_text(s["lift"]),
              " · ".join(x for x in (_spread(s), _measured_on(s)) if x) or "no fix measured yet"),
        _tile("Regressions", str(s["regressed"]), "worse in a later window"
              + (f" · {s['rolled_back']} fix(es) rolled back" if s["rolled_back"] else "")),
        _tile("Time to resolve", duration(s["to_resolve"]), "median, opened to resolved"),
    )) + "</div>"


def _spread(s: dict) -> str:
    """ "middle half +80% to +100% · 116 fixes": where most of the group's lifts fall."""

    if not s["lifted"]:
        return ""

    count = f"{s['lifted']} fix{'' if s['lifted'] == 1 else 'es'} measured"

    if not s["lift_range"]:
        return count

    low, high = s["lift_range"]

    return f"middle half {lift_text(low)} to {lift_text(high)} · {count}"


def _measured_on(s: dict) -> str:
    """
    The KPIs a group's lifts were read on: "on S01 Queue · Flow Files
    Queued" for one, else how many on each metric, "96 on Flow Files
    Queued · 20 on Bytes Sent".
    """

    found = s.get("lifted_on") or {}

    if len(found) == 1:
        name, metric = _kpi_name(next(iter(found)))

        return f"on {name}" + (f" · {metric}" if metric else "")

    kinds = {}

    for key, n in found.items():
        name, metric = _kpi_name(key)
        kinds[metric or name] = kinds.get(metric or name, 0) + n

    return " · ".join(f"{n} on {kind}" for kind, n in sorted(kinds.items(), key=lambda kv: (-kv[1], kv[0])))


def _lift_cell(s: dict) -> str:
    if s["lift"] is None:
        return '<span class="mx-none">—</span>'

    return (f'<b class="sc-lift {_tone(s["lift"])}">{lift_text(s["lift"])}</b>'
            f'<small class="sc-sub">{_e(_spread(s))}</small>'
            + (f'<small class="sc-sub">measured {_e(_measured_on(s))}</small>' if s.get("lifted_on") else ""))


def _bar(counts: dict, order: tuple) -> str:
    total = sum(counts.get(k, 0) for k in order)

    if not total:
        return '<span class="mx-none">—</span>'

    parts = "".join(f'<i class="sc-b-{_BAR[k]}" style="flex:{counts[k]}" title="{_e(k)}: {counts[k]}"></i>'
                    for k in order if counts.get(k))
    text = " · ".join(f"{counts[k]} {k}" for k in order if counts.get(k))

    return f'<div class="sc-bar" role="img" aria-label="{_e(text)}">{parts}</div><span class="sc-bar-t">{_e(text)}</span>'


_REVIEW_BARS = (sc.APPROVED, sc.EDITED, sc.REJECTED, sc.AUTO, sc.UNDECIDED)


_FLAGS = (("/no-memory", "memory off"), ("/no-priors", "track records off"))

# prompts.agent_version's last segment, when approved playbook edits were read.
_EDITION = re.compile(r"/playbooks:([0-9a-f]+)$")


def agent_name(version: str) -> tuple:
    """
    ("instructions fa8c5675be", "nemotron-3-super-120b-a12b · memory on")
    from prompt:fa8c5675be/model:nvidia/nemotron-3-super-120b-a12b; the
    model id has slashes of its own, so the flags come off its end (the
    playbook edition first, it is last).
    """

    if not version.startswith("prompt:") or "/model:" not in version:
        return version, ""

    prompt, model = version[len("prompt:"):].split("/model:", 1)
    flags = []
    edited = _EDITION.search(model)

    if edited:
        model = model[:edited.start()]

    for flag, words in _FLAGS * 2:
        if model.endswith(flag):
            model = model[:-len(flag)]
            flags.insert(0, words)

    if "memory off" not in flags:
        flags.insert(0, "memory on")

    if edited:
        flags.append(f"playbooks edited {edited.group(1)}")

    model = "oracle (the design fix, no LLM)" if model == "oracle" else model.rsplit("/", 1)[-1]

    return f"instructions {prompt}", " · ".join([model] + flags)


def _name(m: dict, group: str, s: dict | None = None) -> str:
    if m["grouping"] != "Agent version":
        return _e(group)

    title, sub = agent_name(group)
    since = f" · since {_when(s['first_at'], True)}" if s and s.get("first_at") else ""

    return (f'<span class="sc-v" title="{_e(group)}">{_e(title)}</span>'
            + (f'<small class="sc-sub">{_e(sub + since)}</small>' if sub or since else ""))


def _groups(m: dict) -> str:
    head = ("<tr><th>{}</th><th>Incidents</th><th>Valid first try</th><th>Design fix</th>"
            "<th title=\"Proposals that cite a past case (memory)\">Cited a case</th>"
            "<th>Review</th><th title=\"How far the fixes moved the KPIs that opened their incidents, "
            "−100% to +100%: the median, and where the middle half falls\">Lift</th>"
            "<th>Worked</th><th>Regressions</th><th>To resolve</th></tr>"
            ).format(_e(m["grouping"]))
    rows = "".join(
        f"<tr><td class=\"sc-g\">{_name(m, name, s)}</td><td>{s['incidents']}</td><td>{pct(s['valid_first'])}</td>"
        f"<td>{pct(s['matched'])}</td><td>{pct(s['cited'])}</td>"
        f"<td class=\"sc-stack\">{_bar(s['review'], _REVIEW_BARS)}</td>"
        f"<td class=\"sc-stack\">{_lift_cell(s)}</td><td>{pct(s['worked'])}</td>"
        f"<td>{s['regressed'] or ''}</td><td>{duration(s['to_resolve'])}</td></tr>"
        for name, s in m["rows"])

    return (f'<div class="mx-ledger-head"><span class="mx-eyebrow">By {_e(m["grouping"].lower())}</span></div>'
            f'<div class="mx-scroll"><table class="mx-ledger sc-table"><thead>{head}</thead>'
            f"<tbody>{rows}</tbody></table></div>")


def _short(incident_id: str) -> str:
    return incident_id.split("-", 3)[-1]


def _moved(c: dict) -> str:
    """The incident's lift, with the KPI and levels behind it; a word where nothing was measured."""

    k = c.get("lift_kpi")
    label = f"reviewer: {c['result']}" if c["result_from"] == "reviewer" else ""

    if k:
        sub = " · ".join(x for x in ("so far" if c["result"] == sc.PENDING else "",
                                     f"measured {k['verdict'].replace('_', ' ').lower()}", label) if x)

        return (f'<b class="sc-lift {_tone(c["lift"])}">{lift_text(c["lift"])}</b>'
                f'<small class="sc-sub">{_e(sub)}</small>')

    if label:
        return _chip(_RESULT_CHIP[c["result"]], label)

    words = {sc.PENDING: "measuring", sc.UNCLEAR: "too little data"}.get(c["result"]) or \
        ("dry run" if c["dry_run"] else "")

    if c["apply_failed"] and not c["result"]:
        return _chip("mx-v-WORSENED", "apply failed")

    return f'<span class="mx-none">{_e(words)}</span>' if words else ""


def _on(c: dict) -> str:
    """The KPI an incident's lift was read on, and its two levels."""

    k = c.get("lift_kpi")

    if not k:
        return ""

    name, metric = _kpi_name(k["kpi"])
    window = window_label(k["window"])
    levels = f"{_level(k['before'])} → {_level(k['after'])}"
    how = (f"average {levels}, before vs the {window} after the fix · higher is better" if k["good"] == "up" else
           f"{levels}, when fixed vs {window} later · lower is better")

    return (f'<span class="sc-kpi">{_e(name)}</span>'
            f'<small class="sc-sub">{_e(" · ".join(x for x in (metric, how) if x))}</small>')


def _case(c: dict, now: float) -> str:
    css, text = _MATCH_CHIP[c["match"]]
    result = _moved(c)

    cited = c.get("cited") or []
    flags = (f'<span class="mx-chip" title="{_e(", ".join(cited))}">cited '
             + (_e(_short(cited[0].split(":", 1)[1])) if len(cited) == 1 else f"{len(cited)} past cases")
             + "</span>" if cited else "") + \
        (_chip("mx-v-WORSENED", "regressed") if c["regressed"] else "") + \
        (_chip("mx-v-WORSENED", "rolled back") if c.get("rolled_back") else "") + \
        (_chip("", "healthy control") if c["control"] else "")
    first = "" if c["valid_first"] is None else "valid" if c["valid_first"] else \
        f"valid on try {c['repairs'] + 1}" if c["valid"] else "never valid"

    return (f"<tr><td><span class=\"sc-id\">{_e(_short(c['incident']))}</span>"
            f"<span class=\"mx-when\">{_e(_when(c['opened_at'], now - c['opened_at'] > 20 * 3600))}</span></td>"
            f"<td>{_e(c['scenario_title'])}</td><td class=\"sc-fix\">{_e(c['action'] or '—')}</td>"
            f"<td>{_e(first)}</td><td>{_chip(css, text) if c['proposed'] else ''}</td>"
            f"<td>{_chip(_REVIEW_CHIP[c['review']], c['review'])}</td>"
            f"<td>{result}</td><td class=\"sc-on\">{_on(c)}</td>"
            f"<td><span class=\"mx-chips\">{flags}</span></td></tr>")


def _recent(m: dict) -> str:
    # Each group shows its own newest, so an older run is not cut off by a newer one.
    key = sc.GROUPINGS[m["grouping"]]
    groups = {}

    for c in m["cases"][::-1]:
        groups.setdefault(key(c), []).append(c)

    each = max(_RECENT // max(len(groups), 1), 10)
    head = ("<tr><th>Incident</th><th>Scenario</th><th>Agent's fix</th><th>First proposal</th>"
            "<th>Against the design</th><th>Review</th><th title=\"How far the fix moved the KPI that opened "
            "the incident, −100% to +100%\">Lift</th><th title=\"The KPI the lift was read on: the one that opened "
            "the incident, and its level when the fix went in and at the end of the window\">Measured on</th>"
            "<th></th></tr>")
    body, cut = [], False

    for name, _ in m["rows"]:
        found = groups.get(name) or []
        cut = cut or len(found) > each

        if len(groups) > 1:
            title, sub = agent_name(name) if m["grouping"] == "Agent version" else (name, "")
            body.append(f'<tr class="mx-grp"><td colspan="9" title="{_e(name)}">{_e(title)}'
                        + (f'<span>{_e(sub)}</span>' if sub else "") + '<span>'
                        + (f"the {each} newest of {len(found)}" if len(found) > each else f"{len(found)}")
                        + "</span></td></tr>")

        body += [_case(c, m["now"]) for c in found[:each]]

    title = "Incidents, newest first" + (f" in each {m['grouping'].lower()}" if len(groups) > 1 else "")
    note = f"<p>The {each} newest of {len(m['cases'])}.</p>" if cut and len(groups) == 1 else ""

    return (f'<div class="mx-ledger-head"><span class="mx-eyebrow">{_e(title)}</span>{note}</div>'
            f'<div class="mx-scroll"><table class="mx-ledger sc-table sc-cases"><thead>{head}</thead><tbody>'
            + "".join(body) + "</tbody></table></div>")


_HOW_TO_REPLAY = ("Run one as a CAI job: script <code>cai_job.py</code>, Arguments "
                  "<code>-m nifi_healer.eval.replay --trials 3</code>. It replays every scenario on its own mock "
                  "tenant against the configured LLM, and never touches the live one.")


def render(m: dict) -> str:
    s = m["overall"]
    window = "all time" if m["since"] == "All" else f"the last {m['since']}"
    seen = m.get("detection")
    replayed = m.get("source") == REPLAY

    if not m["cases"]:
        if replayed:
            return (f'<div class="mx sc"><p class="mx-empty">'
                    + ("No replay runs yet. " if seen is None else f"No replay trials over {_e(window)}. ")
                    + _HOW_TO_REPLAY + "</p></div>")

        return (f'<div class="mx sc"><p class="mx-empty">No incidents over {_e(window)}. '
                "The scorecard fills in as the agent handles them.</p></div>")

    facts = (f"<span><b>{seen['runs']}</b> run(s)</span>" if replayed else "") + \
        f"<span><b>{s['applied']}</b> fixes applied</span>" + \
        ("" if replayed else f"<span><b>{s['dry_runs']}</b> dry runs</span>")
    hint = ("Replay trials run on a fresh mock tenant each round, against the configured LLM; every valid "
            "proposal was approved automatically (mock only), so Review shows no reviewer's judgement, and the "
            "replay's own clock dates them. A fault is detected when it opens an incident within 30 simulated "
            "minutes. " if replayed else "")

    return (f'<div class="mx sc"><div class="mx-head"><span class="mx-name">'
            f'{"Replay scorecard" if replayed else "Agent scorecard"}</span>'
            f'<span class="mx-facts"><span>over <b>{_e(window)}</b></span>' + facts
            + (f"<span><b>{s['apply_failed']}</b> failed to apply</span>" if s["apply_failed"] else "")
            + f"<span><b>{s['repairs']}</b> re-proposals</span></span></div>"
            + _tiles(s, seen if replayed else None) + _groups(m) + _recent(m)
            + f'<p class="mx-hint">{hint}Lift: how far a fix moved the KPI that opened its incident (Measured on), '
              "from −100% (as bad as it gets) through 0 (no change) to +100% (a backlog cleared, a stopped rate "
              "back). The words behind every column are under How to read this page, above.</p></div>")


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
.sc-b-auto{background:var(--mx-good)}
.sc-lift{display:block;font:600 15px/1.2 var(--mx-mono);font-variant-numeric:tabular-nums}
.sc-up{color:var(--mx-good)}.sc-down{color:var(--mx-bad)}
.sc-sub{display:block;margin-top:2px;font:400 11px var(--font);color:var(--mx-muted);white-space:normal}
.sc-v{display:block;font:500 12px var(--mx-mono)}
.sc-kpi{display:block;font:500 12px var(--mx-mono)}
.sc .sc-table td.sc-on{min-width:200px;white-space:normal}
.sc-id{display:block;font:500 12px var(--mx-mono)}
.sc-fix{font:500 12px var(--mx-mono)}
.sc tr.mx-grp td{font-family:var(--mx-mono);font-size:12px}
.sc tr.mx-grp span{margin-left:12px;font:400 11.5px var(--font);color:var(--mx-muted)}
"""
