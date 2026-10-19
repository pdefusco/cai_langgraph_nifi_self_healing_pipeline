"""
The Soak tab: what became of every fault the chaos injector planted
since the soak began (eval/soak.py), overall, by fault kind and by day,
the latest episodes, and the app's own health over the week. Server-
rendered HTML in the Monitor's and the Scorecard's style.

An episode's incidents are buttons: a click opens it in History.
"""

import time

import config
from flows import packs
from flows.scenarios.base import KINDS
from nifi_healer import chaos
from nifi_healer.eval import soak
from nifi_healer.ui import monitor
from nifi_healer.ui.monitor import _e, _when
from nifi_healer.ui.scorecard import _chip, _of, _tile, duration, pct

RECENT = 50
HEALTH_DAYS = 7
HEALTH_STEP = 3600
DAY = 86400

_STATUS_CHIP = {soak.CLEARED: "mx-v-IMPROVED", soak.OPEN: "mx-v-pending", soak.MISSED: "mx-v-WORSENED",
                soak.GAVE_UP: "mx-v-WORSENED"}
_KIND = {"slow_burn": "slow burn"}

# The ledger's other outcomes, as the header counts them.
_OTHERS = ((chaos.DRY_RUN, "dry run(s)"), (chaos.UNCHANGED, "found the fault in place"),
           (chaos.REFUSED, "refused"), (chaos.FAILED, "failed"))


def model(store, now: float, wall: float | None = None) -> dict:
    """
    The soak since chaos first ticked on: its episodes, false alarms and
    summaries (overall, per fault kind, per UTC day), on the client's
    clock `now`; App health over the last week on the wall clock.
    """

    since = chaos.since(store)
    found = soak.episodes(store, now, since) if since is not None else []
    alarms = soak.false_alarms(store, found, now, since) if since is not None else []
    ledger = store.injections(since=since) if since is not None else []
    wall = time.time() if wall is None else wall
    state = chaos.state(store)
    nxt = chaos.next_candidate(store, now) if state != "off" else None

    return {"now": now, "since": since, "state": state, "next": nxt,
            "episodes": found, "alarms": alarms, "overall": soak.summary(found, alarms, since, now),
            "kinds": by_kind(found, alarms, since, now), "days": by_day(found, alarms, since, now),
            "ledger": {status: sum(r["status"] == status for r in ledger) for status in chaos.STATUSES},
            "health": monitor.health_model(store, wall, HEALTH_DAYS * DAY, HEALTH_STEP)}


def _kind_of(alarm: dict) -> str:
    scenario = packs.SCENARIOS.get(alarm.get("scenario") or "")

    return scenario.kind if scenario else "classic"


def by_kind(found: list, alarms: list, since: float | None, now: float) -> list:
    """[(kind, summary)] in KINDS order, for each kind with an episode or a false alarm."""

    rows = []

    for kind in KINDS:
        mine = [e for e in found if e["kind"] == kind]
        theirs = [a for a in alarms if _kind_of(a) == kind]

        if mine or theirs:
            rows.append((kind, soak.summary(mine, theirs, since, now)))

    return rows


def by_day(found: list, alarms: list, since: float | None, now: float) -> list:
    """
    [(day start, summary)] per UTC day from the soak's first, oldest
    first: the episodes planted that day and the false alarms opened
    that day, each rate over its own day (today's so far).
    """

    if since is None:
        return []

    rows = []
    day = since - since % DAY

    while day < now:
        end = day + DAY
        mine = [e for e in found if day <= e["at"] < end]
        theirs = [a for a in alarms if day <= a["opened_at"] < end]
        rows.append((day, soak.summary(mine, theirs, max(day, since), min(end, now))))
        day = end

    return rows


# ---------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------


def _per_day(s: dict) -> str:
    return "—" if s["false_alarms_per_day"] is None else f"{s['false_alarms_per_day']:.1f}"


def _tiles(m: dict) -> str:
    s, found = m["overall"], m["episodes"]
    detected = sum(e["detected_at"] is not None for e in found)
    judged = detected + sum(e["status"] == soak.MISSED for e in found)
    ended = sum(e["status"] != soak.OPEN for e in found)
    cleared = sum(e["status"] == soak.CLEARED for e in found)
    decided = [e for e in found if e["decided_by"]]
    auto = sum(e["decided_by"] == "autonomy" for e in decided)

    return '<div class="sc-tiles">' + "".join((
        _tile("Faults planted", str(s["injections"]),
              f"{s['open']} still open" if s["open"] else "none open"),
        _tile("Detected", pct(s["detected"]), _of(detected, judged, "detected or missed")),
        _tile("Time to detect", duration(s["mttd"]), "median, planted to incident"),
        _tile("Picked the design fix", pct(s["design_fix"]), "the first proposal, against the design"),
        _tile("Cleared", pct(s["cleared"]), _of(cleared, ended, "ended")),
        _tile("Time to clear", duration(s["time_to_clear"]),
              "median, planted to healthy" + (f" · {s['fixes_to_clear']:g} fix(es)"
                                              if s["fixes_to_clear"] is not None else "")),
        _tile("Review wait", duration(s["review_wait"]), "median, proposed to decided"),
        _tile("Approved by autonomy", pct(s["auto"]), _of(auto, len(decided), "decided")),
        _tile("False alarms a day", _per_day(s), f"{s['false_alarms']} over {s['days']:.1f} d"),
        _tile("Regressions", str(s["regressions"]),
              "worse in a later window" + (f" · {s['rollbacks']} rolled back" if s["rollbacks"] else "")),
    )) + "</div>"


_COLUMNS = ("<th title=\"Faults planted (still open)\">Planted</th><th>Detected</th><th>To detect</th>"
            "<th title=\"The first proposal picked the fix the scenario was built to need\">Design fix</th>"
            "<th>Cleared</th><th>To clear</th><th title=\"Median fixes applied, over the cleared\">Fixes</th>"
            "<th title=\"Median, proposed to decided\">Review wait</th><th>By autonomy</th>"
            "<th title=\"Incidents no fault explains, a day\">False alarms</th><th>Regressions</th>"
            "<th>Rollbacks</th>")


def _cells(s: dict) -> str:
    planted = f"{s['injections']}" + (f' <small class="sc-sub-i">({s["open"]} open)</small>' if s["open"] else "")
    fixes = "—" if s["fixes_to_clear"] is None else f"{s['fixes_to_clear']:g}"
    alarms = "—" if not s["false_alarms"] else f"{s['false_alarms']} · {_per_day(s)}/d"

    return (f"<td>{planted}</td><td>{pct(s['detected'])}</td><td>{duration(s['mttd'])}</td>"
            f"<td>{pct(s['design_fix'])}</td><td>{pct(s['cleared'])}</td><td>{duration(s['time_to_clear'])}</td>"
            f"<td>{fixes}</td><td>{duration(s['review_wait'])}</td><td>{pct(s['auto'])}</td>"
            f"<td>{_e(alarms)}</td><td>{s['regressions'] or ''}</td><td>{s['rollbacks'] or ''}</td>")


def _table(title: str, first: str, rows: list, note: str = "") -> str:
    return (f'<div class="mx-ledger-head"><span class="mx-eyebrow">{_e(title)}</span>{note}</div>'
            f'<div class="mx-scroll"><table class="mx-ledger sc-table so-table"><thead><tr><th>{_e(first)}</th>'
            f"{_COLUMNS}</tr></thead><tbody>" + "".join(rows) + "</tbody></table></div>")


def _kinds(m: dict) -> str:
    rows = [f'<tr><td class="sc-g">{_e(_KIND.get(kind, kind))}</td>{_cells(s)}</tr>' for kind, s in m["kinds"]]

    return _table("By fault kind", "Kind", rows)


def _days(m: dict) -> str:
    rows = [f'<tr><td class="sc-g">{_e(time.strftime("%a %d %b", time.gmtime(day)))}</td>{_cells(s)}</tr>'
            for day, s in m["days"][::-1]]

    return _table("By day, newest first", "Day (UTC)", rows, "<p>Each fault counts on the day it was planted.</p>")


def _after(seconds, status: str = "") -> str:
    if seconds is not None:
        return _e(duration(seconds))

    return f'<span class="mx-none">{_e(status)}</span>' if status else '<span class="mx-none">—</span>'


def _incidents(ids: list) -> str:
    return "".join(f'<button class="so-inc" data-pick="{_e(i)}" title="Open {_e(i)} in History">'
                   f'{_e(i.split("-", 3)[-1])}</button>' for i in ids)


def _episode(e: dict, now: float) -> str:
    scenario = packs.SCENARIOS[e["scenario"]]
    waiting = "" if e["status"] != soak.OPEN else "not yet"
    status = _chip(_STATUS_CHIP[e["status"]], e["status"]) + \
        (_chip("mx-v-WORSENED", "regressed") if e["regressed"] else "") + \
        (_chip("mx-v-WORSENED", "rolled back") if e["rolled_back"] else "") + \
        (_chip("mx-v-IMPROVED", "autonomy") if e["decided_by"] == "autonomy" else "")

    return (f'<tr><td><span class="sc-id">{_e(e["pack"])} › {_e(scenario.group_name)}</span>'
            f'<small class="sc-sub">{_e(_KIND.get(e["kind"], e["kind"]))} · {_e(e["family"])}</small></td>'
            f'<td><span class="mx-when">{_e(_when(e["at"], now - e["at"] > 20 * 3600))}</span></td>'
            f'<td>{_after(e["mttd"], "missed" if e["status"] == soak.MISSED else waiting)}</td>'
            f'<td>{e["fixes"] or ""}</td>'
            f'<td>{_after(e["time_to_clear"], waiting if e["status"] == soak.OPEN else "")}</td>'
            f'<td><span class="mx-chips">{status}</span></td><td>{_incidents(e["incidents"])}</td></tr>')


def _recent(m: dict) -> str:
    found = m["episodes"][::-1][:RECENT]
    note = f"<p>The {RECENT} newest of {len(m['episodes'])}.</p>" if len(m["episodes"]) > RECENT else ""
    head = ("<tr><th>Deployment › group</th><th>Planted</th><th>Detected after</th><th>Fixes</th>"
            "<th>Cleared after</th><th>Status</th><th title=\"Click one to open it in History\">Incidents</th></tr>")

    return (f'<div class="mx-ledger-head"><span class="mx-eyebrow">Episodes, newest first</span>{note}</div>'
            f'<div class="mx-scroll"><table class="mx-ledger sc-table so-eps"><thead>{head}</thead><tbody>'
            + "".join(_episode(e, m["now"]) for e in found) + "</tbody></table></div>")


def _alarms(m: dict) -> str:
    if not m["alarms"]:
        return ""

    rows = "".join(f'<tr><td><span class="sc-id">{_e(a["pack"] or "?")} › {_e(a["group"])}</span></td>'
                   f'<td><span class="mx-when">{_e(_when(a["opened_at"], m["now"] - a["opened_at"] > 20 * 3600))}'
                   f'</span></td><td>{_e(a["why"])}</td><td>{_incidents([a["incident"]])}</td></tr>'
                   for a in m["alarms"][::-1][:RECENT])

    return ('<div class="mx-ledger-head"><span class="mx-eyebrow">False alarms, newest first</span></div>'
            '<div class="mx-scroll"><table class="mx-ledger sc-table so-eps"><thead><tr><th>Deployment › group</th>'
            f"<th>Opened</th><th>Why no fault explains it</th><th>Incident</th></tr></thead><tbody>{rows}"
            "</tbody></table></div>")


def _state(m: dict) -> str:
    if m["state"] == "off":
        return "chaos <b>off</b>"

    nxt = m["next"]
    upcoming = "" if not nxt else f" · next {_e(nxt['pack'])} › {_e(nxt['scenario'])}" + \
        (f" ({_e(nxt['blocked'])})" if nxt.get("blocked") else "")

    return f"chaos <b>{_e(m['state'])}</b>{upcoming}"


def _facts(m: dict) -> str:
    rest = duration(config.CHAOS_REST).replace("<1 min", "none")
    others = "".join(f"<span><b>{m['ledger'][status]}</b> {_e(words)}</span>" for status, words in _OTHERS
                     if m["ledger"].get(status))

    return (f'<span class="mx-facts"><span>since <b>{_e(_when(m["since"], True))}</b> UTC '
            f"({m['overall']['days']:.1f} d)</span><span>{_state(m)}</span>"
            f"<span>rests {_e(rest)} · at most {config.CHAOS_PER_DAY} a day per deployment</span>{others}</span>")


def render(m: dict) -> str:
    health = monitor.render_health(m["health"])

    if m["since"] is None:
        why = {"off": "Chaos is off (<code>SELFHEAL_CHAOS</code>), so nothing has been planted.",
               "paused": "Chaos is paused and has not ticked yet."}.get(m["state"], "Chaos has not ticked yet.")

        return (f'<div class="mx sc so"><p class="mx-empty">{why} The soak starts on chaos\'s first tick, and this '
                "page fills in as it plants faults and the agent deals with them.</p></div>" + health)

    body = _tiles(m) + _kinds(m) + _days(m) + _recent(m) + _alarms(m) if m["episodes"] or m["alarms"] else \
        '<p class="mx-empty">Nothing planted yet: chaos plants a fault on a quiet deployment once it has rested.</p>'

    return (f'<div class="mx sc so"><div class="mx-head"><span class="mx-name">Soak</span>{_facts(m)}</div>'
            + body + '<p class="mx-hint">An episode runs from a plant until its KPIs stay healthy for the scenario\'s '
            "clear window after the last fix, or it is missed or given up on. Rates leave out what is still open. "
            "The agent never sees what was planted. The words behind every column are under How to read this page, "
            "above.</p></div>" + health)


CSS = """
.so .so-table td:first-child{min-width:120px}
.so .so-eps td:first-child{min-width:240px}
.sc-sub-i{font:400 11px var(--font);color:var(--mx-muted)}
.so-inc{font:500 12px var(--mx-mono);padding:1px 6px;margin:0 4px 2px 0;border:1px solid var(--mx-line);
  border-radius:3px;background:var(--mx-surface-2);color:var(--mx-ink);cursor:pointer}
.so-inc:hover,.so-inc:focus-visible{border-color:var(--mx-accent);color:var(--mx-accent);outline:none}
"""
