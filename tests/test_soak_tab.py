"""
The Soak tab (ui/soak.py): empty until chaos first ticks, then the soak's
tiles, its rows by fault kind and by day, and its episodes, whose
incidents open in History.
"""

import gradio as gr

import config
from flows import packs
from nifi_healer import chaos
from nifi_healer.agent import graph
from nifi_healer.eval import soak as episodes
from nifi_healer.runtime.poller import Poller
from nifi_healer.ui import gradio_app, legend, soak

from test_chaos import _wait
from test_memory import fleet as fleet_  # noqa: F401 (a fixture)
from test_soak import PACK, _opened, _until, soaked  # noqa: F401 (a fixture)

DAY = 86400


def test_the_soak_tab_waits_for_chaos(fleet_):
    world, client, store, _ = fleet_(PACK)
    m = soak.model(store, client.now())
    html = soak.render(m)

    assert (m["state"], m["since"], m["next"], m["episodes"]) == ("off", None, None, [])
    assert "Chaos is off" in html and "App health" in html
    assert "Chaos is off" in gradio_app.show_soak()


def test_a_soak_shows_its_episodes_by_kind_and_day(soaked):
    world, client, store = soaked
    scenario = packs.SCENARIOS["S20"]
    world.skip(600)

    m = soak.model(store, client.now())
    assert m["state"] == "on" and m["since"] is None and "Chaos has not ticked yet." in soak.render(m)

    row = chaos.tick(client, store, wait=_wait(world, PACK))
    assert row["status"] == chaos.PLANTED
    html = soak.render(soak.model(store, client.now()))
    assert "Faults planted" in html and "1 still open" in html and "not yet" in html

    incident = _until(world, Poller(client, store), lambda: _opened(store, row), scenario.detect_within)
    assert incident, "the planted fault was not detected"
    graph.start(incident["id"])
    graph.resume(incident["id"], {"decision": "approve", "reviewer": "test"})

    m = soak.model(store, client.now())
    [episode] = m["episodes"]
    assert episode["incidents"] == [incident["id"]]
    assert [kind for kind, _ in m["kinds"]] == [scenario.kind]
    assert len(m["days"]) == 1 + (client.now() // DAY > m["since"] // DAY)
    assert sum(s["injections"] for _, s in m["days"]) == 1
    assert m["ledger"][chaos.PLANTED] == 1 and m["next"] is not None

    html = soak.render(m)
    for words in ("By fault kind", "By day, newest first", "Episodes, newest first", scenario.group_name,
                  f'data-pick="{incident["id"]}"', "chaos <b>on</b>", "Picked the design fix"):
        assert words in html, words
    assert "${" not in html and "`" not in html

    # A click on the incident opens History at it; a click anywhere else does nothing.
    tabs, pick = gradio_app.open_in_history(gr.EventData(None, {"pick": incident["id"]}))
    assert tabs.selected == "history" and pick["value"] == incident["id"]
    assert gradio_app.open_in_history(gr.EventData(None, {})) == (gr.skip(), gr.skip())
    assert "Episodes, newest first" in gradio_app.show_soak()
    assert isinstance(gradio_app.build(), gr.Blocks)


def test_by_day_splits_at_utc_midnight_and_by_kind_follows_the_scenario(monkeypatch):
    since = 10 * DAY + 20 * 3600
    now = since + DAY
    planted = [dict(status=episodes.CLEARED, at=since + 3600, kind="classic"),
               dict(status=episodes.OPEN, at=since + 6 * 3600, kind="cascade")]
    alarms = [dict(opened_at=since + 600, scenario="S19"), dict(opened_at=since + 5 * 3600, scenario="S20")]
    monkeypatch.setattr(episodes, "summary", lambda found, theirs, start, end: (
        [e["at"] for e in found], [a["opened_at"] for a in theirs], start, end))

    # Each day over its own span, the first from the soak's start and today's to now.
    assert soak.by_day(planted, alarms, since, now) == [
        (10 * DAY, ([since + 3600], [since + 600], since, 11 * DAY)),
        (11 * DAY, ([since + 6 * 3600], [since + 5 * 3600], 11 * DAY, now))]
    assert soak.by_day(planted, alarms, None, now) == []

    # A false alarm counts under its scenario's kind: S19 is a control, S20 classic.
    assert soak.by_kind(planted, alarms, since, now) == [
        ("classic", ([since + 3600], [since + 5 * 3600], since, now)),
        ("cascade", ([since + 6 * 3600], [], since, now)),
        ("control", ([], [since + 600], since, now))]


def test_the_soak_legend_names_its_columns():
    for words in ("soak", "episode", "Detected", "Time to detect", "Review wait", "Approved by autonomy",
                  "False alarms", "Rollbacks", "By fault kind", "By day", "open it in History", "App health",
                  "never sees what was planted", f"{config.CHAOS_PER_DAY} a day"):
        assert words in legend.SOAK, words
