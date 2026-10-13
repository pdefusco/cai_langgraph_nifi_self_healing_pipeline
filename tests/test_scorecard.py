"""
The agent scorecard: one case per incident (eval/scorecard.py), the
rates over a grouping, and the Scorecard tab's HTML (ui/scorecard.py).
"""

import config
from flows import packs
from nifi_healer.eval import scorecard as sc
from nifi_healer.ui import gradio_app, scorecard

from test_agent_graph import agent  # noqa: F401 (a fixture)
from test_monitor import _approved_s01

S01, S07, S19 = packs.SCENARIOS["S01"], packs.SCENARIOS["S07"], packs.SCENARIOS["S19"]
FIRST, SECOND, LAST = config.VERIFY_WINDOWS


def test_match_counts_the_lever_not_its_value():
    assert sc.match([{"action": "SET_CONCURRENT_TASKS", "target": "S01 Consumer", "value": 4}], S01) == sc.EXPECTED
    assert sc.match([{"action": "SET_CONCURRENT_TASKS", "target": "S01 Consumer", "value": 8}], S01) == sc.EXPECTED
    assert sc.match([{"action": "SET_RUN_SCHEDULE", "target": "S01 Consumer", "value": "1 sec"}], S01) == \
        sc.ACCEPTABLE
    assert sc.match([{"action": "SET_CONCURRENT_TASKS", "target": "S01 Source", "value": 4}], S01) == sc.OTHER
    assert sc.match([], S01) == sc.OTHER
    assert sc.match([{"action": "SET_CONCURRENT_TASKS", "target": "S07 Sink", "value": 4}], S07) == sc.ACCEPTABLE

    # The healthy control: doing nothing is the fix, however it is written.
    assert sc.match([], S19) == sc.EXPECTED and sc.match([{"action": "NO_ACTION"}], S19) == sc.EXPECTED
    assert sc.match([{"action": "SET_CONCURRENT_TASKS", "target": "x", "value": 2}], S19) == sc.OTHER
    assert sc.match([{"action": "NO_ACTION"}], None) == sc.UNKNOWN


def _incident(n, scenario, status, opened=1000.0, closed=None):
    return {"id": f"inc-20261013-0000-{n}", "scenario": scenario, "process_group": "", "status": status,
            "opened_at": opened, "closed_at": closed}


def _proposal(pid, n, steps, ok=True, version="prompt:a/model:m"):
    return {"id": pid, "incident_id": f"inc-20261013-0000-{n}", "agent_version": version, "action": steps,
            "validation": {"ok": ok}, "status": "SUPERSEDED"}


def _action(aid, n, pid, status="APPLIED", dry_run=0):
    return {"id": aid, "incident_id": f"inc-20261013-0000-{n}", "proposal_id": pid, "status": status,
            "dry_run": dry_run}


def _outcome(n, aid, window, verdict, regression=False):
    return {"incident_id": f"inc-20261013-0000-{n}", "action_id": aid, "window_seconds": window,
            "verdict": verdict, "details": {"regression": True} if regression else {}}


def _review(n, decision):
    return {"incident_id": f"inc-20261013-0000-{n}", "decision": decision}


FIX = [dict(S01.expected[0])]
OTHER = [{"action": "SET_CONCURRENT_TASKS", "target": "S01 Source", "value": 2}]


def _record():
    return {
        "incidents": [
            _incident(1, "S01", "RESOLVED", 1000, 1000 + 1800),       # approved, worked, then regressed
            _incident(2, "S01", "UNRESOLVED", 2000, 2000 + 4500),     # invalid first; worked per the reviewer
            _incident(3, "S01", "RESOLVED", 3000, 3000 + 1200),       # edited, measured worse
            _incident(4, "S01", "REJECTED", 4000, 4100),
            _incident(5, "S19", "NO_ACTION", 5000, 5100),             # the healthy control
            _incident(6, "S01", "DRY_RUN", 6000, 6100),
            _incident(7, "S01", "VERIFYING", 7000),                   # 15 min unclear, 1 h to come
            _incident(8, "S01", "PENDING_REVIEW", 8000),
        ],
        "proposals": [
            _proposal(1, 1, FIX),
            _proposal(2, 2, [], ok=False), _proposal(3, 2, FIX),
            _proposal(4, 3, OTHER), _proposal(5, 3, FIX, version="prompt:a/model:m+review"),
            _proposal(6, 4, OTHER, version="prompt:b/model:m"),
            _proposal(7, 5, [{"action": "NO_ACTION"}]),
            _proposal(8, 6, FIX), _proposal(9, 7, FIX), _proposal(10, 8, FIX),
        ],
        "reviews": [_review(1, "approve"), _review(2, "approve"), _review(3, "edit"), _review(3, "approve"),
                    _review(4, "reject"), _review(6, "approve"), _review(7, "approve")],
        "actions": [_action(1, 1, 1), _action(2, 2, 3), _action(3, 3, 5),
                    _action(4, 6, 8, status="DRY_RUN", dry_run=1), _action(5, 7, 9)],
        "outcomes": [_outcome(1, 1, FIRST, "IMPROVED"), _outcome(1, 1, LAST, "WORSENED", regression=True),
                     _outcome(2, 2, FIRST, "NO_CHANGE"), _outcome(2, 2, SECOND, "NO_CHANGE"),
                     _outcome(3, 3, FIRST, "WORSENED"), _outcome(7, 5, FIRST, "INCONCLUSIVE")],
        "action_labels": [{"incident_id": "inc-20261013-0000-2", "action_id": 2, "label": "WRONG_FIX"},
                          {"incident_id": "inc-20261013-0000-2", "action_id": 2, "label": "RIGHT_FIX"}],
    }


def test_cases_take_the_reviewers_label_over_the_verdict():
    found = {c["incident"][-1]: c for c in sc.cases(_record())}

    one = found["1"]
    assert (one["match"], one["review"], one["result"], one["result_from"]) == \
        (sc.EXPECTED, sc.APPROVED, sc.WORKED, "measured")
    assert one["regressed"] and one["valid_first"] and one["origin"] == "agent as proposed"

    # Its latest label, RIGHT_FIX, over two NO_CHANGE windows.
    two = found["2"]
    assert (two["valid_first"], two["valid"], two["repairs"]) == (False, True, 1)
    assert (two["result"], two["result_from"]) == (sc.WORKED, "reviewer")

    # The agent is judged by its own pick; the reviewer's edit is what ran.
    three = found["3"]
    assert (three["match"], three["review"], three["origin"], three["result"]) == \
        (sc.OTHER, sc.EDITED, "reviewer's edit", sc.WORSE)

    assert (found["4"]["review"], found["4"]["result"], found["4"]["agent_version"]) == \
        (sc.REJECTED, None, "prompt:b/model:m")
    assert (found["5"]["control"], found["5"]["match"], found["5"]["action"], found["5"]["review"]) == \
        (True, sc.EXPECTED, "NO_ACTION", sc.NO_REVIEW)
    assert (found["6"]["dry_run"], found["6"]["applied"], found["6"]["result"]) == (True, False, None)
    assert found["7"]["result"] == sc.PENDING and found["8"]["review"] == sc.UNDECIDED


def test_summary_and_groups():
    found = sc.cases(_record())
    s = sc.summarize(found)

    assert (s["incidents"], s["controls"], s["proposed"], s["first_valid"]) == (8, 1, 8, 7)
    assert s["match"] == {sc.EXPECTED: 6, sc.ACCEPTABLE: 0, sc.OTHER: 2} and s["matched"] == 6 / 8
    assert s["review"][sc.APPROVED] == 4 and s["review"][sc.EDITED] == 1 and s["review"][sc.REJECTED] == 1
    assert s["approved"] == 4 / 6
    assert (s["applied"], s["dry_runs"], s["judged"], s["worked"], s["labelled"]) == (4, 1, 3, 2 / 3, 1)
    assert s["regressed"] == 1 and s["to_resolve"] == 1500        # median of 1800 and 1200

    rows = dict(sc.table(found, "Agent version"))
    assert set(rows) == {"prompt:a/model:m", "prompt:b/model:m"} and rows["prompt:b/model:m"]["incidents"] == 1
    assert [name for name, _ in sc.table(found, "Who chose the fix")] == ["agent as proposed", "reviewer's edit"]
    assert dict(sc.table(found, "Scenario"))[S19.group_name]["controls"] == 1
    assert sc.summarize([])["worked"] is None


def test_scorecard_from_the_store_and_its_html(agent):
    client, store, entry, incident = _approved_s01(agent)
    now = client.now()

    m = scorecard.model(store, now, "All", "Scenario")
    case = next(c for c in m["cases"] if c["incident"] == incident["id"])
    assert (case["match"], case["review"], case["result"], case["result_from"]) == \
        (sc.EXPECTED, sc.APPROVED, sc.WORKED, "measured")          # 15 min improved

    html = scorecard.render(m)
    assert "Agent scorecard" in html and "By scenario" in html and S01.group_name in html
    assert "${" not in html and "`" not in html

    # A reviewer's call shows as theirs.
    action = store.actions(incident_id=incident["id"])[-1]
    store.add_label(incident["id"], action["id"], "WRONG_FIX", "<b>drained</b> on its own", "ops", now)
    html = scorecard.render(scorecard.model(store, now, "All", "Who chose the fix"))
    assert '<span class="mx-chip mx-v-NO_CHANGE">reviewer: didn&#x27;t help</span>' in html
    assert "<b>drained" not in html

    assert "No incidents over the last 24 h." in scorecard.render(scorecard.model(store, now + 3 * 86400, "24 h"))
    assert "Agent scorecard" in gradio_app.show_scorecard("7 d", "Agent version")
    assert scorecard.duration(30) == "<1 min" and scorecard.duration(4000) == "67 min" and \
        scorecard.duration(7200) == "2.0 h" and scorecard.duration(3 * 86400) == "3.0 d"
