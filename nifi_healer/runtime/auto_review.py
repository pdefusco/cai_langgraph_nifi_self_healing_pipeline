"""
The answer-key reviewer (NFH-116): for testing on the scenario packs, it
decides every review waiting for a person the way the Scorecard grades
it, so the packs can run end to end without anyone at the Review Queue.

    tick(client, store)     # the poller's tick (ui/gradio_app.py), with SELFHEAL_AUTO_REVIEW on

Each review waiting for a person, oldest first:
  - stale (diagnosed on a flow version no longer deployed): Diagnose, as
    the button does;
  - on a scenario with no design label (a pack this build doesn't know):
    left for a person;
  - not valid after every try: rejected, with its first problem;
  - the scenario's design fix, or an acceptable one, at a value that can
    clear the problem (scorecard.match EXPECTED or ACCEPTABLE): approved,
    a dry run while writes are disarmed. While they are armed it approves
    only with SELFHEAL_AUTO_REVIEW_ARMED on as well;
  - anything else: rejected with a general reason (nothing needs a change,
    part of the fix, too little of it, the right component with the wrong
    change, the wrong kind of fix, the wrong component). Never the design
    fix: the reason is what the agent's memory and the playbook drafter
    read.

A playbook edit drafted from a rejection that passes the drafter's checks
is approved, so the agent reads it from its next diagnosis; one that
fails them waits for a person.

Every decision goes through the Review Queue's own paths (views.decide,
views.diagnose, views.approve_playbook_edit) as REVIEWER, and is logged.
When nothing waits, no diagnosis is queued or running and no draft is
open, it logs once that it is done.
"""

import config
from flows import packs
from nifi_healer.actions import catalog
from nifi_healer.agent import graph
from nifi_healer.eval import scorecard as sc
from nifi_healer.rag import drafter
from nifi_healer.ui import views

REVIEWER = "answer key (auto-review)"

APPROVE, REJECT = "approve", "reject"

NOTHING = "Nothing in this group needs a change: its readings are within what it normally does."
PART = "Part of the fix, not all of it: what holds this group back takes more than this change."
SHORT = ("The right change on the right component, but not far enough to clear the problem: size it from the "
         "measured intake and backlog.")
LEVER = ("The right component, the wrong change: this setting is not what limits it. Look again at what the "
         "readings say causes the symptom.")
KIND = "The wrong kind of fix: the evidence points to a different kind of change than this one."
COMPONENT = ("The wrong component: the cause is elsewhere in this group. Trace the symptom back to the stage it "
             "starts at.")
INVALID = "It did not pass the checks: {problem}"

_said = set()          # what was logged once: (incident id, why), ("edit", id), "done"
_tried_edits = set()   # playbook edits this process tried to approve


def log(message: str) -> None:
    print(f"[auto-review] {message}", flush=True)


def reset() -> None:
    """Forget what was logged and tried (tests; a restart does the same)."""

    _said.clear()
    _tried_edits.clear()


def _once(key, message: str) -> None:
    if key not in _said:
        _said.add(key)
        log(message)


def _keys(steps: list) -> set:
    return {sc.step_key(s) for s in steps}


def _components(steps: list) -> set:
    return {s.get("parameter") or sc._name(s.get("target")) for s in steps} - {"", None}


def _families(steps: list) -> set:
    return {catalog.CATALOG[s["action"]].family for s in steps if s.get("action") in catalog.CATALOG}


def verdict(steps: list, scenario) -> tuple:
    """(APPROVE or REJECT, the reason), as the Scorecard grades the steps against the scenario's design label."""

    graded = sc.match(steps, scenario)

    if graded in (sc.EXPECTED, sc.ACCEPTABLE):
        return APPROVE, f"the {graded} fix"

    if graded == sc.SHORT:
        return REJECT, SHORT

    mine = catalog.merge_routes([catalog.normalize(s) for s in steps if isinstance(s, dict)])
    mine = mine or [{"action": "NO_ACTION"}]
    designs = [list(scenario.expected)] + [[f] for f in scenario.acceptable]

    if all(f.get("action") == "NO_ACTION" for f in scenario.expected):
        return REJECT, NOTHING

    if any(_keys(mine) < _keys(design) for design in designs):
        return REJECT, PART

    if any(_components(mine) & _components(design) for design in designs):
        return REJECT, LEVER

    if not any(_families(mine) & _families(design) for design in designs):
        return REJECT, KIND

    return REJECT, COMPONENT


def _review(store, incident: dict) -> None:
    iid = incident["id"]

    if graph.run_state(iid):
        return

    waiting = graph.pending(iid)

    if not waiting:
        return

    if views.stale(store, incident):
        log(f"{iid}: stale, diagnosing again: {views.diagnose(iid, REVIEWER)}")

        return

    scenario = packs.SCENARIOS.get(incident.get("scenario") or "")

    if scenario is None or not scenario.expected:
        _once((iid, "unknown"), f"{iid}: no design label for {incident.get('scenario') or 'its group'}; "
                                "left for a person")

        return

    plan = waiting.get("plan") or {}
    steps = plan.get("steps") or (waiting.get("proposal") or {}).get("steps") or []

    if not plan.get("ok"):
        decision, reason = REJECT, INVALID.format(problem=(plan.get("problems") or ["no usable proposal"])[0])

    else:
        decision, reason = verdict(steps, scenario)

    if decision == APPROVE:
        if config.ARMED and not config.AUTO_REVIEW_ARMED:
            _once((iid, "armed"), f"{iid}: {reason}, but writes are armed: left for a person "
                                  "(SELFHEAL_AUTO_REVIEW_ARMED approves them)")

            return

        log(f"{iid} ({scenario.id}): approve, {reason}: {views.decide(iid, APPROVE, REVIEWER)}")

    else:
        log(f"{iid} ({scenario.id}): reject, {reason} {views.decide(iid, REJECT, REVIEWER, reason)}")


def _edits(store) -> bool:
    """Approve the proposed playbook edits that pass the checks; True while a draft is being written."""

    drafting = False

    for row in store.playbook_edits(("DRAFTING", "PROPOSED")):
        if row["status"] == "DRAFTING":
            drafting = drafting or drafter.drafting(row["id"])
            continue

        if row["id"] in _tried_edits:
            continue

        _tried_edits.add(row["id"])

        if row.get("problems"):
            log(f"PE-{row['id']}: fails the drafter's checks, left for a person: {'; '.join(row['problems'])}")
            continue

        log(f"PE-{row['id']}: {views.approve_playbook_edit(row['id'], reviewer=REVIEWER)}")

    return drafting


def tick(client, store) -> None:
    for incident in sorted(store.incidents(("PENDING_REVIEW",), limit=1000), key=lambda i: i["opened_at"]):
        try:
            _review(store, incident)

        except Exception as exc:  # one review's failure never stops the others
            log(f"{incident['id']}: failed: {type(exc).__name__}: {exc}")

    drafting = _edits(store)
    left = [i["id"] for i in store.incidents(("PENDING_REVIEW",), limit=1000)
            if (i["id"], "unknown") not in _said and (i["id"], "armed") not in _said]

    if drafting or left or any(graph.backlog().values()):
        _said.discard("done")

    else:
        _once("done", "done: nothing waits for a review, no diagnosis is queued or running, no draft is open")
