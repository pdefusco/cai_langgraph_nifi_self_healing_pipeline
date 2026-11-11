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
  - graded against what is left of the design: an earlier armed run's
    change stays on the deployment, so a step of the design fix, or an
    acceptable fix, the deployment already has (its plan is refused,
    "already") is left out (NFH-124). One with a whole fix already there,
    the design fix or an acceptable one, still alerts though the design
    says that clears it: it is left for a person, and nothing is drafted
    from it, as no proposal could pass and each rejection's reason would be
    wrong;
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
fails them waits for a person, as does one of a section that already has
SELFHEAL_AUTO_REVIEW_EDITS_PER_SECTION of this reviewer's edits applied
(2 by default; merging the patch, or reverting one, frees the section),
so no section keeps growing unread. None is approved while a draft is
being written: a draft takes over the open ones of its section, so a
section's drafts become one before the section changes. A draft whose
section an approval changed first (STALE), or whose drafting failed, is
drafted again, once, as Draft again does, so its reason is not lost;
only one drafted from this reviewer's rejection, so a person's old draft
is left as it is, and never one that went STALE after it was applied:
its section was changed by a person (a new version of the file, a
revert), and drafting it again would undo that.

Every decision goes through the Review Queue's own paths (views.decide,
views.diagnose, views.approve_playbook_edit) as REVIEWER, and is logged.
When nothing waits, no diagnosis is queued or running and no draft is
open, it logs once that it is done.
"""

from dataclasses import replace

import config
from flows import packs
from nifi_healer.actions import catalog, executor
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
# Short on text, a JSON path or a URL, where sizing is no help (NFH-125).
VALUE = ("The right setting on the right component, at the wrong value: check what the readings and the "
         "component's configuration say it should be.")
LEVER =("The right component, the wrong change: this setting is not what limits it. Look again at what the "
         "readings say causes the symptom.")
KIND = "The wrong kind of fix: the evidence points to a different kind of change than this one."
COMPONENT = ("The wrong component: the cause is elsewhere in this group. Trace the symptom back to the stage it "
             "starts at.")
INVALID = "It did not pass the checks: {problem}"

_said = set()          # what was logged once: (incident id, why), ("edit", id), "done"
_tried_edits = set()   # playbook edits this process tried to approve, or drafted again
_there = {}            # (incident id, flow version): the design's fixes its deployment already has


def log(message: str) -> None:
    print(f"[auto-review] {message}", flush=True)


def reset() -> None:
    """Forget what was logged and tried (tests; a restart does the same)."""

    _said.clear()
    _tried_edits.clear()
    _there.clear()


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


def _short_on_text(mine: list, designs: list, scenario) -> bool:
    """Whether a picked design falls short only on text values: a step it names that mine has, at other text."""

    def text(fix) -> bool:
        wrong = {f for s in mine if sc.step_key(s) == sc.step_key(fix) for f in sc.short_of(s, fix, scenario)}

        return bool(wrong) and all(sc.amount(fix.get(f)) is None for f in wrong)

    for design in designs:
        if not _keys(design) <= _keys(mine):
            continue

        unreached = [f for f in design if not any(sc.reaches(s, f, scenario) for s in mine)]

        if unreached and all(map(text, unreached)):
            return True

    return False


def verdict(steps: list, scenario) -> tuple:
    """(APPROVE or REJECT, the reason), as the Scorecard grades the steps against the scenario's design label."""

    graded = sc.match(steps, scenario)

    if graded in (sc.EXPECTED, sc.ACCEPTABLE):
        return APPROVE, f"the {graded} fix"

    mine = catalog.merge_routes([catalog.normalize(s) for s in steps if isinstance(s, dict)])
    mine = mine or [{"action": "NO_ACTION"}]
    designs = [list(scenario.expected)] + [[f] for f in scenario.acceptable]

    if graded == sc.SHORT:
        return REJECT, VALUE if _short_on_text(mine, designs, scenario) else SHORT

    if all(f.get("action") == "NO_ACTION" for f in scenario.expected):
        return REJECT, NOTHING

    if any(_keys(mine) < _keys(design) for design in designs):
        return REJECT, PART

    if any(_components(mine) & _components(design) for design in designs):
        return REJECT, LEVER

    # Against the design fix alone: S20's is a resize, but one acceptable fix is a property change, so another
    # processor's edit read "the wrong component, trace the symptom back", away from sizing (NFH-116).
    if not _families(mine) & _families(scenario.expected):
        return REJECT, KIND

    return REJECT, COMPONENT


def on_deployment(client, store, incident: dict, scenario) -> list:
    """
    The design fix's steps, and the acceptable fixes, the incident's
    deployment already has: each planned alone on it, which the executor
    refuses with "already". Read once an incident and flow version.
    """

    key = (incident["id"], incident.get("expected_flow_version_crn") or "")

    if key not in _there:
        try:
            current = executor.rebased(client, store, incident)
            _there[key] = [f for f in (*scenario.expected, *scenario.acceptable)
                           if any(map(executor.already, executor.plan(client, current, [f], store)["problems"]))]

        except Exception as exc:  # graded against the whole design, as before
            log(f"{incident['id']}: could not read what its deployment already has: {type(exc).__name__}: {exc}")
            _there[key] = []

    return _there[key]


def _shown(step: dict) -> str:
    return " ".join(str(v) for k, v in step.items() if k != "target" and v not in ("", None)) + \
        (f" on {step['target']}" if step.get("target") else "")


def _review(client, store, incident: dict) -> None:
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

    there = on_deployment(client, store, incident, scenario)

    # S20, live: Compression Level 1 stayed from an earlier run and changed nothing, so every proposal was graded
    # against the resize alone and rejected, each reason drafting a playbook edit.
    if any(all(f in there for f in fix) for fix in (scenario.expected, *((f,) for f in scenario.acceptable))):
        _once((iid, "there"), f"{iid} ({scenario.id}): a whole fix of its design is already on the deployment "
                              f"({'; '.join(map(_shown, there))}) and it still alerts: left for a person")

        return

    if there:
        scenario = replace(scenario, expected=tuple(f for f in scenario.expected if f not in there),
                           acceptable=tuple(f for f in scenario.acceptable if f not in there))

    plan = waiting.get("plan") or {}
    steps = plan.get("steps") or (waiting.get("proposal") or {}).get("steps") or []

    if not plan.get("ok"):
        decision, reason = REJECT, INVALID.format(problem=(plan.get("problems") or ["no usable proposal"])[0])

    else:
        decision, reason = verdict(steps, scenario)

    # Only the log says so: the reason is what memory and the drafter read.
    who = f"{scenario.id}, the deployment already has {'; '.join(map(_shown, there))}" if there else scenario.id

    if decision == APPROVE:
        if config.ARMED and not config.AUTO_REVIEW_ARMED:
            _once((iid, "armed"), f"{iid}: {reason}, but writes are armed: left for a person "
                                  "(SELFHEAL_AUTO_REVIEW_ARMED approves them)")

            return

        log(f"{iid} ({who}): approve, {reason}: {views.decide(iid, APPROVE, REVIEWER)}")

    else:
        log(f"{iid} ({who}): reject, {reason} {views.decide(iid, REJECT, REVIEWER, reason)}")


def _ours(store, row: dict) -> bool:
    """Whether the edit was drafted from this reviewer's rejection, not a person's feedback."""

    return any(r["decision"] == REJECT and r["reviewer"] == REVIEWER
               for s in drafter.sources_of(row) for r in store.reviews(s["incident_id"]))


def _approved_on(store, row: dict) -> int:
    """This reviewer's edits applied on the row's section."""

    return sum(1 for e in store.applied_playbook_edits()
               if e.get("reviewer") == REVIEWER and drafter.same_target(e, row))


def _edits(store) -> bool:
    """Approve the proposed playbook edits that pass the checks; True while a draft is being written."""

    rows = store.playbook_edits(("DRAFTING", "PROPOSED", "STALE", "FAILED"))
    drafting = any(r["status"] == "DRAFTING" and drafter.drafting(r["id"]) for r in rows)

    for row in rows:
        if row["status"] in ("STALE", "FAILED") and (row["id"], "again") not in _tried_edits:
            _tried_edits.add((row["id"], "again"))

            # One applied, then STALE, had its section changed under it by a person: a new version of the file,
            # or a revert. Drafting it again would undo that.
            if row["status"] == "STALE" and row.get("reviewer"):
                _once(("edit", row["id"], "applied"), f"PE-{row['id']}: STALE after it was applied (its section "
                                                      "changed in git, or an edit before it was reverted); left as it is")

            elif _ours(store, row):
                log(f"PE-{row['id']}: {row['status']}, drafting it again: {views.redraft_playbook_edit(row['id'])}")
                drafting = True

    # A draft takes over the open drafts of its section, so the others wait for it: one approved while it is
    # written leaves it drafted on the old text, and STALE (live, round 2: PE-13 and PE-10).
    if drafting:
        return True

    for row in rows:
        key = (row["id"], row["status"])

        if row["status"] != "PROPOSED" or key in _tried_edits:
            continue

        if row.get("problems"):
            _tried_edits.add(key)
            log(f"PE-{row['id']}: fails the drafter's checks, left for a person: {'; '.join(row['problems'])}")
            continue

        # Not marked tried: merging the patch or reverting one of the edits frees the section.
        if _approved_on(store, row) >= config.AUTO_REVIEW_EDITS_PER_SECTION:
            _once(("edit", row["id"], "cap"),
                  f"PE-{row['id']}: {drafter.target_id(row)} already has {config.AUTO_REVIEW_EDITS_PER_SECTION} "
                  "of this reviewer's edits applied; left for a person (SELFHEAL_AUTO_REVIEW_EDITS_PER_SECTION)")
            continue

        _tried_edits.add(key)
        log(f"PE-{row['id']}: {views.approve_playbook_edit(row['id'], reviewer=REVIEWER)}")

    return drafting


def tick(client, store) -> None:
    for incident in sorted(store.incidents(("PENDING_REVIEW",), limit=1000), key=lambda i: i["opened_at"]):
        try:
            _review(client, store, incident)

        except Exception as exc:  # one review's failure never stops the others
            log(f"{incident['id']}: failed: {type(exc).__name__}: {exc}")

    drafting = _edits(store)
    left = [i["id"] for i in store.incidents(("PENDING_REVIEW",), limit=1000)
            if not any((i["id"], why) in _said for why in ("unknown", "armed", "there"))]

    if drafting or left or any(graph.backlog().values()):
        _said.discard("done")

    else:
        _once("done", "done: nothing waits for a review, no diagnosis is queued or running, no draft is open")
