"""
Reviewer feedback drafted into playbook edits (rag/drafter.py), laid over
the files (rag/overlay.py), decided on the Playbooks tab (ui/views.py,
ui/playbooks.py), and read by the agent and the replay.
"""

import shutil
import sqlite3
import subprocess

import pytest

import config
from flows import packs
from nifi_healer import llm
from nifi_healer.actions import rollback
from nifi_healer.agent import graph
from nifi_healer.eval import replay
from nifi_healer.rag import drafter, ingest, overlay, retriever
from nifi_healer.ui import gradio_app, legend, playbooks, scorecard, views

from test_agent_graph import S01_FIX, ScriptedLLM, agent, proposal  # noqa: F401 (agent is a fixture)
from test_replay import replayed  # noqa: F401 (a fixture)

SECTION = "backlog-slow-consumer#raise-concurrent-tasks-on-the-bottleneck-processor"
PLAYBOOK = "backlog-slow-consumer.md"
S02_FIX = packs.SCENARIOS["S02"].expected

RAW = """---
title: Sample
collection: nifi_playbooks
---

# Sample

Intro line.

## First section
<!-- actions: NO_ACTION -->

Old advice here.

## Second section

Second body.
"""

FIRST = overlay.clean("<!-- actions: NO_ACTION -->\n\nOld advice here.")


def edit(edit_id, new, old=FIRST, section="sample#first-section", heading="First section"):
    return {"id": edit_id, "playbook": "sample.md", "section_id": section, "heading": heading,
            "old_text": old, "new_text": new}


# ---------------------------------------------------------------------
# The overlay
# ---------------------------------------------------------------------


def test_an_edit_replaces_one_section():
    new = "<!-- actions: NO_ACTION -->\n\nNew advice here."
    edited, states = overlay.apply(RAW, [edit(1, new)])

    assert states == {1: overlay.APPLIED}
    assert "New advice here." in edited and "Old advice here." not in edited
    assert edited.startswith("---\ntitle: Sample") and "Intro line." in edited and "Second body." in edited
    chunk = next(c for c in ingest.parse_text("sample.md", edited) if c["id"] == "sample#first-section")
    assert "New advice here." in chunk["text"]

    # A later edit of the same section reads the first one's text.
    chained, states = overlay.apply(RAW, [edit(2, new + "\nAnd more.", old=new), edit(1, new)])
    assert states == {1: overlay.APPLIED, 2: overlay.APPLIED} and "New advice here.\nAnd more." in chained


def test_a_new_section_is_appended_once():
    added, states = overlay.apply(RAW, [edit(3, "Brand new guidance.", old="", section="", heading="Third section")])

    assert states == {3: overlay.APPLIED} and added.rstrip().endswith("## Third section\nBrand new guidance.")

    # A heading the playbook already has is not a new section.
    _, states = overlay.apply(RAW, [edit(4, "Again.", old="", section="", heading="First section")])
    assert states == {4: overlay.STALE}


def test_a_changed_section_is_stale_and_a_committed_one_merged():
    stale, states = overlay.apply(RAW, [edit(1, "Mine.", old="What it said before git changed it.")])
    assert states == {1: overlay.STALE} and stale == RAW

    merged, states = overlay.apply(RAW, [edit(1, FIRST, old="Whatever it said before.")])
    assert states == {1: overlay.MERGED} and merged == RAW


def test_the_edition_names_the_applied_edits():
    one, two = edit(1, "New."), edit(2, "Other.", section="sample#second-section", old="Second body.")

    assert overlay.edition([one], {1: overlay.APPLIED})
    assert overlay.edition([one], {1: overlay.STALE}) == "" and overlay.edition([], {}) == ""
    assert overlay.edition([one], {1: overlay.APPLIED}) != overlay.edition([dict(one, new_text="Newer.")],
                                                                           {1: overlay.APPLIED})
    assert overlay.edition([one, two], {1: overlay.APPLIED, 2: overlay.APPLIED}) != \
        overlay.edition([one], {1: overlay.APPLIED})


def test_the_patch_applies_to_the_files(tmp_path):
    (tmp_path / "playbooks").mkdir()
    (tmp_path / "playbooks" / "sample.md").write_text(RAW)
    patch = overlay.patch(str(tmp_path / "playbooks"), [edit(1, "<!-- actions: NO_ACTION -->\n\nNew advice.")])

    assert "--- a/playbooks/sample.md" in patch and "+++ b/playbooks/sample.md" in patch
    assert [line for line in patch.splitlines() if line[:1] in "+-" and line[:3] not in ("---", "+++")] == \
        ["-Old advice here.", "+New advice."]
    assert overlay.patch(str(tmp_path / "playbooks"), []) == ""

    if shutil.which("git"):
        (tmp_path / "playbooks.patch").write_text(patch)
        done = subprocess.run(["git", "apply", "--check", "playbooks.patch"], cwd=tmp_path, capture_output=True,
                              text=True)
        assert done.returncode == 0, done.stderr


# ---------------------------------------------------------------------
# What the agent must not read
# ---------------------------------------------------------------------


def test_check_refuses_what_the_agent_must_not_read():
    raws = ingest.raw_playbooks(edits=[])
    found = drafter.sections(raws)
    base = found[SECTION]["text"]
    ok = base + "\n\nFirst check the deployment's CPU: extra threads on saturated cores slow the flow down."

    def problems(text, **fields):
        return " | ".join(drafter.check(dict({"playbook": PLAYBOOK, "section_id": SECTION, "text": text}, **fields),
                                        raws, found))

    assert problems(ok) == ""
    assert problems(ok, section_id="", heading="Check the CPU before adding threads") == ""

    assert "platform rules" in problems(ok, playbook="cdf-platform-rules.md")
    assert "no playbook 'nope.md'" in problems(ok, playbook="nope.md")
    assert "has no section" in problems(ok, section_id="backlog-slow-consumer#nope")
    assert "catalog does not have" in problems(ok + "\n<!-- actions: MAKE_IT_FAST -->")
    assert "STOP_AND_EMPTY_QUEUES".lower() in problems(ok + " Then change the version with STOP_AND_EMPTY_QUEUES.")
    assert "restart the deployment" in problems(ok + " If that fails, restart the deployment.")
    assert "heading line" in problems(ok + "\n\n## Another heading")
    assert "front matter" in problems("---\n" + ok)
    assert "CRN" in problems(ok + " As on crn:cdp:df:us-west-1:abc:deployment:x/y.")
    assert "names an incident" in problems(ok + " As in INC-20261015-005651-S01-a3f0.")
    assert "characters" in problems("Raise it.")
    assert "nothing would change" in problems(base)
    assert "already has a section" in problems(ok, section_id="", heading="Verifying the fix")
    assert "already has a section" in problems(ok, section_id="", heading="Backlog behind a slow consumer")
    assert "needs a heading" in problems(ok, section_id="", heading="  ")


RUNBOOK = "parameter-misconfiguration#runbook-correct-values-for-the-nfh-p5-parameters-context"

# The live PE-1: a reviewer's "policy" turned into a rewrite that dropped the runbook's values.
GENERIC = """<!-- symptoms: -->
A parameter value deviates from its intended setting, causing abnormal flow behavior.
<!-- actions: -->
1. Identify the correct parameter value from design documentation or policy.
2. If the correct value violates a policy, do not set it. Instead raise the downstream consumer's concurrent tasks
   using SET_CONCURRENT_TASKS, or adjust the parameter to the highest allowed value using UPDATE_PARAMETER."""


def test_check_refuses_a_rewrite_that_loses_the_section():
    raws = ingest.raw_playbooks()
    found = drafter.sections(raws)
    old = found[RUNBOOK]["text"]
    problems = " ".join(drafter.check({"playbook": "parameter-misconfiguration.md", "section_id": RUNBOOK,
                                       "text": GENERIC}, raws, found))
    assert "drops UPDATE_PARAMETER from the section's actions line" in problems
    assert "keeps 0 of the section's 3 table rows" in problems and "the symptoms line is empty" in problems
    assert "% of the section's words; change only what" in problems
    head, tail = old.rsplit("\n\n", 1)
    assert drafter._dropped(old, head + "\n\n" + " ".join(tail.split())) == []      # a paragraph rewrapped

    # A value changed in its row, and a line added, is still a small edit.
    changed = old.replace("| `1` |", "| `2` |") + "\n\nCheck the downstream queue after the change."
    assert drafter.check({"playbook": "parameter-misconfiguration.md", "section_id": RUNBOOK, "text": changed},
                         raws, found) == []

    # Every shipped section passes as it reads, so no check refuses a playbook's own text.
    for chunk_id, section in found.items():
        assert drafter._dropped(section["text"], section["text"]) == [], chunk_id
        assert all(drafter._tags(section["text"]).values()), chunk_id


def test_check_keeps_an_existing_never_line():
    raws = {"x.md": "---\ntitle: X\n---\n# X\n\n## Never\n"
                    "Never restart the deployment to drain a queue: it drains nothing.\n"}
    line = "Never restart the deployment to drain a queue: it drains nothing, and the source keeps producing."

    assert drafter.check({"playbook": "x.md", "section_id": "x#never", "text": line}, raws) == []
    assert "restart the deployment" in " ".join(drafter.check(
        {"playbook": "x.md", "section_id": "x#never", "text": line + " Or restart the deployment twice."}, raws))


# ---------------------------------------------------------------------
# Drafting, deciding, reading
# ---------------------------------------------------------------------


@pytest.fixture
def drafting(agent, monkeypatch):  # noqa: F811 (agent is a fixture)
    monkeypatch.setattr(config, "DRAFT_PLAYBOOK_EDITS", True)
    monkeypatch.setattr(retriever, "rebuild", retriever.invalidate)  # builds on the next search, not in a thread
    drafter.reset()

    yield agent

    settle()
    drafter.reset()


def settle() -> None:
    """Wait for the drafts queued so far."""

    for future in list(drafter._waiting.values()):
        future.result(timeout=30)


def edit_reply(addition: str, section: str = SECTION):
    """The drafter's reply: the section as it reads now, plus a paragraph."""

    def reply(messages):
        base = drafter.sections(ingest.raw_playbooks())[section]["text"]

        return {"playbook": PLAYBOOK, "section_id": section, "text": base + "\n\n" + addition,
                "rationale": "the reviewer saw the CPU was saturated"}

    return reply


def indexed(chunk_id: str) -> str:
    retriever.ensure()

    return ingest.collection(ingest.PLAYBOOKS).get(ids=[chunk_id])["documents"][0]


class Request:
    def __init__(self, **headers):
        self.headers = headers


VIEWER = Request(**{"remote-user": "guest", "remote-user-perm": "RO"})
OWNER = Request(**{"remote-user": "pdefusco", "remote-user-perm": "RW"})

CPU = "Check the deployment's CPU first: on saturated cores, extra threads slow the flow down."
SECOND = "Raise the run schedule instead when the consumer waits between runs."
REASON = "CPU is pegged <at 100%>; more threads will only contend"


def test_a_rejection_becomes_an_edit_the_agent_reads(drafting):
    _, _, store, incidents, install = drafting("p1-throughput")
    s01, s02 = incidents["S01"]["id"], incidents["S02"]["id"]
    model = install(ScriptedLLM(proposal(S01_FIX, cite=[SECTION]), edit_reply(CPU),
                                proposal(S02_FIX, diagnosis="S02 Drain waits between runs", cite=[SECTION]),
                                edit_reply(SECOND)))

    graph.start(s01)
    assert "/playbooks:" not in store.proposals(s01)[-1]["agent_version"]
    graph.resume(s01, {"decision": "reject", "reason": REASON, "reviewer": "ops"})
    settle()

    [row] = store.playbook_edits()
    assert row["status"] == "PROPOSED" and row["trigger_kind"] == drafter.REJECT and row["incident_id"] == s01
    assert row["section_id"] == SECTION and row["heading"] == "Raise Concurrent Tasks on the bottleneck processor"
    assert row["new_text"].endswith(CPU) and row["problems"] == [] and row["drafted_by"].startswith("draft:")
    prompt = model.calls[1][1].content
    assert REASON in prompt and "needed tasks = intake / rate per task" in prompt and f"- {SECTION}:" in prompt
    assert "cdf-platform-rules#" not in prompt.split("Every section you may edit", 1)[1]

    # The tab: the edit, why it was drafted (escaped), its diff.
    assert playbooks.choices(store) == [(playbooks.label(row), str(row["id"]))]
    assert f"from a rejection of {s01}" in playbooks.label(row) and playbooks.label(row).endswith("· proposed")
    panel = playbooks.panel(store, row["id"])
    assert panel["can_approve"] and panel["can_edit"] and panel["can_reject"] and not panel["can_revert"]
    assert "&lt;at 100%&gt;" in panel["html"] and "<at 100%>" not in panel["html"]
    assert '<div class="pb-ln pb-add"><i>+</i>Check the deployment' in panel["html"]
    assert "Rejected before it ran" in panel["html"] and "by ops" in panel["html"]

    # A read-only viewer can't approve, and a decision for another edit than the panel showed is not sent.
    approve = gradio_app._playbook_decision("approve")
    out = approve("To review", str(row["id"]), "", "", "", panel["key"], request=VIEWER)
    assert "can only view" in out[0] and len(out) == 10 and store.playbook_edit(row["id"])["status"] == "PROPOSED"
    out = approve("To review", str(row["id"]), "", "", "", "999|PROPOSED||0", request=OWNER)
    assert "another edit" in out[0] and store.playbook_edit(row["id"])["status"] == "PROPOSED"
    assert all(isinstance(o, dict) and o.get("__type__") == "update" and len(o) == 1
               for o in gradio_app.follow_playbook_edit(str(row["id"]), panel["key"]))
    assert gradio_app.select_playbook_edit(str(row["id"]))[-1] == panel["key"]

    # Approved: the agent searches the new text, and its version names the edits.
    out = approve("To review", str(row["id"]), "", "", "", panel["key"], request=OWNER)
    assert out[0].startswith(f"Approved PE-{row['id']}") and CPU in out[-1]
    row = store.playbook_edit(row["id"])
    assert row["status"] == "APPLIED" and row["reviewer"] == "pdefusco" and row["decided_at"]
    assert CPU in indexed(SECTION) and retriever.edition()
    assert playbooks.choices(store) == [] and playbooks.choices(store, keep=str(row["id"])) == \
        [(playbooks.label(row), str(row["id"]))]
    assert [v for _, v in playbooks.choices(store, "Applied")] == [str(row["id"])]
    assert "+" + CPU in views.playbook_patch() and f"a/playbooks/{PLAYBOOK}" in views.playbook_patch()
    assert "Approved" not in views.approve_playbook_edit(row["id"])

    graph.start(s02)
    first = retriever.edition()
    assert store.proposals(s02)[-1]["agent_version"].endswith(f"/playbooks:{first}")

    # A second rejection drafts onto the applied text; approving it chains the two.
    graph.resume(s02, {"decision": "reject", "reason": "it waits between runs", "reviewer": "ops"})
    settle()
    second = store.playbook_edits(("PROPOSED",))[0]
    assert CPU in second["old_text"] and second["new_text"].endswith(SECOND)
    assert views.approve_playbook_edit(second["id"], reviewer="ops").startswith("Approved")
    assert CPU in indexed(SECTION) and SECOND in indexed(SECTION) and retriever.edition() not in ("", first)

    # Reverted, newest first, with a reason: the files alone again.
    assert "reason is required" in views.revert_playbook_edit(row["id"], " ")
    assert f"revert PE-{second['id']} first" in views.revert_playbook_edit(row["id"], "too narrow")
    assert views.revert_playbook_edit(second["id"], "too narrow", "ops").startswith("Reverted")
    assert views.revert_playbook_edit(row["id"], "too narrow", "ops").startswith("Reverted")
    assert {e["status"] for e in store.playbook_edits()} == {"REVERTED"}
    assert CPU not in indexed(SECTION) and retriever.edition() == "" and views.playbook_patch() == ""


def test_a_wrong_fix_label_drafts_once(drafting):
    _, _, store, incidents, install = drafting("p1-throughput")
    incident_id = incidents["S01"]["id"]
    model = install(ScriptedLLM(proposal(S01_FIX, cite=[SECTION]),
                                {"skip": True, "rationale": "the backlog drained on its own; the advice holds"}))
    graph.start(incident_id)
    graph.resume(incident_id, {"decision": "approve", "reviewer": "ops"})
    action = store.actions(incident_id=incident_id)[-1]

    message = views.label_fix(incident_id, action["id"], "WRONG_FIX", "the backlog drained on its own", "ops")
    assert views.DRAFTING_NOTE in message
    settle()

    [row] = store.playbook_edits()
    assert row["status"] == "SKIPPED" and row["trigger_kind"] == drafter.WRONG_FIX
    assert "advice holds" in row["rationale"] and "labelled it a wrong fix" in model.calls[-1][1].content
    assert playbooks.choices(store) == [] and len(playbooks.choices(store, "All")) == 1
    assert "Applied, then labelled a wrong fix" in playbooks.panel(store, row["id"])["html"]

    label_id = store.labels(incident_id)[action["id"]]["id"]
    assert drafter.queue(drafter.WRONG_FIX, incident_id, label_id) is None
    assert views.DRAFTING_NOTE not in views.label_fix(incident_id, action["id"], "RIGHT_FIX", "it was", "ops")
    assert len(store.playbook_edits()) == 1


def test_a_second_draft_of_a_section_takes_over_the_first(drafting):
    _, _, store, incidents, install = drafting("p1-throughput")
    s01, s02 = incidents["S01"]["id"], incidents["S02"]["id"]
    model = install(ScriptedLLM(proposal(S01_FIX, cite=[SECTION]), edit_reply(CPU),
                                proposal(S02_FIX, diagnosis="S02 Drain waits between runs", cite=[SECTION]),
                                edit_reply(CPU + " " + SECOND)))

    graph.start(s01)
    graph.resume(s01, {"decision": "reject", "reason": REASON, "reviewer": "ops"})
    settle()
    graph.start(s02)
    graph.resume(s02, {"decision": "reject", "reason": "it waits between runs", "reviewer": "ops"})
    settle()

    first, second = sorted(store.playbook_edits(), key=lambda r: r["id"])
    assert first["status"] == "SUPERSEDED" and first["reason"] == f"drafted again as PE-{second['id']}"
    assert second["status"] == "PROPOSED" and {s["incident_id"] for s in second["sources"]} == {s01, s02}
    assert "Open drafts" in model.calls[3][1].content and REASON in model.calls[3][1].content
    assert playbooks.label(second).count("and 1 more") == 1
    assert [v for _, v in playbooks.choices(store)] == [str(second["id"])]
    html = playbooks.panel(store, second["id"])["html"]
    assert s01 in html and s02 in html

    assert "reason is required" in views.reject_playbook_edit(second["id"], "")
    assert views.reject_playbook_edit(second["id"], "too specific", "ops").startswith("Rejected")
    assert store.playbook_edit(second["id"])["status"] == "REJECTED"
    assert "only an open draft" in views.reject_playbook_edit(second["id"], "again")


def test_a_draft_without_the_llm_fails_and_is_drafted_again(drafting, monkeypatch):
    _, _, store, incidents, install = drafting("p1-throughput")
    incident_id = incidents["S01"]["id"]
    install(ScriptedLLM(proposal(S01_FIX, cite=[SECTION]), edit_reply(CPU)))
    graph.start(incident_id)
    available = llm.chat_available
    monkeypatch.setattr(llm, "chat_available", lambda: (False, "no endpoint configured"))
    graph.resume(incident_id, {"decision": "reject", "reason": REASON, "reviewer": "ops"})
    settle()

    [row] = store.playbook_edits()
    assert row["status"] == "FAILED" and "no endpoint configured" in row["error"]
    panel = playbooks.panel(store, row["id"])
    assert panel["can_redraft"] and panel["can_reject"] and not panel["can_approve"]

    monkeypatch.setattr(llm, "chat_available", available)
    assert views.redraft_playbook_edit(row["id"]).startswith("Drafting")
    settle()

    row = store.playbook_edit(row["id"])
    assert row["status"] == "PROPOSED" and row["error"] == "" and row["new_text"].endswith(CPU)
    assert "only a failed or stale draft" in views.redraft_playbook_edit(row["id"])


def test_a_stale_draft_is_not_approved(drafting):
    _, _, store, incidents, install = drafting("p1-throughput")
    incident_id = incidents["S01"]["id"]
    install(ScriptedLLM(proposal(S01_FIX, cite=[SECTION]), edit_reply(CPU)))
    graph.start(incident_id)
    graph.resume(incident_id, {"decision": "reject", "reason": REASON, "reviewer": "ops"})
    settle()

    [row] = store.playbook_edits()
    store.update_playbook_edit(row["id"], old_text="The section as it read before git changed it.")
    assert "no longer fits" in views.approve_playbook_edit(row["id"])
    assert store.playbook_edit(row["id"])["status"] == "STALE" and playbooks.panel(store, row["id"])["can_redraft"]

    # A reviewer's own text is checked like the drafter's.
    store.update_playbook_edit(row["id"], status="PROPOSED", old_text=drafter.sections(
        ingest.raw_playbooks())[SECTION]["text"])
    assert "Not approved" in views.approve_playbook_edit(row["id"], text="Too short.")
    assert store.playbook_edit(row["id"])["status"] == "PROPOSED"


def test_a_section_id_without_its_playbook_is_resolved(drafting):
    _, _, store, incidents, install = drafting("p1-throughput")
    incident_id = incidents["S01"]["id"]
    bare = SECTION.split("#", 1)[1]                 # the live nemotron reply: the slug alone
    reply = edit_reply(CPU)

    def wrong(messages):
        return dict(reply(messages), section_id="no-such-section")

    install(ScriptedLLM(proposal(S01_FIX, cite=[SECTION]), lambda m: dict(reply(m), section_id=bare),
                        proposal(S02_FIX, cite=[SECTION]), wrong, wrong, reply))
    graph.start(incident_id)
    graph.resume(incident_id, {"decision": "reject", "reason": REASON, "reviewer": "ops"})
    settle()

    [row] = store.playbook_edits()
    assert (row["status"], row["section_id"], row["problems"]) == ("PROPOSED", SECTION, [])
    assert row["old_text"] == drafter.sections(ingest.raw_playbooks())[SECTION]["text"]
    found = drafter.sections(ingest.raw_playbooks())
    assert drafter.resolve(bare, "", "", found) == SECTION
    assert drafter.resolve("x", PLAYBOOK, "Raise Concurrent Tasks on the bottleneck processor", found) == SECTION

    # An id that is no section at all: the check names the real ones, twice (the retry), and
    # Approve refuses it without calling it stale; Draft again stays open.
    s02 = incidents["S02"]["id"]
    graph.start(s02)
    graph.resume(s02, {"decision": "reject", "reason": "it waits between runs", "reviewer": "ops"})
    settle()

    row = store.playbook_edits(("PROPOSED",))[0]          # newest first
    assert row["section_id"] == "no-such-section" and SECTION in row["problems"][0]
    assert "Not approved" in views.approve_playbook_edit(row["id"], reviewer="ops")
    assert store.playbook_edit(row["id"])["status"] == "PROPOSED"
    assert playbooks.panel(store, row["id"])["can_redraft"] and "Draft again" in playbooks.panel(store, row["id"])["html"]

    store.update_playbook_edit(row["id"], decided_at=123.0, reviewer="ops")
    assert views.redraft_playbook_edit(row["id"]).startswith("Drafting")
    settle()
    row = store.playbook_edit(row["id"])
    assert (row["status"], row["section_id"], row["problems"], row["decided_at"]) == ("PROPOSED", SECTION, [], None)
    assert not playbooks.panel(store, row["id"])["can_redraft"]


def test_what_is_not_drafted(drafting, monkeypatch):
    _, _, store, incidents, _ = drafting("p1-throughput")
    incident_id = incidents["S01"]["id"]

    assert drafter.queue("praise", incident_id, 1) is None and drafter.queue(drafter.REJECT, incident_id, 0) is None
    monkeypatch.setattr(rollback, "is_rollback", lambda incident: True)
    assert drafter.queue(drafter.REJECT, incident_id, 1) is None
    monkeypatch.setattr(config, "DRAFT_PLAYBOOK_EDITS", False)
    monkeypatch.setattr(rollback, "is_rollback", lambda incident: False)
    assert drafter.queue(drafter.REJECT, incident_id, 1) is None
    assert store.playbook_edits() == []
    assert "No playbook edits here yet" in playbooks.panel(store, None)["html"]


# ---------------------------------------------------------------------
# The replay, the scorecard, the legend
# ---------------------------------------------------------------------


def test_a_replay_reads_the_approved_edits(tenant, replayed, tmp_path):  # noqa: F811 (replayed is a fixture)
    _, _, store = tenant("p1-throughput")
    live = config.DB_PATH
    base = drafter.sections(ingest.raw_playbooks(edits=[]))[SECTION]["text"]
    edit_id = store.add_playbook_edit(drafter.REJECT, "INC-1", 1, 0.0, playbook=PLAYBOOK, section_id=SECTION,
                                      heading="Raise Concurrent Tasks on the bottleneck processor",
                                      old_text=base, new_text=base + "\n\n" + CPU)
    store.update_playbook_edit(edit_id, status="APPLIED")

    assert [e["id"] for e in replay.live_edits(live)] == [edit_id]
    assert replay.live_edits(str(tmp_path / "missing.db")) == [] and replay.live_edits("") == []

    replay.run(["S01"], oracle=True)
    assert "/playbooks:" in replay.load()["runs"][0]["agent_version"] and ingest._override["edits"] is None

    replay.run(["S01"], oracle=True, playbook_edits=False)
    assert "/playbooks:" not in replay.load()["runs"][1]["agent_version"]

    with sqlite3.connect(live) as connection:  # the live store was only read
        assert connection.execute("SELECT status FROM playbook_edits").fetchall() == [("APPLIED",)]


def test_the_scorecard_names_an_agent_with_edited_playbooks():
    assert scorecard.agent_name("prompt:a/model:nvidia/x/no-memory/playbooks:abc123") == \
        ("instructions a", "x · memory off · playbooks edited abc123")
    assert scorecard.agent_name("prompt:a/model:nvidia/x") == ("instructions a", "x · memory on")


def test_the_playbooks_legend():
    assert legend.PLAYBOOKS.strip() and "{" not in legend.PLAYBOOKS
    for term in ("Why it was drafted", "cdf-platform-rules.md", "Approve", "Revert", "STALE", "MERGED",
                 "Make the applied edits permanent", "playbooks edited"):
        assert term in legend.PLAYBOOKS, term
    assert "Playbooks edited" in legend.SCORECARD
