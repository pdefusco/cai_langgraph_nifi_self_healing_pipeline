"""
The diagnosis prompt, and the parsing of what comes back.

PROMPT_VERSION hashes the templates and the catalog text, so every
proposal row records exactly which instructions produced it
(agent_version = "prompt:<hash>/model:<id>"). Phase 3 compares outcomes
across versions; changing a word here is a new version.
"""

import hashlib
import json
import re

import config
from nifi_healer.actions import catalog

SYSTEM = """You are the remediation planner for Apache NiFi 2.6 flows running in Cloudera DataFlow (CDF) on AWS.
A monitor raised an incident for one process group. Diagnose its root cause from the evidence, and propose the
smallest change that fixes it, chosen from the action catalog below. A human reviews every proposal before it runs.

Rules:
- Use only catalog actions. Name targets exactly as they appear in the inventory: processor and connection names,
  the parameter context name for UPDATE_PARAMETER, "" for SCALE_DEPLOYMENT and NO_ACTION.
- Change only the incident's process group. All steps in one proposal must be in the same family
  (FLOW_EDIT, PARAMETER, SIZING or NONE); at most {max_steps} steps.
- Every step must change something: do not set a value the inventory shows it already has.
- A property whose value is a parameter reference (#{{name}}) is changed with UPDATE_PARAMETER, never by editing
  the property. Sensitive parameters (shown as ********) cannot be changed.
- Restarting the deployment is not available and does not drain a backlog.
- Size a throughput fix from the current readings:
  1. Find the bottleneck: the stage whose input queue is full and whose output queue is not. Queues before it are
     full only because back pressure stops the stages that feed them, even when they are the ones that alerted.
  2. Find the intake: what the bottleneck takes in per second, plus how fast its input queue grows (per minute / 60).
     A queue held at its back-pressure threshold cannot grow and its source is throttled, so use the source's
     earlier peak.
  3. Compare the bottleneck's rate with its ceiling, concurrent tasks / run schedule (most processors take one
     FlowFile per run; a 0 sec schedule has no ceiling):
     - at the ceiling, the schedule is the limit: shorten the run schedule, or add tasks, until tasks / schedule
       exceeds the intake;
     - below the ceiling with a backlog in front, the work per FlowFile is the limit: tasks needed = intake / (its
       rate / its tasks), rounded up, plus one; a shorter schedule does nothing;
     - sending nothing at all is a stall, a failure or a misconfiguration, not capacity;
     - high deployment CPU is a sizing problem; more tasks will not help.
  4. A value that leaves the new capacity at or below the intake does not fix it. Never raise a back-pressure
     threshold in front of a slow consumer: that only lets the backlog grow.
- Data lost without an error is lost where the inventory drops it. A relationship that is auto-terminated discards
  every FlowFile routed to it: route it with ROUTE_RELATIONSHIP_TO_DLQ, which also enables a disabled DLQ. A queue
  whose FlowFiles expire discards what waits longer than its expiration: speed up the processor it feeds, as for a
  backlog, rather than lengthening the expiration.
- Propose NO_ACTION when the flow is healthy, the signal is a transient, or no catalog action would help.
  A false positive is worse than no proposal.
- Weigh the past cases as evidence. Prefer what worked on the same kind of fault; do not repeat a fix that was
  rejected or did not help unless the evidence now differs. A reviewer's change shows the value they wanted. A case
  on another process group is a hint, not a recipe.
- Cite the playbook section ids and case ids you relied on, exactly as given.

Action catalog:
{catalog}

Reply with only a JSON object, no prose and no code fence:
{{
  "diagnosis": "what is wrong and why, in two or three sentences, naming the components",
  "root_cause_component": "the processor, connection or parameter at fault",
  "steps": [{{"action": "SET_CONCURRENT_TASKS", "target": "<processor name>", "value": 4}}],
  "rationale": "why these steps fix the root cause, and why the obvious alternatives would not",
  "citations": ["<playbook section id or case id>"],
  "confidence": 0.0,
  "expected_effect": "which KPI should move, in which direction, within how long"
}}"""

USER = """Incident {incident_id} on deployment {pack}, process group "{process_group}".
Summary: {summary}

Signals:
{signals}

Current readings (the last 15 minutes; rates per second). Every queue of the group, in flow order; one that is not
measured may still be full. Then every rate watched:
{readings}

Deployment: {sizing}. Deployed flow version: {flow_version} ({state}).
{context_error}
Flow inventory (current settings):
{inventory}

Playbook sections:
{playbooks}

Past cases on similar incidents (what was tried, what the reviewer did, what it measured):
{memory}
{feedback}"""

FEEDBACK = """
Your previous proposal was rejected by validation.{previous}
Problems:
{problems}
Keep your diagnosis unless a problem shows it was wrong, and change only what the problems name. Reply again with
the full JSON object.
"""

PREVIOUS = """
Diagnosis: {diagnosis}
Steps: {steps}"""

REPAIR = ("Your reply could not be used: {error}\n"
          "Reply again with only the JSON object described in the instructions.")

PROMPT_VERSION = hashlib.sha256(
    (SYSTEM + USER + FEEDBACK + PREVIOUS + REPAIR + catalog.describe_for_prompt()).encode()).hexdigest()[:10]


CRITIC_SYSTEM = """You check a remediation proposal for an Apache NiFi 2.6 flow in Cloudera DataFlow (CDF) before a
person reviews it. Another planner read the evidence below and proposed the fix. Find where the evidence contradicts
the proposal; do not propose a fix of your own.

Object only to:
- a claim in the diagnosis or rationale that a reading, the inventory or a signal contradicts: a queue called empty
  that holds FlowFiles, a rate called zero that is not, FlowFiles said to go where the readings show none arrive;
- for a backlog, more capacity (more tasks, a shorter run schedule, a larger deployment) for a stage that is not the
  bottleneck. The bottleneck is the first stage, in flow order, whose input queue is full and whose output queue is
  not; the queues before it are full only from back pressure, even the one that alerted. Slowing a source, routing
  FlowFiles elsewhere, or changing a threshold or an expiration adds no capacity, and this rule does not apply to it;
- a change to a property or parameter that alters what the flow produces (which fields it reads, how it splits or
  merges FlowFiles) when the fault is how fast it runs or where it loses data. Routing failures to a dead-letter
  queue keeps what was lost; it does not alter what the flow produces.

A reading with no data was not measured: it says nothing about the queue or the rate, which may be full or busy. Never
call it empty or zero, and never quote it.

Do not object to the size of a value; that is checked separately. For each objection, copy as its evidence one line,
or part of one, exactly as it appears in the signals, the readings or the inventory, without its leading dash. An
objection you cannot back with such a line is not an objection. If the proposal agrees with the evidence, reply with
no objections: a needless objection costs the planner a retry.

Reply with only a JSON object, no prose and no code fence:
{"objections": [{"problem": "what is wrong, naming the components", "evidence": "<a line copied from the evidence>"}]}"""

CRITIC_USER = """Incident {incident_id} on deployment {pack}, process group "{process_group}".
Summary: {summary}

Signals:
{signals}

Current readings (the last 15 minutes; rates per second). Every queue of the group, in flow order; one that is not
measured may still be full. Then every rate watched:
{readings}

Deployment: {sizing}.
Flow inventory (current settings):
{inventory}

The proposal to check:
Diagnosis: {diagnosis}
Root cause: {root_cause}
Steps: {steps}
Rationale: {rationale}
Expected effect: {expected_effect}"""

CRITIC_VERSION = hashlib.sha256((CRITIC_SYSTEM + CRITIC_USER + REPAIR).encode()).hexdigest()[:10]


def agent_version(model_id: str, playbooks: str = "") -> str:
    # Without its past cases the same prompt is a different agent (the replay's baseline), and so it is
    # with approved playbook edits (`playbooks`, their rag/overlay.edition), or with the critic checking
    # its proposals; none leaves the name as it was.
    return f"prompt:{PROMPT_VERSION}/model:{model_id or 'unknown'}" + ("" if config.MEMORY else "/no-memory") \
        + (f"/playbooks:{playbooks}" if playbooks else "") + (f"/critic:{CRITIC_VERSION}" if config.CRITIC else "") \
        + (f"/samples:{config.SAMPLES}" if config.SAMPLES > 1 else "")


# ---------------------------------------------------------------------
# Drafting a playbook edit from a reviewer's feedback (rag/drafter.py)
# ---------------------------------------------------------------------

DRAFT_SYSTEM = """You maintain the internal playbooks a remediation agent for Apache NiFi 2.6 flows in Cloudera DataFlow
reads before it proposes a fix. A reviewer turned down a fix the agent based on these playbooks, and said why. Decide
whether the playbook advice led the agent wrong, and if it did, draft the smallest edit of one section that would
have led it right. A person reviews your edit before the agent ever reads it.

Rules:
- Edit one section: give the full new body of an existing section (by its id), or add a section to one playbook
  (section_id "" and a new heading). Prefer the section the agent relied on.
- Change the advice the feedback contradicts, minimally; keep every other line of the section as it reads, and its
  <!-- symptoms: --> and <!-- actions: --> lines. No markdown heading lines (# or ##) in the text.
- Keep the section's tables: their rows are values the flows' owners set. Change a value in its row only when the
  feedback shows that value is wrong; never remove a row.
- Write general guidance in what you add: no incident ids, CRNs or deployment names, and no values that are right
  only for this incident.
- Name only these actions: {actions}. Never advise restarting the deployment or emptying queues
  (STOP_AND_EMPTY_QUEUES).
- The platform rules (cdf-platform-rules.md) are not edited here.
- If the feedback is about this incident only (a value the reviewer prefers, a timing, a fault the playbooks already
  describe correctly), do not edit: reply with skip true and say why in the rationale.

Reply with only a JSON object, no prose and no code fence:
{{
  "skip": false,
  "playbook": "<file name>.md",
  "section_id": "<the id of the section you rewrite, or empty for a new section>",
  "heading": "<the new section's heading; the existing heading otherwise>",
  "text": "<the section's full new body, markdown>",
  "rationale": "what the feedback showed the playbook got wrong, and how the edit fixes it, in two sentences"
}}"""

DRAFT_USER = """{feedback}

The playbook sections the agent relied on, as it read them:
{cited}
{open}
Every section you may edit (id: heading):
{toc}"""

DRAFT_FEEDBACK = """
Your edit cannot be used as it is. Fix these problems and reply again with the full JSON object:
{problems}
"""

DRAFT_VERSION = hashlib.sha256((DRAFT_SYSTEM + DRAFT_USER + DRAFT_FEEDBACK + REPAIR).encode()).hexdigest()[:10]


def system_prompt() -> str:
    return SYSTEM.format(catalog=catalog.describe_for_prompt(), max_steps=catalog.MAX_STEPS)


def _signal_line(signal: dict) -> str:
    evidence = json.dumps(signal.get("evidence") or {}, sort_keys=True, default=str)

    if len(evidence) > 400:
        evidence = evidence[:400] + "..."

    return f"- [{signal.get('severity', '')}] {signal.get('summary', '')} evidence={evidence}"


def _playbook_block(hit: dict) -> str:
    return f"[{hit['id']}] {hit.get('heading') or hit.get('title') or ''}\n{hit.get('text', '').strip()}"


def _previous(proposal: dict | None) -> str:
    """The proposal validation sent back, so a retry changes it rather than starting over."""

    if not proposal or not (proposal.get("diagnosis") or proposal.get("steps")):
        return ""

    return PREVIOUS.format(diagnosis=proposal.get("diagnosis") or "(none)",
                           steps=json.dumps(proposal.get("steps") or [], default=str))


def _evidence(incident: dict, context: dict) -> dict:
    """The fields the planner and the critic both read."""

    sizing = context.get("sizing") or {}

    return dict(
        incident_id=incident["id"], pack=incident["pack"], process_group=incident["process_group"],
        summary=incident.get("summary") or "",
        signals="\n".join(_signal_line(s) for s in incident.get("signals") or []) or "- none recorded",
        readings=context.get("readings") or "- none recorded",
        sizing=(f"{sizing.get('cluster_size')} x{sizing.get('nodes')} node(s)" if sizing else "unknown"),
        inventory=context.get("inventory") or "(unavailable)",
    )


def user_prompt(incident: dict, context: dict, playbooks: list, feedback: list | None = None,
                memory: list | None = None, previous: dict | None = None) -> str:
    return USER.format(
        **_evidence(incident, context),
        flow_version=context.get("flow_version") or "?", state=context.get("state") or "?",
        context_error=(f"Note: {context['error']}\n" if context.get("error") else ""),
        playbooks="\n\n".join(_playbook_block(h) for h in playbooks) or "(none found)",
        memory="\n\n".join(h["text"].strip() for h in memory or []) or "(none)",
        feedback=FEEDBACK.format(previous=_previous(previous), problems="\n".join(f"- {p}" for p in feedback))
        if feedback else "",
    )


def critic_prompt(incident: dict, context: dict, proposal: dict) -> str:
    return CRITIC_USER.format(
        **_evidence(incident, context),
        diagnosis=proposal.get("diagnosis") or "(none)",
        root_cause=proposal.get("root_cause_component") or "(none)",
        steps=json.dumps(proposal.get("steps") or [], default=str),
        rationale=proposal.get("rationale") or "(none)",
        expected_effect=proposal.get("expected_effect") or "(none)",
    )


def critic_evidence(incident: dict, context: dict) -> str:
    """What the critic may quote: the signals, the readings and the inventory, as it read them."""

    found = _evidence(incident, context)

    return "\n".join((found["signals"], found["readings"], found["inventory"]))


def _plain(text: str) -> str:
    return " ".join(str(text or "").lower().split())


def quoted(evidence: str, text: str) -> bool:
    """
    Whether an objection's evidence is copied from the text: a dozen
    characters or more, spacing aside, and not a reading with no data,
    which shows only that nothing was measured.
    """

    quote = _plain(evidence).strip(" \"'`-")

    if len(quote) < 12 or quote not in _plain(text):
        return False

    lines = [line for line in map(_plain, str(text).splitlines()) if quote in line]   # none: it spans lines

    return not lines or any("no data in the last" not in line for line in lines)


# ---------------------------------------------------------------------
# Reply parsing
# ---------------------------------------------------------------------

_FENCE = re.compile(r"```(?:json)?\s*(.*?)```", re.S)


def text_of(message) -> str:
    content = getattr(message, "content", message)

    if isinstance(content, list):
        return "".join(part.get("text", "") if isinstance(part, dict) else str(part) for part in content)

    return str(content or "")


def extract_json(text: str) -> dict:
    """
    The proposal object in a reply, fenced or not. Raises ValueError.

    A reasoning model (nemotron) thinks out loud first and ends with
    </think>, and its thinking can sketch the answer's shape with
    placeholders ("steps": [...]). So only the text after the last
    </think> counts, and of the objects in it the last that parses wins,
    fenced ones first.
    """

    text = text.rsplit("</think>", 1)[-1]
    error = ""

    for block in _FENCE.findall(text) + [text]:
        objects, error = _objects(block, error)

        if objects:
            return objects[-1]

    raise ValueError(error or "the reply has no JSON object")


def _objects(text: str, error: str) -> tuple:
    """([top-level JSON objects in text], the first parse error seen)."""

    found, start = [], text.find("{")

    while start >= 0:
        try:
            value, length = json.JSONDecoder().raw_decode(text[start:])

        except json.JSONDecodeError as exc:
            error = error or f"the JSON does not parse: {exc}"
            start = text.find("{", start + 1)
            continue

        if isinstance(value, dict):
            found.append(value)

        start = text.find("{", start + length)

    return found, error
