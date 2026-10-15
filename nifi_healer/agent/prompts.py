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
- Restarting the deployment is not available and does not drain a backlog. Raising a back-pressure threshold
  in front of a slow consumer only lets the backlog grow; fix the consumer.
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
Your previous proposal was rejected by validation. Fix these problems and reply again with the full JSON object:
{problems}
"""

REPAIR = ("Your reply could not be used: {error}\n"
          "Reply again with only the JSON object described in the instructions.")

PROMPT_VERSION = hashlib.sha256(
    (SYSTEM + USER + FEEDBACK + REPAIR + catalog.describe_for_prompt()).encode()).hexdigest()[:10]


def agent_version(model_id: str, playbooks: str = "") -> str:
    # Without its past cases the same prompt is a different agent (the replay's baseline), and so it is
    # with approved playbook edits (`playbooks`, their rag/overlay.edition); none leaves the name as it was.
    return f"prompt:{PROMPT_VERSION}/model:{model_id or 'unknown'}" + ("" if config.MEMORY else "/no-memory") \
        + (f"/playbooks:{playbooks}" if playbooks else "")


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
- Change the advice the feedback contradicts, minimally; keep the rest of the section, and its
  <!-- symptoms: --> and <!-- actions: --> lines. No markdown heading lines (# or ##) in the text.
- Write general guidance that holds for any flow: no incident ids, CRNs or deployment names, and no values that are
  right only for this incident.
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


def user_prompt(incident: dict, context: dict, playbooks: list, feedback: list | None = None,
                memory: list | None = None) -> str:
    sizing = context.get("sizing") or {}

    return USER.format(
        incident_id=incident["id"], pack=incident["pack"], process_group=incident["process_group"],
        summary=incident.get("summary") or "",
        signals="\n".join(_signal_line(s) for s in incident.get("signals") or []) or "- none recorded",
        sizing=(f"{sizing.get('cluster_size')} x{sizing.get('nodes')} node(s)" if sizing else "unknown"),
        flow_version=context.get("flow_version") or "?", state=context.get("state") or "?",
        context_error=(f"Note: {context['error']}\n" if context.get("error") else ""),
        inventory=context.get("inventory") or "(unavailable)",
        playbooks="\n\n".join(_playbook_block(h) for h in playbooks) or "(none found)",
        memory="\n\n".join(h["text"].strip() for h in memory or []) or "(none)",
        feedback=FEEDBACK.format(problems="\n".join(f"- {p}" for p in feedback)) if feedback else "",
    )


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
