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
- Cite the playbook section ids you relied on, exactly as given.

Action catalog:
{catalog}

Reply with only a JSON object, no prose and no code fence:
{{
  "diagnosis": "what is wrong and why, in two or three sentences, naming the components",
  "root_cause_component": "the processor, connection or parameter at fault",
  "steps": [{{"action": "SET_CONCURRENT_TASKS", "target": "<processor name>", "value": 4}}],
  "rationale": "why these steps fix the root cause, and why the obvious alternatives would not",
  "citations": ["<playbook section id>"],
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
{feedback}"""

FEEDBACK = """
Your previous proposal was rejected by validation. Fix these problems and reply again with the full JSON object:
{problems}
"""

REPAIR = ("Your reply could not be used: {error}\n"
          "Reply again with only the JSON object described in the instructions.")

PROMPT_VERSION = hashlib.sha256(
    (SYSTEM + USER + FEEDBACK + REPAIR + catalog.describe_for_prompt()).encode()).hexdigest()[:10]


def agent_version(model_id: str) -> str:
    return f"prompt:{PROMPT_VERSION}/model:{model_id or 'unknown'}"


def system_prompt() -> str:
    return SYSTEM.format(catalog=catalog.describe_for_prompt(), max_steps=catalog.MAX_STEPS)


def _signal_line(signal: dict) -> str:
    evidence = json.dumps(signal.get("evidence") or {}, sort_keys=True, default=str)

    if len(evidence) > 400:
        evidence = evidence[:400] + "..."

    return f"- [{signal.get('severity', '')}] {signal.get('summary', '')} evidence={evidence}"


def _playbook_block(hit: dict) -> str:
    return f"[{hit['id']}] {hit.get('heading') or hit.get('title') or ''}\n{hit.get('text', '').strip()}"


def user_prompt(incident: dict, context: dict, playbooks: list, feedback: list | None = None) -> str:
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
    """The first JSON object in a reply, fenced or not. Raises ValueError."""

    fenced = _FENCE.search(text)

    if fenced:
        text = fenced.group(1)

    start = text.find("{")

    if start < 0:
        raise ValueError("the reply has no JSON object")

    try:
        value, _ = json.JSONDecoder().raw_decode(text[start:])

    except json.JSONDecodeError as exc:
        raise ValueError(f"the JSON does not parse: {exc}") from None

    if not isinstance(value, dict):
        raise ValueError("the reply's JSON is not an object")

    return value
