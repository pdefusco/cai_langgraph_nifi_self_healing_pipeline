"""
The graph's nodes. Each takes the state and returns the fields it changed.

    entry                 a rollback incident skips the diagnosis
    propose_rollback      ROLLBACK of the one action it undoes, without the LLM
    gather_context        the deployed flow's inventory, sizing, version, readings, and notes on
                          backlogged stages more capacity would not help (actions/capacity.py)
    retrieve_playbooks    playbook sections for the signals and the parameters used,
                          the most similar past cases (rag/memory.py), and, for
                          the reviewer only, the fixes' track records (rag/priors.py)
    diagnose_and_propose  the LLM: evidence -> Proposal (one repair retry)
    validate_proposal     executor.plan: catalog, bounds, targets, diff, argv; capacity
                          against the intake (actions/capacity.py)
    critique_proposal     with SELFHEAL_CRITIC, the LLM again: objections to a valid fix,
                          each backed by a line it quotes from the evidence
    human_review          interrupt(): approve | reject | edit | rediagnose
    apply_remediation     executor.execute (dry run unless SELFHEAL_ARMED)
    record_feedback       a rejection, with its reason, queued for a playbook edit (rag/drafter.py)
    record_no_action      the agent says leave it alone

The store gets the durable record (proposals, reviews, actions, incident
status) as it happens; the state carries only what the next node needs.
Nodes read the client and store through get_client()/get_store(), so
tests can swap both.
"""

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langgraph.types import interrupt
from pydantic import ValidationError

import config
from nifi_healer import llm
from nifi_healer.actions import capacity, catalog, executor, rollback
from nifi_healer.agent import prompts
from nifi_healer.agent.schemas import Critique, Proposal
from nifi_healer.cdp.client import get_client
from nifi_healer.flow.inventory import inventory, render
from nifi_healer.kpi import readings
from nifi_healer.rag import drafter, memory, priors, retriever
from nifi_healer.sim.world import groups_from_definition
from nifi_healer.store.db import get_store

# LLM proposals per run before an invalid one goes to review as it is.
MAX_ATTEMPTS = 3

DECISIONS = ("approve", "reject", "edit", "rediagnose")


def log(message: str) -> None:
    print(f"[agent] {message}", flush=True)


def _now() -> float:
    return get_client().now()


def _status(incident_id: str, status: str) -> None:
    get_store().update_incident(incident_id, _now(), status=status)


# ---------------------------------------------------------------------
# Context
# ---------------------------------------------------------------------


def entry(state: dict) -> str:
    return "rollback" if rollback.is_rollback(get_store().incident(state["incident_id"])) else "diagnose"


def propose_rollback(state: dict) -> dict:
    """The rollback incident's one proposal: undo its action. Nothing to diagnose, so no LLM."""

    store = get_store()
    incident = store.incident(state["incident_id"])
    _status(incident["id"], "DIAGNOSING")
    action_id = incident["rollback_of_action_id"]
    undone = store.action(action_id) or {}
    proposal = {
        "diagnosis": incident.get("summary") or "",
        "root_cause_component": "",
        "steps": [{"action": "ROLLBACK", "action_id": action_id}],
        "rationale": f"Undo action {action_id} ({undone.get('action_type')}) of {undone.get('incident_id')}: "
                     "set back what it changed, as it was before it was applied.",
        "expected_effect": "the KPIs return to where they were before that action",
        "citations": [],
        "confidence": 1.0,
    }

    return {"incident": incident, "context": {}, "playbooks": [], "proposal": proposal, "attempts": 1,
            "feedback": [], "source": "rollback", "agent_version": "rollback", "error": "", "review_error": ""}


def gather_context(state: dict) -> dict:
    client, store = get_client(), get_store()
    incident = store.incident(state["incident_id"])
    _status(incident["id"], "DIAGNOSING")
    deployment, flow = incident["deployment_crn"], incident["deployed_flow_crn"]
    context = {"inventory": "", "parameters": [], "sizing": {}, "error": ""}
    found = None

    facts = client.flow_facts(deployment, flow)
    context.update(flow_version=facts.get("flow_version"), flow_version_crn=facts.get("flow_version_crn"),
                   state=facts.get("state"))
    version = incident.get("expected_flow_version_crn") or facts.get("flow_version_crn")

    if version and facts.get("flow_version_crn") and version != facts["flow_version_crn"]:
        context["error"] = (f"the deployment now runs v{facts.get('flow_version')}, not the version this "
                            "incident was opened on; the inventory is of the version it was opened on")

    definition = client.flow_definition(version) if version else {"error": "no flow version recorded"}

    if "error" in definition:
        context["error"] = f"could not read the flow definition: {definition['error']}"

    else:
        configuration = client.flow_configuration(deployment, flow)
        groups = (configuration.get("parameterGroups") if "error" not in configuration else None) \
            or groups_from_definition(definition)
        found = inventory(definition, incident["process_group"], groups)
        context.update(inventory=render(found), parameters=found.get("parameters") or [])

    sizing = client.deployment_configuration(deployment)

    if "error" not in sizing:
        context["sizing"] = {"cluster_size": (sizing.get("clusterSize") or {}).get("name"),
                             "nodes": sizing.get("staticNodeCount")}

    context["readings"] = readings.render(readings.for_incident(incident, store, client.now()), found)

    try:
        context["analysis"] = "\n".join(capacity.analysis(found, incident, store, client.now()))

    except Exception as exc:  # the notes help the diagnosis; they are not a precondition of it
        log(f"{incident['id']}: capacity analysis failed: {exc}")
        context["analysis"] = ""

    return {"incident": incident, "context": context, "attempts": 0, "feedback": [],
            "source": "agent", "error": "", "review_error": ""}


def retrieve_playbooks(state: dict) -> dict:
    incident = state["incident"]

    try:
        hits = retriever.for_incident(incident.get("signals") or [],
                                      context=f"{incident['process_group']}: {incident.get('summary') or ''}",
                                      parameters=(state.get("context") or {}).get("parameters") or [])

    except Exception as exc:  # retrieval is evidence, not a precondition
        log(f"{incident['id']}: playbook search failed: {exc}")
        hits = []

    try:
        cases = memory.recall(incident)

    except Exception as exc:  # so is memory
        log(f"{incident['id']}: past cases unavailable: {exc}")
        cases = []

    try:
        record = priors.for_incident(incident)

    except Exception as exc:  # and so are the track records
        log(f"{incident['id']}: fix track records unavailable: {exc}")
        record = []

    keep = ("id", "collection", "heading", "title", "text", "similarity")

    return {"playbooks": [{k: h.get(k) for k in keep} for h in hits],
            "memory": [{k: h.get(k) for k in ("id", "headline", "text", "similarity")} for h in cases],
            "priors": record}


# ---------------------------------------------------------------------
# Diagnosis
# ---------------------------------------------------------------------


def _ask(model, messages: list, schema=Proposal) -> tuple:
    """(a `schema` | None, raw text, error). One repair turn on a bad reply."""

    raw, error = "", ""

    for turn in range(2):
        reply = llm.invoke(model, messages)
        raw = prompts.text_of(reply)

        try:
            return schema.model_validate(prompts.extract_json(raw)), raw, ""

        except (ValueError, ValidationError) as exc:
            error = str(exc) if isinstance(exc, ValueError) and not isinstance(exc, ValidationError) else \
                "; ".join(f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors())
            messages = messages + [AIMessage(content=raw),
                                   HumanMessage(content=prompts.REPAIR.format(error=error))]

    return None, raw, error


def diagnose_and_propose(state: dict) -> dict:
    incident = state["incident"]
    ok, reason = llm.chat_available()

    if not ok:
        log(f"{incident['id']}: no LLM: {reason}")
        _status(incident["id"], "NEW")

        return {"error": f"LLM unavailable: {reason}"}

    model = llm.chat_model()
    version = prompts.agent_version(getattr(model, "model_name", "") or config.LLM_MODEL_ID, retriever.edition())
    messages = [SystemMessage(content=prompts.system_prompt()),
                HumanMessage(content=prompts.user_prompt(incident, state["context"], state.get("playbooks") or [],
                                                         state.get("feedback"), state.get("memory"),
                                                         state.get("proposal")))]

    try:
        proposal, raw, error = _ask(model, messages)

    except Exception as exc:  # the endpoint, not the reply
        log(f"{incident['id']}: LLM call failed: {exc}")
        _status(incident["id"], "NEW")

        return {"error": f"LLM call failed: {exc}"}

    attempts = state.get("attempts", 0) + 1

    if proposal is None:
        log(f"{incident['id']}: attempt {attempts}: unusable reply: {error}")

        return {"proposal": {"diagnosis": "", "steps": [], "rationale": raw[:2000], "citations": [],
                             "confidence": 0.0, "parse_error": error},
                "attempts": attempts, "agent_version": version, "source": "agent"}

    return {"proposal": proposal.as_dict(), "attempts": attempts, "agent_version": version,
            "source": "agent"}


def after_diagnosis(state: dict) -> str:
    return "end" if state.get("error") else "validate"


# ---------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------


def validate_proposal(state: dict) -> dict:
    client, store = get_client(), get_store()
    incident, proposal = state["incident"], dict(state["proposal"])

    if proposal.get("parse_error"):
        report = {"ok": False, "problems": [f"the reply was not a usable proposal: {proposal['parse_error']}"],
                  "family": "", "steps": [], "risk": "high", "changes": [], "diff": [], "commands": []}

    else:
        full = executor.plan(client, incident, proposal.get("steps") or [], store)
        report = executor.public(full)
        report["capacity"] = _capacity(full, incident, state)

    known = {h["id"] for h in (state.get("playbooks") or []) + (state.get("memory") or [])}
    cited = list(proposal.get("citations") or [])
    report["warnings"] = [f"cites {c!r}, which was not among the retrieved sections" for c in cited
                          if c not in known]
    failed = priors.warning(state.get("priors"), report["steps"] or proposal.get("steps") or [])

    if failed:
        report["warnings"].append(failed)

    if report.get("capacity"):
        # Too small a fix goes back to the agent while it has attempts left; after
        # that, or on a reviewer's edit, a person decides with the warning.
        if state.get("source") == "agent" and state.get("attempts", 0) < MAX_ATTEMPTS:
            report["problems"] = report["problems"] + report["capacity"]
            report["ok"] = False

        else:
            report["warnings"].extend(report["capacity"])

    if report["ok"] and report["family"] == catalog.NONE:
        status = "NO_ACTION"

    else:
        status = "VALID" if report["ok"] else "INVALID"

    for older in store.proposals(incident["id"]):
        if older["status"] in ("VALID", "INVALID", "NO_ACTION"):
            store.update_proposal(older["id"], status="SUPERSEDED")

    proposal_id = store.add_proposal(
        incident["id"], _now(), attempt=state.get("attempts", 0),
        agent_version=state.get("agent_version", "") if state.get("source") != "review"
        else f"{state.get('agent_version', '')}+review",
        diagnosis=proposal.get("diagnosis", ""), action=report["steps"] or proposal.get("steps") or [],
        rationale=proposal.get("rationale", ""), citations=cited, validation=report,
        risk=report["risk"], status=status)
    log(f"{incident['id']}: proposal {proposal_id} {status}"
        + (f": {'; '.join(report['problems'])}" if report["problems"] else ""))

    return {"plan": report, "proposal_id": proposal_id, "feedback": report["problems"]}


def _capacity(full: dict, incident: dict, state: dict) -> list:
    if not full["ok"]:
        return []

    try:
        nodes = ((state.get("context") or {}).get("sizing") or {}).get("nodes") or 1

        return capacity.check(full, incident, get_store(), _now(), nodes)

    except Exception as exc:  # a check on the evidence, never a reason to fail validation
        log(f"{incident['id']}: capacity check failed: {exc}")

        return []


def after_validation(state: dict) -> str:
    report = state["plan"]

    if report["ok"]:
        if report["family"] == catalog.NONE:
            return "no_action"

        return "critique" if config.CRITIC and state.get("source") == "agent" else "review"

    if state.get("source") == "agent" and state.get("attempts", 0) < MAX_ATTEMPTS:
        return "retry"

    return "review"


# ---------------------------------------------------------------------
# A second reading
# ---------------------------------------------------------------------


def critique_proposal(state: dict) -> dict:
    """
    The LLM reads a valid fix against the same evidence and objects where
    the evidence contradicts it. An objection counts only with a line it
    quotes from the signals, readings or inventory: it sends the fix back
    while the agent has attempts left, and is a warning after that. No
    second opinion at all is a warning too, so the fix waits for a person.
    """

    incident, store = state["incident"], get_store()
    report = dict(state["plan"], warnings=list(state["plan"].get("warnings") or []))
    critique = {"version": prompts.CRITIC_VERSION, "objections": [], "dropped": [], "error": ""}
    reply = None
    ok, reason = llm.chat_available()

    if not ok:
        critique["error"] = f"LLM unavailable: {reason}"

    else:
        messages = [SystemMessage(content=prompts.CRITIC_SYSTEM),
                    HumanMessage(content=prompts.critic_prompt(incident, state["context"], state["proposal"]))]

        try:
            reply, _, error = _ask(llm.chat_model(), messages, Critique)
            critique["error"] = f"unusable reply: {error}" if reply is None else ""

        except Exception as exc:  # the endpoint, not the reply
            critique["error"] = f"LLM call failed: {exc}"

    evidence = prompts.critic_evidence(incident, state["context"])

    for objection in reply.objections if reply else []:
        found = critique["objections"] if prompts.quoted(objection.evidence, evidence) else critique["dropped"]
        found.append(objection.model_dump())

    report["critique"] = critique
    objections = [f"a second reading objects: {o['problem']} (the evidence: \"{o['evidence']}\")"
                  for o in critique["objections"]]

    if critique["error"]:
        report["warnings"].append(f"no second opinion: {critique['error']}")

    elif objections and state.get("attempts", 0) < MAX_ATTEMPTS:
        report["problems"] = list(report["problems"]) + objections
        report["ok"] = False

    else:
        report["warnings"].extend(objections)

    store.update_proposal(state["proposal_id"], validation=report, status="VALID" if report["ok"] else "INVALID")
    log(f"{incident['id']}: proposal {state['proposal_id']} critique: "
        + (critique["error"] or f"{len(critique['objections'])} objection(s), {len(critique['dropped'])} unquoted"))

    return {"plan": report, "feedback": report["problems"]}


def after_critique(state: dict) -> str:
    return "review" if state["plan"]["ok"] else "retry"


# ---------------------------------------------------------------------
# Review
# ---------------------------------------------------------------------


def review_payload(state: dict) -> dict:
    return {
        "incident_id": state["incident_id"],
        "proposal_id": state.get("proposal_id"),
        "proposal": state.get("proposal"),
        "plan": state.get("plan"),
        "playbooks": [{"id": h["id"], "heading": h.get("heading")} for h in state.get("playbooks") or []],
        "memory": [{"id": h["id"], "headline": h.get("headline")} for h in state.get("memory") or []],
        "armed": config.ARMED,
        "error": state.get("review_error", ""),
    }


def human_review(state: dict) -> dict:
    incident_id = state["incident_id"]
    _status(incident_id, "PENDING_REVIEW")
    answer = interrupt(review_payload(state)) or {}
    decision = str(answer.get("decision") or "").strip().lower()
    reason = str(answer.get("reason") or "").strip()
    reviewer = str(answer.get("reviewer") or "").strip()

    if decision not in DECISIONS:
        return {"review_error": f"decision must be one of {', '.join(DECISIONS)}, got {decision!r}"}

    if decision == "approve" and not state["plan"]["ok"]:
        return {"review_error": "this proposal failed validation; edit it or reject it"}

    if decision == "reject" and not reason:
        return {"review_error": "a rejection needs a reason (it is what the agent learns from)"}

    steps = answer.get("steps")

    if decision in ("edit", "rediagnose") and rollback.is_rollback(state.get("incident")):
        return {"review_error": "a rollback is approved or rejected as proposed; to change something else, "
                                "reject it and diagnose the incident it came from"}

    if decision == "edit" and not isinstance(steps, list):
        return {"review_error": "an edit needs the new steps, as a list"}

    if decision == "rediagnose":
        # The proposal read a flow version the deployment no longer runs:
        # diagnose again on the one it runs now.
        incident = state["incident"]
        facts = get_client().flow_facts(incident["deployment_crn"], incident["deployed_flow_crn"])

        if facts.get("error") or not facts.get("flow_version_crn"):
            return {"review_error": f"could not read the deployed flow: {facts.get('error') or 'no version'}"}

        get_store().update_incident(incident_id, _now(), expected_flow_version_crn=facts["flow_version_crn"])

    get_store().add_review(incident_id, decision, _now(), proposal_id=state.get("proposal_id"),
                           reviewer=reviewer, reason=reason, edits=steps if decision == "edit" else None)
    review = {"decision": decision, "reason": reason, "reviewer": reviewer}

    if decision == "edit":
        proposal = dict(state["proposal"], steps=steps)
        proposal.pop("parse_error", None)

        return {"review": dict(review, steps=steps), "review_error": "", "proposal": proposal,
                "source": "review"}

    return {"review": review, "review_error": ""}


def after_review(state: dict) -> str:
    if state.get("review_error"):
        return "again"

    return {"approve": "apply", "reject": "reject", "edit": "validate",
            "rediagnose": "rediagnose"}[state["review"]["decision"]]


# ---------------------------------------------------------------------
# Outcomes of a review
# ---------------------------------------------------------------------


def apply_remediation(state: dict) -> dict:
    client, store = get_client(), get_store()
    incident = store.incident(state["incident_id"])
    proposal = {"id": state.get("proposal_id"), "action": state["plan"]["steps"], "diff": state["plan"].get("diff")}
    action = executor.execute(client, store, incident, proposal, wait=_wait_for(client, incident))

    return {"result": {k: action.get(k) for k in ("id", "status", "dry_run", "action_type", "argv", "error",
                                                  "from_flow_version_crn", "to_flow_version_crn")}}


def _wait_for(client, incident: dict):
    """The mock settles a change at once; a live tenant is polled until steady."""

    world = getattr(client, "world", None)

    if world is not None:
        return lambda: world.settle(incident["deployment_crn"])

    return None


def record_feedback(state: dict) -> dict:
    incident_id = state["incident_id"]
    _status(incident_id, "REJECTED")
    log(f"{incident_id}: rejected: {state['review'].get('reason')}")

    try:  # the reason, drafted into a playbook edit for review (rag/drafter.py)
        rejected = [r for r in get_store().reviews(incident_id) if r["decision"] == "reject"]

        if rejected:
            drafter.queue(drafter.REJECT, incident_id, rejected[-1]["id"])

    except Exception as exc:  # never fails the rejection
        log(f"{incident_id}: could not queue a playbook edit: {exc}")

    return {}


def record_no_action(state: dict) -> dict:
    _status(state["incident_id"], "NO_ACTION")
    log(f"{state['incident_id']}: no action: {state['proposal'].get('diagnosis', '')[:120]}")

    return {}
