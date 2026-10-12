"""
The graph's nodes. Each takes the state and returns the fields it changed.

    gather_context        the deployed flow's inventory, sizing, version
    retrieve_playbooks    playbook sections for the signals
    diagnose_and_propose  the LLM: evidence -> Proposal (one repair retry)
    validate_proposal     executor.plan: catalog, bounds, targets, diff, argv
    human_review          interrupt(): approve | reject | edit
    apply_remediation     executor.execute (dry run unless SELFHEAL_ARMED)
    record_feedback       a rejection, with its reason
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
from nifi_healer.actions import catalog, executor
from nifi_healer.agent import prompts
from nifi_healer.agent.schemas import Proposal
from nifi_healer.cdp.client import get_client
from nifi_healer.flow.inventory import inventory, render
from nifi_healer.rag import retriever
from nifi_healer.sim.world import groups_from_definition
from nifi_healer.store.db import get_store

# LLM proposals per run before an invalid one goes to review as it is.
MAX_ATTEMPTS = 3

DECISIONS = ("approve", "reject", "edit")


def log(message: str) -> None:
    print(f"[agent] {message}", flush=True)


def _now() -> float:
    return get_client().now()


def _status(incident_id: str, status: str) -> None:
    get_store().update_incident(incident_id, _now(), status=status)


# ---------------------------------------------------------------------
# Context
# ---------------------------------------------------------------------


def gather_context(state: dict) -> dict:
    client, store = get_client(), get_store()
    incident = store.incident(state["incident_id"])
    _status(incident["id"], "DIAGNOSING")
    deployment, flow = incident["deployment_crn"], incident["deployed_flow_crn"]
    context = {"inventory": "", "sizing": {}, "error": ""}

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
        context["inventory"] = render(inventory(definition, incident["process_group"], groups))

    sizing = client.deployment_configuration(deployment)

    if "error" not in sizing:
        context["sizing"] = {"cluster_size": (sizing.get("clusterSize") or {}).get("name"),
                             "nodes": sizing.get("staticNodeCount")}

    return {"incident": incident, "context": context, "attempts": 0, "feedback": [],
            "source": "agent", "error": "", "review_error": ""}


def retrieve_playbooks(state: dict) -> dict:
    incident = state["incident"]

    try:
        hits = retriever.for_incident(incident.get("signals") or [],
                                      context=f"{incident['process_group']}: {incident.get('summary') or ''}")

    except Exception as exc:  # retrieval is evidence, not a precondition
        log(f"{incident['id']}: playbook search failed: {exc}")
        hits = []

    keep = ("id", "collection", "heading", "title", "text", "similarity")

    return {"playbooks": [{k: h.get(k) for k in keep} for h in hits]}


# ---------------------------------------------------------------------
# Diagnosis
# ---------------------------------------------------------------------


def _ask(model, messages: list) -> tuple:
    """(Proposal | None, raw text, error). One repair turn on a bad reply."""

    raw, error = "", ""

    for turn in range(2):
        reply = model.invoke(messages)
        raw = prompts.text_of(reply)

        try:
            return Proposal.model_validate(prompts.extract_json(raw)), raw, ""

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
    version = prompts.agent_version(getattr(model, "model_name", "") or config.LLM_MODEL_ID)
    messages = [SystemMessage(content=prompts.system_prompt()),
                HumanMessage(content=prompts.user_prompt(incident, state["context"], state.get("playbooks") or [],
                                                         state.get("feedback")))]

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
        report = executor.public(executor.plan(client, incident, proposal.get("steps") or []))

    known = {h["id"] for h in state.get("playbooks") or []}
    cited = list(proposal.get("citations") or [])
    report["warnings"] = [f"cites {c!r}, which was not among the retrieved sections" for c in cited
                          if c not in known]

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


def after_validation(state: dict) -> str:
    report = state["plan"]

    if report["ok"]:
        return "no_action" if report["family"] == catalog.NONE else "review"

    if state.get("source") != "review" and state.get("attempts", 0) < MAX_ATTEMPTS:
        return "retry"

    return "review"


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

    if decision == "edit" and not isinstance(steps, list):
        return {"review_error": "an edit needs the new steps, as a list"}

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

    return {"approve": "apply", "reject": "reject", "edit": "validate"}[state["review"]["decision"]]


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
    _status(state["incident_id"], "REJECTED")
    log(f"{state['incident_id']}: rejected: {state['review'].get('reason')}")

    return {}


def record_no_action(state: dict) -> dict:
    _status(state["incident_id"], "NO_ACTION")
    log(f"{state['incident_id']}: no action: {state['proposal'].get('diagnosis', '')[:120]}")

    return {}
