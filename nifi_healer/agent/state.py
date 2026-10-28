"""
The graph's state, one per incident (thread_id = incident id).

Everything here is plain JSON (dicts, lists, strings), because the
SqliteSaver checkpoints it after every node and a pending review has to
survive an app restart. The store holds the durable record; the state
is what the next node needs.
"""

from typing import TypedDict


class AgentState(TypedDict, total=False):
    incident_id: str
    incident: dict           # the incidents row when the run started
    context: dict            # {inventory, sizing, flow_version, flow_version_crn, state, error}
    playbooks: list          # retriever hits: {id, collection, heading, text, similarity}
    memory: list             # past cases (rag/memory.recall): {id, headline, text, similarity}
    priors: list             # fix track records (rag/priors.for_incident): {scope, fix, worked, tries, ...}
    proposal: dict           # schemas.Proposal as a dict; steps may be a reviewer's edit
    proposal_id: int
    plan: dict               # executor.public(executor.plan(...)): ok, problems, diff, commands, risk;
                             # warnings, capacity, the vote when several were drawn, the tool
                             # calls when it made any, and the critic's critique when it read the fix
    attempts: int            # LLM proposals made so far in this run
    vote: dict               # with SELFHEAL_SAMPLES, how the drawn proposals agreed; {} for one draw
    tool_calls: list         # with SELFHEAL_TOOLS, the proposal's tool calls: {tool, args, result}
    feedback: list           # problems with the last proposal, for the next attempt
    source: str              # "agent" | "review" | "rollback": who wrote the steps being validated
    review: dict             # {decision, reason, reviewer, steps}
    review_error: str        # why the last decision was not accepted
    result: dict             # the actions row executor.execute returned
    agent_version: str       # prompt hash + model id
    error: str               # why the run stopped early (LLM unavailable, unreadable flow)
