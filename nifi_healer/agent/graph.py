"""
The remediation graph, one thread per incident (thread_id = incident id).

    gather_context -> retrieve_playbooks -> diagnose_and_propose -> validate_proposal
    (a rollback incident: propose_rollback -> validate_proposal; no LLM, no retry, and
     its review is approve or reject)
        validate_proposal: invalid, agent-written, attempts left -> diagnose_and_propose
                           NO_ACTION                              -> record_no_action -> END
                           valid, agent-written, SELFHEAL_CRITIC  -> critique_proposal
                           otherwise                              -> human_review
        critique_proposal: an objection, attempts left            -> diagnose_and_propose
                           otherwise                              -> human_review
        human_review [interrupt]: approve    -> apply_remediation -> END
                                  edit       -> validate_proposal
                                  reject     -> record_feedback -> END
                                  rediagnose -> gather_context (the proposal is stale)
                                  invalid    -> human_review (asks again)

The checkpointer is SQLite on the project filesystem, so a run paused at
human_review survives an app restart; the UI resumes it with
resume(incident_id, decision). Runs started by the poller go through a
small thread pool so a slow LLM never stalls polling; armed approvals
go through their own pool (resume_in_background) so a reviewer's click
returns while the deployment changes. A background run that pauses at
review asks agent/autonomy.py whether its fix may approve itself. A run
the LLM didn't answer leaves its incident NEW; retry_failed(), on every
KPI poll, queues it again after config.LLM_RETRY_MINUTES. So does a run
that raised: its incident goes back to NEW with why (run_failure()), and
the retry continues from the step that raised (NFH-101).
"""

import sqlite3
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor

from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command

import config
from nifi_healer.agent import nodes
from nifi_healer.agent.state import AgentState
from nifi_healer.store.db import get_store

# Incident statuses a run can start from.
RUNNABLE = ("NEW", "DIAGNOSING")

# A checkpoint read longer than this is logged.
SLOW_STATE_SECONDS = 1.0

_lock = threading.Lock()
_graph = None
_connection = None
_pool = None
_apply_pool = None
_running: dict = {}  # incident id -> Lock: one run of a thread at a time
_waiting: dict = {}  # incident id -> Future, from submit() until the run ends
_applying: dict = {}  # incident id -> Future, from resume_in_background() until the decision is applied
_apply_failed: dict = {}  # incident id -> why its last background approval raised, until the next one
_llm_failed: dict = {}  # incident id -> (runs in a row the LLM didn't answer or that raised, wall time of the last)
_run_failed: dict = {}  # incident id -> why its last run raised, until a run ends without raising
_started: dict = {}  # incident id -> wall time its background run began, while it runs

# Approvals applied at once; each one waits minutes for its deployment.
APPLY_WORKERS = 4


def build(checkpointer=None):
    graph = StateGraph(AgentState)

    # The nodes that do the work are timed (NFH-94); a review waits on a person.
    for name in ("propose_rollback", "gather_context", "retrieve_playbooks", "diagnose_and_propose",
                 "validate_proposal", "critique_proposal"):
        graph.add_node(name, nodes.timed(name, getattr(nodes, name)))

    graph.add_node("human_review", nodes.human_review)
    graph.add_node("apply_remediation", nodes.timed("apply_remediation", nodes.apply_remediation))
    graph.add_node("record_feedback", nodes.record_feedback)
    graph.add_node("record_no_action", nodes.record_no_action)

    graph.add_conditional_edges(START, nodes.entry,
                                {"diagnose": "gather_context", "rollback": "propose_rollback"})
    graph.add_edge("propose_rollback", "validate_proposal")
    graph.add_edge("gather_context", "retrieve_playbooks")
    graph.add_edge("retrieve_playbooks", "diagnose_and_propose")
    graph.add_conditional_edges("diagnose_and_propose", nodes.after_diagnosis,
                                {"validate": "validate_proposal", "end": END})
    graph.add_conditional_edges("validate_proposal", nodes.after_validation,
                                {"retry": "diagnose_and_propose", "review": "human_review",
                                 "no_action": "record_no_action", "critique": "critique_proposal"})
    graph.add_conditional_edges("critique_proposal", nodes.after_critique,
                                {"retry": "diagnose_and_propose", "review": "human_review"})
    graph.add_conditional_edges("human_review", nodes.after_review,
                                {"apply": "apply_remediation", "reject": "record_feedback",
                                 "validate": "validate_proposal", "again": "human_review",
                                 "rediagnose": "gather_context"})
    graph.add_edge("apply_remediation", END)
    graph.add_edge("record_feedback", END)
    graph.add_edge("record_no_action", END)

    return graph.compile(checkpointer=checkpointer)


def get_graph():
    global _graph, _connection

    with _lock:
        if _graph is None:
            config.ensure_dirs()
            # SqliteSaver serializes access with its own lock. The timeout
            # matches the store's: the project filesystem is network storage.
            _connection = sqlite3.connect(str(config.CHECKPOINT_PATH), timeout=30, check_same_thread=False)
            _graph = build(SqliteSaver(_connection))

        return _graph


def reset() -> None:
    """Forget the compiled graph and its connection (tests, a changed CHECKPOINT_PATH)."""

    global _graph, _connection

    with _lock:
        if _connection is not None:
            _connection.close()

        _graph, _connection = None, None
        _running.clear()
        _waiting.clear()
        _applying.clear()
        _apply_failed.clear()
        _llm_failed.clear()
        _run_failed.clear()


def _config(incident_id: str) -> dict:
    return {"configurable": {"thread_id": incident_id}}


def _thread_lock(incident_id: str) -> threading.Lock:
    with _lock:
        return _running.setdefault(incident_id, threading.Lock())


def start(incident_id: str) -> dict:
    """
    Run an incident's graph until it ends or waits for review. Returns the
    state. A run already waiting for review is left alone; one cut off
    mid-way (the app stopped) continues from its last checkpoint; a NEW
    incident whose last run ended early (no LLM) is diagnosed again.
    """

    graph = get_graph()

    with _thread_lock(incident_id):
        snapshot = graph.get_state(_config(incident_id))

        if snapshot.interrupts:
            return snapshot.values

        if snapshot.next:
            nodes.log(f"{incident_id}: continuing an interrupted run at {', '.join(snapshot.next)}")

            return _noted(incident_id, graph.invoke(None, _config(incident_id)))

        incident = get_store().incident(incident_id)

        if incident is None:
            raise ValueError(f"no incident {incident_id}")

        if not diagnosable(incident):
            raise ValueError(f"{incident_id} is {incident['status']}; only {' or '.join(RUNNABLE)} "
                             "incidents are diagnosed, or one marked for review with no paused review")

        return _noted(incident_id, graph.invoke({"incident_id": incident_id}, _config(incident_id)))


def resume(incident_id: str, decision: dict) -> dict:
    """
    Answer a pending review: {"decision": "approve"|"reject"|"edit",
    "reason", "reviewer", "steps"}. Returns the state; if the decision was
    not accepted the run is waiting again and pending() carries the error.
    """

    graph = get_graph()

    with _thread_lock(incident_id):
        if not pending(incident_id):
            raise ValueError(f"{incident_id} is not waiting for a review")

        return _noted(incident_id, graph.invoke(Command(resume=decision), _config(incident_id)))


def _noted(incident_id: str, values: dict) -> dict:
    """Count a run diagnose_and_propose ended because the LLM didn't answer; any other run clears the count."""

    with _lock:
        if ((values or {}).get("error") or "").startswith("LLM "):
            _llm_failed[incident_id] = (_llm_failed.get(incident_id, (0, 0.0))[0] + 1, time.time())

        else:
            _llm_failed.pop(incident_id, None)

    return values


def llm_failures(incident_id: str) -> int:
    """Runs in a row of this incident the LLM didn't answer, or that raised (kept in memory only)."""

    with _lock:
        return _llm_failed.get(incident_id, (0, 0.0))[0]


def retry_at(incident_id: str) -> float | None:
    """The wall time retry_failed() queues the incident again, or None when its last run wasn't an LLM failure."""

    with _lock:
        found = _llm_failed.get(incident_id)

    if found is None:
        return None

    count, at = found

    return at + 60 * config.LLM_RETRY_MINUTES[min(count, len(config.LLM_RETRY_MINUTES)) - 1]


def retry_failed(now: float | None = None) -> int:
    """
    Queue again every incident still NEW after a run the LLM didn't
    answer, once its wait is over (the poller's tick, with AUTO_DIAGNOSE).
    An incident no longer NEW is forgotten. Returns how many were queued.
    """

    now = time.time() if now is None else now
    store, queued = get_store(), 0

    with _lock:
        failed = list(_llm_failed)

    for incident_id in failed:
        incident = store.incident(incident_id)

        if incident is None or incident["status"] != "NEW":
            with _lock:
                _llm_failed.pop(incident_id, None)

            continue

        due = retry_at(incident_id)

        if due is not None and due <= now and run_state(incident_id) is None:
            nodes.log(f"{incident_id}: asking the LLM again (it didn't answer {llm_failures(incident_id)} time(s))")
            submit(incident)
            queued += 1

    return queued


def _snapshot(incident_id: str):
    """The run's checkpoint; a read over SLOW_STATE_SECONDS is logged, with its thread (NFH-111)."""

    began = time.monotonic()
    snapshot = get_graph().get_state(_config(incident_id))
    took = time.monotonic() - began

    if took > SLOW_STATE_SECONDS:
        nodes.log(f"{incident_id}: reading its checkpoint took {took:.1f}s ({threading.current_thread().name})")

    return snapshot


def pending(incident_id: str) -> dict | None:
    """The review payload if the incident's run is paused at human_review."""

    snapshot = _snapshot(incident_id)

    for item in snapshot.interrupts or ():
        return item.value

    return None


def state(incident_id: str) -> dict:
    return _snapshot(incident_id).values


def forget(incident_ids: list) -> int:
    """
    Delete closed incidents' checkpoints (retention): nothing reads a
    closed incident's run again. Returns how many threads were deleted.
    """

    if not incident_ids:
        return 0

    checkpointer = get_graph().checkpointer

    for incident_id in incident_ids:
        with _thread_lock(incident_id):
            checkpointer.delete_thread(incident_id)

        with _lock:
            _running.pop(incident_id, None)

    return len(incident_ids)


def orphaned(incident: dict) -> bool:
    """
    Marked PENDING_REVIEW but with no paused review: human_review set the
    status and then the checkpoint write failed (the database was locked).
    """

    return incident["status"] == "PENDING_REVIEW" and not pending(incident["id"])


def diagnosable(incident: dict) -> bool:
    return incident["status"] in RUNNABLE or orphaned(incident)


def recover() -> int:
    """
    At app start: queue every incident left NEW or DIAGNOSING (the app
    stopped, or the LLM was down), and every orphaned review. Pending
    reviews need nothing; their checkpoint is waiting. Returns how many
    were queued.
    """

    store, queued = get_store(), 0
    orphans = [i for i in store.incidents(statuses=("PENDING_REVIEW",)) if orphaned(i)]

    for incident in store.incidents(statuses=RUNNABLE) + orphans:
        submit(incident)
        queued += 1

    return queued


# ---------------------------------------------------------------------
# Background runs (the poller's on_incident hook)
# ---------------------------------------------------------------------


def _executor() -> ThreadPoolExecutor:
    global _pool

    with _lock:
        if _pool is None:
            _pool = ThreadPoolExecutor(max_workers=config.AGENT_WORKERS, thread_name_prefix="agent")

        return _pool


def _stopped(incident_id: str, exc: Exception, doing: str = "run") -> None:
    """
    A run raised: log it with its traceback and put a DIAGNOSING incident
    back to NEW, so the panel shows why and retry_failed() tries again on
    the LLM's backoff. Left DIAGNOSING, it read "diagnosing" with nothing
    running, and nothing ever ran it again (live, S50 on p22).
    """

    why = f"{type(exc).__name__}: {exc}"
    nodes.log(f"{incident_id}: {doing} failed: {why}\n{traceback.format_exc()}")

    with _lock:
        _run_failed[incident_id] = why
        _llm_failed[incident_id] = (_llm_failed.get(incident_id, (0, 0.0))[0] + 1, time.time())

    try:
        store = get_store()
        incident = store.incident(incident_id)

        if incident and incident["status"] == "DIAGNOSING":
            store.update_incident(incident_id, time.time(), status="NEW")

    except Exception as again:  # the panel still says the run stopped
        nodes.log(f"{incident_id}: could not mark it NEW: {type(again).__name__}: {again}")


def run_failure(incident_id: str) -> str:
    """Why the incident's last run raised, or "" (kept in memory only)."""

    with _lock:
        return _run_failed.get(incident_id, "")


def _run(incident_id: str) -> None:
    _started[incident_id] = time.time()

    try:
        start(incident_id)

    except Exception as exc:  # a failed run must not kill the worker
        _stopped(incident_id, exc)
        return

    finally:
        _ran(incident_id)

    with _lock:
        _run_failed.pop(incident_id, None)

    from nifi_healer.agent import autonomy  # it imports this module

    try:
        autonomy.consider(incident_id)

    except Exception as exc:  # the review stays open for a person either way
        nodes.log(f"{incident_id}: autonomy check failed: {type(exc).__name__}: {exc}")


def _rediagnose(incident_id: str, decision: dict) -> None:
    _started[incident_id] = time.time()

    try:
        resume(incident_id, decision)

    except Exception as exc:  # a failed run must not kill the worker
        _stopped(incident_id, exc, "diagnosing again")
        return

    finally:
        _ran(incident_id)

    with _lock:
        _run_failed.pop(incident_id, None)


def _ran(incident_id: str) -> None:
    _started.pop(incident_id, None)
    nodes.steps.pop(incident_id, None)


def progress(incident_id: str) -> dict | None:
    """
    How far a running background run has got, for the review panel
    (NFH-113): {step, step_started, started} (wall times), and from the
    LLM step on {attempt, attempts, sent_back}: which try of how many,
    and the problems the last try was sent back for ([] on the first).
    None when no run is running. The step is None until the first timed
    node begins.
    """

    if (run_state(incident_id) or ("",))[0] != "running" or incident_id not in _started:
        return None

    step, since, attempts, feedback = nodes.steps.get(incident_id) or (None, None, 0, [])
    started = _started[incident_id]

    if since is not None and since < started:      # the last run's
        step, since = None, None

    # The LLM step runs before its try is counted; the checks after it.
    attempt = (attempts + 1 if step == "diagnose_and_propose" else
               attempts if step in ("validate_proposal", "critique_proposal") else None)

    return {"step": step, "step_started": since, "started": started, "attempt": attempt,
            "attempts": nodes.MAX_ATTEMPTS, "sent_back": feedback if attempt and attempt > 1 else []}


def submit(incident: dict, rediagnose: dict | None = None):
    """
    Start an incident's run in the background. Returns the Future; an
    incident already queued or running keeps its place and its Future.
    With `rediagnose` (a decision: reviewer, reason), a stale review is
    answered "rediagnose" instead, which runs the agent again from
    gather_context on the same worker pool.
    """

    incident_id = incident["id"]
    pool = _executor()

    with _lock:
        future = _waiting.get(incident_id)

        if future is None:
            future = _waiting[incident_id] = (
                pool.submit(_rediagnose, incident_id, dict(rediagnose, decision="rediagnose")) if rediagnose
                else pool.submit(_run, incident_id))

    future.add_done_callback(lambda done: _forget(incident_id, done, _waiting))

    return future


def _forget(incident_id: str, future, futures: dict) -> None:
    with _lock:
        if futures.get(incident_id) is future:
            del futures[incident_id]


def _resume(incident_id: str, decision: dict) -> None:
    _apply_failed.pop(incident_id, None)

    try:
        resume(incident_id, decision)

    except Exception as exc:  # recorded by the executor where it can be; logged and shown either way
        _apply_failed[incident_id] = f"{type(exc).__name__}: {exc}"
        nodes.log(f"{incident_id}: applying the decision failed: {_apply_failed[incident_id]}")


def apply_failure(incident_id: str) -> str:
    """Why the incident's last background approval raised, or "" (kept in memory only)."""

    return _apply_failed.get(incident_id, "")


def resume_in_background(incident_id: str, decision: dict):
    """
    resume() on a worker thread, for an armed approval: the executor waits
    for the deployment to settle, which takes minutes, and a UI handler
    that long blocks the reviewer's tab. Returns the Future; an incident
    already being applied keeps its Future and the new decision is dropped.
    """

    global _apply_pool

    with _lock:
        if _apply_pool is None:
            _apply_pool = ThreadPoolExecutor(max_workers=APPLY_WORKERS, thread_name_prefix="apply")

        future = _applying.get(incident_id)

        if future is None:
            future = _applying[incident_id] = _apply_pool.submit(_resume, incident_id, decision)

    future.add_done_callback(lambda done: _forget(incident_id, done, _applying))

    return future


def run_state(incident_id: str) -> tuple | None:
    """
    None when no run is queued or running for the incident; else
    ("applying", 0, 0) for a decision being applied, ("running", 0, 0), or
    ("queued", position, waiting) with position from 1.
    """

    with _lock:
        if incident_id in _applying:
            return "applying", 0, 0

        future = _waiting.get(incident_id)

        if future is None:
            return None

        if future.running():
            return "running", 0, 0

        queued = [i for i, f in _waiting.items() if not f.running() and not f.done()]

    return ("queued", queued.index(incident_id) + 1, len(queued)) if incident_id in queued else None


def backlog() -> dict:
    """Runs queued and running, and decisions being applied: the App health counts."""

    with _lock:
        return {"agent.queue": sum(1 for f in _waiting.values() if not f.done()),
                "apply.inflight": sum(1 for f in _applying.values() if not f.done())}
