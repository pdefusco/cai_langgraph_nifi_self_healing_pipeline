"""
The review app: Fleet, Review Queue, History, Playbooks, Monitor, Scorecard, System.

    python app.py            # CAI Application script; binds CDSW_APP_PORT on 127.0.0.1

startup() runs once per process: the mock tenant (built if absent), the
poller thread with the agent as its on_incident hook, and a re-queue of
incidents a previous process left undiagnosed. Page loads never start
anything, so opening the app in five tabs still polls once.

Every handler reads the store or the poller cache and goes through
views.py; the UI has no write path of its own besides a reviewer's
decision, and none at all to SELFHEAL_ARMED.
"""

import functools
import json
import os
import socket
import sqlite3
import threading
import time
import traceback

import gradio as gr

import config
from nifi_healer import fleet
from nifi_healer.agent import autonomy, graph
from nifi_healer.cdp.client import get_client
from nifi_healer.rag import retriever
from nifi_healer.runtime import poller as poller_module
from nifi_healer.store.db import get_store
from nifi_healer.ui import legend, monitor, playbooks, scorecard, views

_started = threading.Event()
_start_lock = threading.Lock()

# A handler slower than this is logged, to find what keeps a page waiting.
SLOW_HANDLER_SECONDS = 2.0


def log(message: str) -> None:
    print(f"[app] {message}", flush=True)


def startup() -> None:
    with _start_lock:
        if _started.is_set():
            return

        config.ensure_dirs()
        _claim_data_dir()
        client = get_client()

        if config.MOCK:
            from nifi_healer.sim import bootstrap

            if bootstrap.ensure():
                log("built the mock tenant")

        threading.Thread(target=_warm_playbooks, name="rag-warm", daemon=True).start()
        poller_module.start(client, get_store(), on_incident=graph.submit, on_tick=autonomy.tick)
        queued = graph.recover() if config.AUTO_DIAGNOSE else 0

        if queued:
            log(f"re-queued {queued} undiagnosed incident(s)")

        log(f"{client.kind} client, writes {'ARMED' if config.ARMED else 'disarmed'}")
        _started.set()


def _warm_playbooks() -> None:
    try:
        retriever.ensure()

    except Exception as exc:  # the first incident retries; the agent runs without playbooks if not
        log(f"playbook ingest failed: {exc}")


def _snapshot() -> dict:
    current = poller_module.get()

    if current is None:
        return {"packs": {}, "heartbeat": {}, "now": get_client().now()}

    return current.snapshot()


# ---------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------


def _owner_file() -> str:
    return os.path.join(config.DATA_DIR, "app.owner")


def _claim_data_dir() -> None:
    """Record this process as the data directory's user, to name a second one if locks appear."""

    me = {"host": socket.gethostname(), "pid": os.getpid(), "started": time.time()}

    try:
        with open(_owner_file(), "w") as handle:
            json.dump(me, handle)

    except OSError as exc:
        log(f"could not write {_owner_file()}: {exc}")

    log(f"using {config.DATA_DIR} as {me['host']} pid {me['pid']}")


def _other_owner() -> str:
    """Who else claimed the data directory since this process did, or ""."""

    try:
        with open(_owner_file()) as handle:
            owner = json.load(handle)

    except (OSError, ValueError):
        return ""

    if owner.get("host") == socket.gethostname() and owner.get("pid") == os.getpid():
        return ""

    return f"{owner.get('host')} pid {owner.get('pid')}"


def _locked(handler, exc: sqlite3.OperationalError):
    frames = [f.filename for f in traceback.extract_tb(exc.__traceback__)]
    database = ("checkpoints" if any("checkpoint" in f for f in frames)
                else "healer" if any(f.endswith(os.path.join("store", "db.py")) for f in frames) else "?")
    other = _other_owner()
    log(f"{handler.__qualname__}: {exc} ({database} database)"
        + (f"; another app process claimed the data directory: {other}" if other else ""))

    return gr.Error(f"The {database} database was busy ({exc}); this view loads on the next refresh."
                    + (f" Another app process ({other}) is using the same data directory." if other else ""))


def _timed(handler):
    @functools.wraps(handler)
    def timed(*args, **kwargs):
        began = time.monotonic()

        try:
            return handler(*args, **kwargs)

        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc):
                raise

            raise _locked(handler, exc) from exc

        finally:
            took = time.monotonic() - began

            if took > SLOW_HANDLER_SECONDS:
                log(f"slow handler {handler.__qualname__}: {took:.1f}s")

    return timed


@_timed
def refresh_fleet(choice):
    snapshot = _snapshot()

    store = get_store()

    return (views.fleet_header(snapshot), views.deployment_rows(snapshot, store),
            views.group_rows(snapshot, store), views.fleet_rows(snapshot, store),
            views.kpi_frame(snapshot, choice))


def _new_choices(choices: list, selected):
    """
    A timer's dropdown update. It leaves the value alone while the
    selection is still listed. A tick sends the value it read when it
    fired, so setting it back would undo a pick made while it ran.
    """

    values = [value for _, value in choices]

    if selected in values:
        return gr.update(choices=choices)

    return gr.update(choices=choices, value=values[0] if values else None)


@_timed
def refresh_kpi_choices(choice):
    return _new_choices(views.kpi_choices(_snapshot()), choice)


@_timed
def refresh_queue(selected):
    return _new_choices(views.queue_choices(get_store(), keep=selected), selected)


def _queue_at(incident_id):
    """The queue with incident_id selected: the incident a decision was just made on."""

    return gr.update(choices=views.queue_choices(get_store(), keep=incident_id), value=incident_id)


# show_review's outputs: evidence, proposal, diff, commands, steps and the five buttons (Hold among them).
PANEL_OUTPUTS = 10


@_timed
def show_review(incident_id):
    view = views.review(get_store(), get_client(), incident_id)
    pending = view["pending"]

    return (view["evidence"], view["proposal"], view["diff"], view["commands"],
            gr.Code(value=view["steps"], interactive=pending),
            gr.Button(interactive=view["can_approve"]),
            gr.Button(interactive=pending), gr.Button(interactive=pending),
            gr.Button(interactive=view["can_diagnose"]), gr.Button(visible=view["can_hold"]))


def _panel(incident_id) -> tuple:
    """
    The panel and its key, the key read first: a run that moves on while
    the panel renders then changes the key, and the next follow re-renders.
    (Read after, it named the newer state the panel never showed: a
    rollback reached review mid-render and its buttons stayed disabled.)
    """

    key = views.review_key(get_store(), incident_id)

    return show_review(incident_id) + (key,)


@_timed
def select_review(incident_id):
    return _panel(incident_id)


@_timed
def follow_review(incident_id, seen):
    """Re-render the panel only when it would change (an approval being applied finishes)."""

    key = views.review_key(get_store(), incident_id)

    if key == seen:
        return (gr.skip(),) * (PANEL_OUTPUTS + 1)

    return _panel(incident_id)


def caller(request: gr.Request | None) -> tuple:
    """
    (login, permission) of whoever sent the request: CAI's proxy adds
    REMOTE-USER and REMOTE-USER-PERM (RO or RW) to every request to an
    Application. ("", "") when they are absent (run locally).
    """

    headers = getattr(request, "headers", None) or {}

    return (headers.get("remote-user") or "").strip(), (headers.get("remote-user-perm") or "").strip()


@_timed
def who_is_reviewing(request: gr.Request):
    return caller(request)[0]


def _answer(message, incident_id):
    """A decision handler's outputs: the message, the panel, the queue and the panel's key."""

    *panel, key = _panel(incident_id)

    return (message, *panel, _queue_at(incident_id), key)


def _shows_another(incident_id, shown) -> str:
    """
    Why not to act when the panel the reviewer read (shown, its review key)
    was for another incident than the dropdown's, else "".
    """

    if shown and shown.split("|", 1)[0] != incident_id:
        return (f"The panel was showing another incident; it now shows {incident_id}, and nothing was sent. "
                "Check it and decide again.")

    return ""


def _decide(decision):
    @_timed
    def handler(incident_id, reviewer, reason, steps_text, shown=None, request: gr.Request = None,
                progress=gr.Progress()):
        login, permission = caller(request)

        if permission.upper() == "RO":
            message = (f"{login or 'You'} can only view this project, so the decision was not sent; "
                       "a project collaborator with write access decides.")

            return _answer(message, incident_id)

        if mismatch := _shows_another(incident_id, shown):
            return _answer(mismatch, incident_id)

        if decision == "approve":
            progress(0, desc="Applying (dry run)" if not config.ARMED else "Applying to the tenant")

        message = views.decide(incident_id, decision, (reviewer or "").strip() or login, reason, steps_text)

        return _answer(message, incident_id)

    return handler


@_timed
def diagnose(incident_id, shown=None, reviewer="", request: gr.Request = None):
    login, permission = caller(request)

    if permission.upper() == "RO" and incident_id and graph.pending(incident_id):
        # Diagnosing an open review again replaces its proposal: a decision.
        return _answer(f"{login or 'You'} can only view this project, so the review was not diagnosed "
                       "again; a project collaborator with write access decides.", incident_id)

    return _answer(_shows_another(incident_id, shown)
                   or views.diagnose(incident_id, (reviewer or "").strip() or login), incident_id)


@_timed
def refresh_history(selected):
    store = get_store()
    choices = views.history_choices(store)
    values = [value for _, value in choices]
    selected = selected if selected in values else (values[0] if values else None)

    # The detail too: an unchanged pick fires no .change, and its status may have moved on.
    return (views.history_rows(store), gr.Dropdown(choices=choices, value=selected),
            views.incident_detail(store, selected))


@_timed
def show_incident(incident_id):
    return views.incident_detail(get_store(), incident_id)


@_timed
def pick_fix(incident_id, current):
    """The incident's applied fixes to correct, keeping the pick while it is still one of them."""

    choices = views.correction_choices(get_store(), incident_id)
    values = [value for _, value in choices]

    return gr.Dropdown(choices=choices, value=current if current in values else (values[0] if values else None))


def _corrected(message, incident_id):
    """A correction's outputs: the message, the history table and the incident's detail."""

    store = get_store()

    return message, views.history_rows(store), views.incident_detail(store, incident_id)


def _read_only(login, permission) -> str:
    if permission.upper() == "RO":
        return (f"{login or 'You'} can only view this project, so nothing was recorded; "
                "a project collaborator with write access corrects outcomes.")

    return ""


@_timed
def label_fix(incident_id, action_id, label, reason, reviewer, request: gr.Request = None):
    login, permission = caller(request)

    return _corrected(_read_only(login, permission)
                      or views.label_fix(incident_id, action_id, label, reason, (reviewer or "").strip() or login),
                      incident_id)


@_timed
def recheck(incident_id, action_id, window, start, reviewer, request: gr.Request = None,
            progress=gr.Progress()):
    login, permission = caller(request)

    if refused := _read_only(login, permission):
        return _corrected(refused, incident_id)

    progress(0, desc="Reading the chart")

    return _corrected(views.recheck(incident_id, action_id, window, start, (reviewer or "").strip() or login,
                                    runner=poller_module.get()), incident_id)


@_timed
def propose_rollback(incident_id, action_id, reason, reviewer, request: gr.Request = None):
    login, permission = caller(request)

    return _corrected(_read_only(login, permission)
                      or views.propose_rollback(incident_id, action_id, reason, (reviewer or "").strip() or login),
                      incident_id)


@_timed
def refresh_playbooks(view, selected):
    return _new_choices(playbooks.choices(get_store(), view, keep=selected), selected)


def _edit_panel(edit_id) -> tuple:
    """The edit's panel, its text box, its four buttons and its key."""

    p = playbooks.panel(get_store(), edit_id)

    return (p["html"], gr.Textbox(value=p["text"], interactive=p["can_edit"]),
            gr.Button(interactive=p["can_approve"]), gr.Button(interactive=p["can_reject"]),
            gr.Button(interactive=p["can_revert"]), gr.Button(interactive=p["can_redraft"]), p["key"])


@_timed
def select_playbook_edit(edit_id):
    return _edit_panel(edit_id)


@_timed
def follow_playbook_edit(edit_id, seen):
    """Re-render only when the edit changed (a draft finishing), so a text being edited stays."""

    if playbooks.key(get_store(), edit_id) == seen:
        return (gr.skip(),) * 7

    return _edit_panel(edit_id)


_PLAYBOOK_DECISIONS = {
    "approve": lambda edit_id, text, reason, who: views.approve_playbook_edit(edit_id, text, who),
    "reject": lambda edit_id, text, reason, who: views.reject_playbook_edit(edit_id, reason, who),
    "revert": lambda edit_id, text, reason, who: views.revert_playbook_edit(edit_id, reason, who),
    "redraft": lambda edit_id, text, reason, who: views.redraft_playbook_edit(edit_id),
}


def _playbook_decision(decision):
    @_timed
    def handler(view, edit_id, text, reason, reviewer, shown=None, request: gr.Request = None):
        login, permission = caller(request)

        if permission.upper() == "RO":
            message = (f"{login or 'You'} can only view this project, so nothing was changed; "
                       "a project collaborator with write access decides on playbook edits.")

        elif shown and shown.split("|", 1)[0] != str(edit_id):
            message = "The panel was showing another edit; it now shows this one, and nothing was sent. Decide again."

        else:
            message = _PLAYBOOK_DECISIONS[decision](edit_id, text, reason, (reviewer or "").strip() or login)

        return (message, *_edit_panel(edit_id),
                gr.update(choices=playbooks.choices(get_store(), view, keep=edit_id), value=edit_id),
                views.playbook_patch())

    return handler


@_timed
def playbook_patch():
    return views.playbook_patch()


def _monitor_model(pack, span, change):
    entry = fleet.deployed_packs().get(pack or "")

    if not entry:
        return None

    return monitor.model(get_store(), pack, entry, get_client().now(), span or monitor.DEFAULT_RANGE, change)


@_timed
def refresh_monitor(pack, span, change):
    """The timer's and the page load's: the deployments, the chart and its changes."""

    packs = _new_choices(monitor.deployment_choices(), pack)
    pack = packs.get("value", pack)
    m = _monitor_model(pack, span, change)

    return packs, monitor.render(m), _new_choices(monitor.change_choices(m) if m else [], change)


@_timed
def pick_deployment(pack, span):
    """Another deployment: its newest change is selected."""

    m = _monitor_model(pack, span, None)
    choices = monitor.change_choices(m) if m else []

    return monitor.render(m), gr.update(choices=choices, value=choices[0][1] if choices else None)


@_timed
def show_monitor(pack, span, change):
    return monitor.render(_monitor_model(pack, span, change))


@_timed
def show_fleet(span, query, sort, attention, pack):
    """The fleet overview, with the deployment shown below it highlighted."""

    f = monitor.fleet_model(get_store(), fleet.deployed_packs(), get_client().now(), span or monitor.DEFAULT_RANGE,
                            query or "", sort or monitor.FLEET_SORTS[0], bool(attention), pack or "")

    return monitor.render_fleet(f)


@_timed
def pick_fleet(span, evt: gr.EventData):
    """A fleet row clicked: its deployment below, at the marker's change if one was clicked."""

    data = getattr(evt, "_data", None) or {}
    m = _monitor_model(data.get("pack"), span, data.get("pick"))

    if not m:
        return gr.skip(), gr.skip(), gr.skip()

    choices = monitor.change_choices(m)
    selected = str(m["selected"]) if m.get("selected") is not None else None

    return (gr.update(value=data["pack"]), monitor.render(m),
            gr.update(choices=choices, value=selected if selected in [v for _, v in choices] else None))


def pick_change(evt: gr.EventData):
    """A change marker or ledger column clicked in the chart."""

    pick = (getattr(evt, "_data", None) or {}).get("pick")

    return gr.update(value=str(pick)) if pick is not None else gr.skip()


@_timed
def show_scorecard(since, grouping, source=scorecard.LIVE):
    m = scorecard.model(get_store(), get_client().now(), since or scorecard.DEFAULT_SINCE,
                        grouping or scorecard.GROUPINGS[0], source or scorecard.LIVE)

    return scorecard.render(m)


@_timed
def refresh_system():
    return views.system(_snapshot(), get_client())


def _autonomy_switch(paused: bool):
    @_timed
    def handler(request: gr.Request = None):
        login, permission = caller(request)

        if permission.upper() == "RO":
            return (f"{login or 'You'} can only view this project, so autonomy was left as it is; "
                    "a project collaborator with write access pauses or resumes it."), refresh_system()

        return views.set_autonomy(paused, login), refresh_system()

    return handler


def poll_now():
    current = poller_module.get()

    if current is None:
        return "The poller is not running."

    current.poll_now()

    return "Polled every deployment."


def selftest():
    return get_client().selftest()


def fast_forward(minutes):
    if not config.MOCK:
        return "Fast-forward only exists in mock mode."

    get_client().world.skip(float(minutes) * 60)
    current = poller_module.get()

    if current is not None:
        current.poll_now()
        current.evaluate_outcomes()

    return f"Moved the mock clock {minutes:g} minutes forward, polled, and evaluated outcomes."


# ---------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------


def _legend(text: str) -> None:
    with gr.Accordion("How to read this page", open=False):
        gr.Markdown(text)


def build() -> gr.Blocks:
    with gr.Blocks(title="NiFi Self-Healing") as demo:
        gr.Markdown("## NiFi Self-Healing Pipeline\n"
                    f"CDF DataFlow · {'mock tenant' if config.MOCK else 'live tenant'} · writes "
                    f"{'**ARMED**' if config.ARMED else 'disarmed (dry runs)'}"
                    + (" · autonomy on (trusted low-risk fixes approve themselves)" if config.AUTONOMY else ""))

        with gr.Tab("Fleet"):
            _legend(legend.FLEET)

            fleet_header = gr.Markdown()
            deployment_table = gr.Dataframe(headers=views.DEPLOYMENT_HEADERS, interactive=False, wrap=True,
                                            label="Deployments")
            group_table = gr.Dataframe(headers=views.GROUP_HEADERS, interactive=False, wrap=True,
                                       max_height=420, label="Process groups (in and out: FlowFiles per bucket)")

            with gr.Accordion("KPIs", open=True):
                fleet_table = gr.Dataframe(headers=views.FLEET_HEADERS, interactive=False, wrap=True,
                                           max_height=520)

            with gr.Row():
                kpi_pick = gr.Dropdown(label="KPI chart", choices=[], scale=3, allow_custom_value=True)

            kpi_plot = gr.LinePlot(x="time", y="value", color="series", height=300, label="KPI")
            fleet_outputs = [fleet_header, deployment_table, group_table, fleet_table, kpi_plot]
            fleet_timer = gr.Timer(1.0)
            choices_timer = gr.Timer(15.0)
            fleet_timer.tick(refresh_fleet, inputs=[kpi_pick], outputs=fleet_outputs, show_progress="hidden")
            choices_timer.tick(refresh_kpi_choices, inputs=[kpi_pick], outputs=[kpi_pick],
                               show_progress="hidden")
            kpi_pick.change(refresh_fleet, inputs=[kpi_pick], outputs=fleet_outputs)

        with gr.Tab("Review Queue"):
            _legend(legend.REVIEW)

            with gr.Row():
                queue_pick = gr.Dropdown(label="Incident", choices=[], scale=4, allow_custom_value=True)
                queue_refresh = gr.Button("Refresh", scale=1)

            with gr.Row():
                with gr.Column(scale=2):
                    evidence = gr.Markdown()

                with gr.Column(scale=3):
                    proposal = gr.Markdown()

            diff = gr.Dataframe(headers=views.DIFF_HEADERS, interactive=False, wrap=True, label="Change")
            commands = gr.Code(language="shell", label="Exact cdp commands", interactive=False)
            steps = gr.Code(language="json", label="Steps (edit, then Submit edit to re-validate)",
                            interactive=False)

            with gr.Row():
                reviewer = gr.Textbox(label="Reviewer", placeholder="your CAI login if left blank", scale=1)
                reason = gr.Textbox(label="Reason (required to reject)", scale=3)

            with gr.Row():
                approve_btn = gr.Button("Approve", variant="primary", interactive=False)
                edit_btn = gr.Button("Submit edit", interactive=False)
                reject_btn = gr.Button("Reject", variant="stop", interactive=False)
                diagnose_btn = gr.Button("Diagnose", interactive=False)
                hold_btn = gr.Button("Hold for review", visible=False)

            review_result = gr.Markdown()
            panel = [evidence, proposal, diff, commands, steps, approve_btn, edit_btn, reject_btn, diagnose_btn,
                     hold_btn]
            seen = gr.State("")
            queue_timer = gr.Timer(5.0)
            follow_timer = gr.Timer(3.0)

            queue_pick.change(select_review, inputs=[queue_pick], outputs=panel + [seen])
            queue_refresh.click(refresh_queue, inputs=[queue_pick], outputs=[queue_pick]).then(
                select_review, inputs=[queue_pick], outputs=panel + [seen])
            queue_timer.tick(refresh_queue, inputs=[queue_pick], outputs=[queue_pick], show_progress="hidden")
            follow_timer.tick(follow_review, inputs=[queue_pick, seen], outputs=panel + [seen],
                              show_progress="hidden")

            # seen goes in so a decision can tell the panel the reviewer read
            # was for the incident the dropdown now names.
            decision_inputs = [queue_pick, reviewer, reason, steps, seen]
            decision_outputs = [review_result] + panel + [queue_pick, seen]
            approve_btn.click(_decide("approve"), inputs=decision_inputs, outputs=decision_outputs)
            edit_btn.click(_decide("edit"), inputs=decision_inputs, outputs=decision_outputs)
            reject_btn.click(_decide("reject"), inputs=decision_inputs, outputs=decision_outputs)
            hold_btn.click(_decide("hold"), inputs=decision_inputs, outputs=decision_outputs)
            diagnose_btn.click(diagnose, inputs=[queue_pick, seen, reviewer], outputs=decision_outputs)

        with gr.Tab("History"):
            _legend(legend.HISTORY)

            history_refresh = gr.Button("Refresh")
            history_table = gr.Dataframe(headers=views.HISTORY_HEADERS, interactive=False, wrap=True,
                                         max_height=420)
            history_pick = gr.Dropdown(label="Incident", choices=[], allow_custom_value=True)
            history_detail = gr.Markdown()

            # The measured verdicts stay as measured: a reviewer labels the fix
            # (the scorecard counts the label), re-measures a window, say from
            # after another fault cleared, or proposes undoing the fix.
            with gr.Accordion("Correct the outcome, or undo the fix", open=False):
                fix_pick = gr.Dropdown(label="Applied fix", choices=[], allow_custom_value=True)

                with gr.Row():
                    fix_label = gr.Radio(views.label_choices(), label="Your call on the fix", scale=2)
                    fix_reason = gr.Textbox(label="Reason (required)", scale=3)
                    fix_reviewer = gr.Textbox(label="Reviewer", placeholder="your CAI login if left blank",
                                              scale=1)

                label_btn = gr.Button("Save label")

                with gr.Row():
                    fix_window = gr.Dropdown(views.window_choices(), value=config.VERIFY_WINDOWS[0],
                                             label="Window to re-check", scale=1)
                    fix_start = gr.Textbox(label="Measure from (UTC)", scale=2,
                                           placeholder="blank: when the fix finished · +30 · 14:05 · "
                                                       "2026-10-12 14:05")

                recheck_btn = gr.Button("Re-check")

                # Undo the fix: a rollback incident, approved or rejected in the Review Queue.
                with gr.Row():
                    rollback_reason = gr.Textbox(label="Why undo it (required)", scale=4)
                    rollback_btn = gr.Button("Propose a rollback", scale=1)

                correction_result = gr.Markdown()

            history_refresh.click(refresh_history, inputs=[history_pick],
                                  outputs=[history_table, history_pick, history_detail]).then(
                pick_fix, inputs=[history_pick, fix_pick], outputs=[fix_pick])
            history_pick.change(show_incident, inputs=[history_pick], outputs=[history_detail])
            history_pick.change(pick_fix, inputs=[history_pick, fix_pick], outputs=[fix_pick])
            corrected = [correction_result, history_table, history_detail]
            label_btn.click(label_fix, inputs=[history_pick, fix_pick, fix_label, fix_reason, fix_reviewer],
                            outputs=corrected)
            recheck_btn.click(recheck, inputs=[history_pick, fix_pick, fix_window, fix_start, fix_reviewer],
                              outputs=corrected)
            rollback_btn.click(propose_rollback, inputs=[history_pick, fix_pick, rollback_reason, fix_reviewer],
                               outputs=corrected)

        with gr.Tab("Playbooks"):
            _legend(legend.PLAYBOOKS)

            with gr.Row():
                pb_view = gr.Radio(list(playbooks.VIEWS), value=playbooks.DEFAULT_VIEW, label="Show", scale=2)
                pb_pick = gr.Dropdown(label="Playbook edit", choices=[], scale=5, allow_custom_value=True)
                pb_refresh = gr.Button("Refresh", scale=1)

            pb_html = gr.HTML()
            pb_text = gr.Textbox(label="The section's new text (change it before approving if you like)", lines=10,
                                 max_lines=30, interactive=False)

            with gr.Row():
                pb_reviewer = gr.Textbox(label="Reviewer", placeholder="your CAI login if left blank", scale=1)
                pb_reason = gr.Textbox(label="Reason (required to reject or revert)", scale=3)

            with gr.Row():
                pb_approve = gr.Button("Approve", variant="primary", interactive=False)
                pb_reject = gr.Button("Reject", variant="stop", interactive=False)
                pb_revert = gr.Button("Revert", interactive=False)
                pb_redraft = gr.Button("Draft again", interactive=False)

            pb_result = gr.Markdown()

            with gr.Accordion("Make the applied edits permanent", open=False):
                gr.Markdown("The applied edits as a patch to `playbooks/`. Save it as `playbooks.patch` in a clone of "
                            "the repository, run `git apply playbooks.patch`, commit and push, then pull on the app "
                            "(`pull_and_restart.py`). The app then finds each edit already in the file and marks "
                            "it MERGED, so nothing is applied twice. Empty while no edit is applied.")
                pb_patch = gr.Code(language=None, label="playbooks.patch", interactive=False)

            pb_panel = [pb_html, pb_text, pb_approve, pb_reject, pb_revert, pb_redraft]
            pb_seen = gr.State("")
            pb_timer = gr.Timer(5.0)

            pb_pick.change(select_playbook_edit, inputs=[pb_pick], outputs=pb_panel + [pb_seen])
            pb_view.change(refresh_playbooks, inputs=[pb_view, pb_pick], outputs=[pb_pick])
            pb_refresh.click(refresh_playbooks, inputs=[pb_view, pb_pick], outputs=[pb_pick]).then(
                select_playbook_edit, inputs=[pb_pick], outputs=pb_panel + [pb_seen]).then(
                playbook_patch, outputs=[pb_patch])
            pb_timer.tick(refresh_playbooks, inputs=[pb_view, pb_pick], outputs=[pb_pick],
                          show_progress="hidden").then(
                follow_playbook_edit, inputs=[pb_pick, pb_seen], outputs=pb_panel + [pb_seen],
                show_progress="hidden")

            pb_inputs = [pb_view, pb_pick, pb_text, pb_reason, pb_reviewer, pb_seen]
            pb_outputs = [pb_result] + pb_panel + [pb_seen, pb_pick, pb_patch]
            pb_approve.click(_playbook_decision("approve"), inputs=pb_inputs, outputs=pb_outputs)
            pb_reject.click(_playbook_decision("reject"), inputs=pb_inputs, outputs=pb_outputs)
            pb_revert.click(_playbook_decision("revert"), inputs=pb_inputs, outputs=pb_outputs)
            pb_redraft.click(_playbook_decision("redraft"), inputs=pb_inputs, outputs=pb_outputs)

        with gr.Tab("Monitor"):
            _legend(legend.MONITOR)

            with gr.Row():
                mon_range = gr.Radio(list(monitor.RANGES), value=monitor.DEFAULT_RANGE, label="Range", scale=2)
                fleet_query = gr.Textbox(label="Filter deployments", placeholder="name", scale=2)
                fleet_sort = gr.Radio(list(monitor.FLEET_SORTS), value=monitor.FLEET_SORTS[0], label="Sort",
                                      scale=3)
                fleet_attention = gr.Checkbox(label="Needs attention only", scale=1)

            fleet_html = gr.HTML(js_on_load=monitor.JS)

            with gr.Row():
                mon_pack = gr.Dropdown(label="Deployment", choices=[], scale=2, allow_custom_value=True)
                mon_change = gr.Dropdown(label="Change", choices=[], scale=3, allow_custom_value=True)

            mon_html = gr.HTML(js_on_load=monitor.JS)
            mon_timer = gr.Timer(30.0)
            mon_inputs = [mon_pack, mon_range, mon_change]
            fleet_inputs = [mon_range, fleet_query, fleet_sort, fleet_attention, mon_pack]
            fleet_out = dict(inputs=fleet_inputs, outputs=[fleet_html], show_progress="hidden")

            mon_timer.tick(refresh_monitor, inputs=mon_inputs, outputs=[mon_pack, mon_html, mon_change],
                           show_progress="hidden").then(show_fleet, **fleet_out)
            mon_pack.input(pick_deployment, inputs=[mon_pack, mon_range], outputs=[mon_html, mon_change]).then(
                show_fleet, **fleet_out)
            mon_range.change(show_monitor, inputs=mon_inputs, outputs=[mon_html])
            mon_range.change(show_fleet, **fleet_out)
            fleet_query.input(show_fleet, trigger_mode="always_last", **fleet_out)
            fleet_sort.change(show_fleet, **fleet_out)
            fleet_attention.change(show_fleet, **fleet_out)
            mon_change.change(show_monitor, inputs=mon_inputs, outputs=[mon_html], show_progress="hidden")
            mon_html.click(pick_change, outputs=[mon_change])
            fleet_html.click(pick_fleet, inputs=[mon_range], outputs=[mon_pack, mon_html, mon_change]).then(
                show_fleet, **fleet_out)

        with gr.Tab("Scorecard"):
            _legend(legend.SCORECARD)

            with gr.Row():
                score_source = gr.Radio(list(scorecard.SOURCES), value=scorecard.LIVE, label="Incidents",
                                        info="Replay: the offline replay's trials (eval/replay.py)", scale=2)
                score_since = gr.Radio(list(scorecard.SINCE), value=scorecard.DEFAULT_SINCE, label="Since",
                                       scale=2)
                score_group = gr.Radio(list(scorecard.GROUPINGS), value=scorecard.GROUPINGS[0],
                                       label="Group by", scale=4)
                score_refresh = gr.Button("Refresh", scale=1)

            score_html = gr.HTML()
            score_inputs = dict(inputs=[score_since, score_group, score_source], outputs=[score_html])

            score_refresh.click(show_scorecard, **score_inputs)
            score_source.change(show_scorecard, **score_inputs)
            score_since.change(show_scorecard, **score_inputs)
            score_group.change(show_scorecard, **score_inputs)

        with gr.Tab("System"):
            _legend(legend.SYSTEM)

            system_md = gr.Markdown()

            with gr.Row():
                system_refresh = gr.Button("Refresh")
                poll_btn = gr.Button("Poll now")
                selftest_btn = gr.Button("Selftest")
                pause_btn = gr.Button("Pause autonomy", visible=config.AUTONOMY)
                unpause_btn = gr.Button("Resume autonomy", visible=config.AUTONOMY)

            with gr.Row(visible=config.MOCK):
                minutes = gr.Slider(1, 120, value=20, step=1, label="Fast-forward the mock clock (minutes)")
                forward_btn = gr.Button("Fast-forward")

            system_result = gr.Markdown()
            selftest_out = gr.JSON(label="Selftest")

            system_refresh.click(refresh_system, outputs=[system_md])
            poll_btn.click(poll_now, outputs=[system_result]).then(refresh_system, outputs=[system_md])
            selftest_btn.click(selftest, outputs=[selftest_out])
            pause_btn.click(_autonomy_switch(True), outputs=[system_result, system_md])
            unpause_btn.click(_autonomy_switch(False), outputs=[system_result, system_md])
            forward_btn.click(fast_forward, inputs=[minutes], outputs=[system_result]).then(
                refresh_system, outputs=[system_md])

        # Choices are filled per session; allow_custom_value keeps a pick valid
        # between refreshes (and for API callers). Handlers check the ids.
        demo.load(refresh_kpi_choices, inputs=[kpi_pick], outputs=[kpi_pick])
        demo.load(who_is_reviewing, outputs=[reviewer])
        demo.load(who_is_reviewing, outputs=[fix_reviewer])
        demo.load(who_is_reviewing, outputs=[pb_reviewer])
        demo.load(refresh_playbooks, inputs=[pb_view, pb_pick], outputs=[pb_pick]).then(
            select_playbook_edit, inputs=[pb_pick], outputs=pb_panel + [pb_seen])
        demo.load(playbook_patch, outputs=[pb_patch])
        demo.load(refresh_queue, inputs=[queue_pick], outputs=[queue_pick]).then(
            select_review, inputs=[queue_pick], outputs=panel + [seen])
        demo.load(refresh_history, inputs=[history_pick], outputs=[history_table, history_pick, history_detail]).then(
            pick_fix, inputs=[history_pick, fix_pick], outputs=[fix_pick])
        demo.load(refresh_system, outputs=[system_md])
        demo.load(show_scorecard, **score_inputs)
        demo.load(refresh_monitor, inputs=mon_inputs, outputs=[mon_pack, mon_html, mon_change]).then(
            show_fleet, **fleet_out)

    return demo


def main() -> None:
    startup()
    build().queue(default_concurrency_limit=4).launch(
        server_name=config.APP_HOST, server_port=config.APP_PORT, show_error=True,
        theme=gr.themes.Soft(), css=monitor.CSS + scorecard.CSS + playbooks.CSS)
