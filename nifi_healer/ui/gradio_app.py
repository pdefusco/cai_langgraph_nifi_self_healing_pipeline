"""
The review app: Fleet, Review Queue, History, System.

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
from nifi_healer.agent import graph
from nifi_healer.cdp.client import get_client
from nifi_healer.rag import retriever
from nifi_healer.runtime import poller as poller_module
from nifi_healer.store.db import get_store
from nifi_healer.ui import views

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
        poller_module.start(client, get_store(), on_incident=graph.submit)
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

    return (views.fleet_header(snapshot), views.fleet_rows(snapshot, get_store()),
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


# show_review's outputs: evidence, proposal, diff, commands, steps and the four buttons.
PANEL_OUTPUTS = 9


@_timed
def show_review(incident_id):
    view = views.review(get_store(), get_client(), incident_id)
    pending = view["pending"]

    return (view["evidence"], view["proposal"], view["diff"], view["commands"],
            gr.Code(value=view["steps"], interactive=pending),
            gr.Button(interactive=view["can_approve"]),
            gr.Button(interactive=pending), gr.Button(interactive=pending),
            gr.Button(interactive=view["can_diagnose"]))


@_timed
def select_review(incident_id):
    return show_review(incident_id) + (views.review_key(get_store(), incident_id),)


@_timed
def follow_review(incident_id, seen):
    """Re-render the panel only when it would change (an approval being applied finishes)."""

    key = views.review_key(get_store(), incident_id)

    if key == seen:
        return (gr.skip(),) * (PANEL_OUTPUTS + 1)

    return show_review(incident_id) + (key,)


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

    return ((message,) + show_review(incident_id) +
            (_queue_at(incident_id), views.review_key(get_store(), incident_id)))


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
def diagnose(incident_id, shown=None):
    return _answer(_shows_another(incident_id, shown) or views.diagnose(incident_id), incident_id)


@_timed
def refresh_history(selected):
    store = get_store()
    choices = views.history_choices(store)
    values = [value for _, value in choices]
    selected = selected if selected in values else (values[0] if values else None)

    return views.history_rows(store), gr.Dropdown(choices=choices, value=selected)


@_timed
def show_incident(incident_id):
    return views.incident_detail(get_store(), incident_id)


@_timed
def refresh_system():
    return views.system(_snapshot(), get_client())


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


def build() -> gr.Blocks:
    with gr.Blocks(title="NiFi Self-Healing") as demo:
        gr.Markdown("## NiFi Self-Healing Pipeline\n"
                    f"CDF DataFlow · {'mock tenant' if config.MOCK else 'live tenant'} · writes "
                    f"{'**ARMED**' if config.ARMED else 'disarmed (dry runs)'}")

        with gr.Tab("Fleet"):
            fleet_header = gr.Markdown()
            fleet_table = gr.Dataframe(headers=views.FLEET_HEADERS, interactive=False, wrap=True,
                                       max_height=520)

            with gr.Row():
                kpi_pick = gr.Dropdown(label="KPI chart", choices=[], scale=3, allow_custom_value=True)

            kpi_plot = gr.LinePlot(x="time", y="value", color="series", height=300, label="KPI")
            fleet_timer = gr.Timer(1.0)
            choices_timer = gr.Timer(15.0)
            fleet_timer.tick(refresh_fleet, inputs=[kpi_pick], outputs=[fleet_header, fleet_table, kpi_plot],
                             show_progress="hidden")
            choices_timer.tick(refresh_kpi_choices, inputs=[kpi_pick], outputs=[kpi_pick],
                               show_progress="hidden")
            kpi_pick.change(refresh_fleet, inputs=[kpi_pick], outputs=[fleet_header, fleet_table, kpi_plot])

        with gr.Tab("Review Queue"):
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

            review_result = gr.Markdown()
            panel = [evidence, proposal, diff, commands, steps, approve_btn, edit_btn, reject_btn, diagnose_btn]
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
            diagnose_btn.click(diagnose, inputs=[queue_pick, seen], outputs=decision_outputs)

        with gr.Tab("History"):
            history_refresh = gr.Button("Refresh")
            history_table = gr.Dataframe(headers=views.HISTORY_HEADERS, interactive=False, wrap=True,
                                         max_height=420)
            history_pick = gr.Dropdown(label="Incident", choices=[], allow_custom_value=True)
            history_detail = gr.Markdown()

            history_refresh.click(refresh_history, inputs=[history_pick],
                                  outputs=[history_table, history_pick])
            history_pick.change(show_incident, inputs=[history_pick], outputs=[history_detail])

        with gr.Tab("System"):
            system_md = gr.Markdown()

            with gr.Row():
                system_refresh = gr.Button("Refresh")
                poll_btn = gr.Button("Poll now")
                selftest_btn = gr.Button("Selftest")

            with gr.Row(visible=config.MOCK):
                minutes = gr.Slider(1, 120, value=20, step=1, label="Fast-forward the mock clock (minutes)")
                forward_btn = gr.Button("Fast-forward")

            system_result = gr.Markdown()
            selftest_out = gr.JSON(label="Selftest")

            system_refresh.click(refresh_system, outputs=[system_md])
            poll_btn.click(poll_now, outputs=[system_result]).then(refresh_system, outputs=[system_md])
            selftest_btn.click(selftest, outputs=[selftest_out])
            forward_btn.click(fast_forward, inputs=[minutes], outputs=[system_result]).then(
                refresh_system, outputs=[system_md])

        # Choices are filled per session; allow_custom_value keeps a pick valid
        # between refreshes (and for API callers). Handlers check the ids.
        demo.load(refresh_kpi_choices, inputs=[kpi_pick], outputs=[kpi_pick])
        demo.load(who_is_reviewing, outputs=[reviewer])
        demo.load(refresh_queue, inputs=[queue_pick], outputs=[queue_pick]).then(
            select_review, inputs=[queue_pick], outputs=panel + [seen])
        demo.load(refresh_history, inputs=[history_pick], outputs=[history_table, history_pick])
        demo.load(refresh_system, outputs=[system_md])

    return demo


def main() -> None:
    startup()
    build().queue(default_concurrency_limit=4).launch(
        server_name=config.APP_HOST, server_port=config.APP_PORT, show_error=True,
        theme=gr.themes.Soft())
