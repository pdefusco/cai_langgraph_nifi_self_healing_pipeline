"""
What the UI shows, as plain data: table rows, markdown, chart frames.
No Gradio here, so every view is testable without a browser; the
reviewer's decisions also go through here (decide, diagnose) so the
UI cannot reach the graph any other way.

Times are tenant time (client.now()): in mock mode the clock runs
MOCK_SPEED times faster than the wall, and fast-forward moves it.
"""

import json
import time
from datetime import datetime, timezone

import config
from nifi_healer import llm
from nifi_healer.agent import graph
from nifi_healer.kpi.detectors import group_name
from nifi_healer.store.db import CLOSED_STATUSES, get_store

FLEET_HEADERS = ["Pack", "Process group", "KPI", "Status", "Latest", "Threshold", "Trend /min", "Incident"]
DIFF_HEADERS = ["Path", "Before", "After"]
HISTORY_HEADERS = ["Incident", "Pack", "Process group", "Status", "Verdict", "Opened", "Action", "Reviewed by"]

QUEUE_STATUSES = ("PENDING_REVIEW", "NEW", "DIAGNOSING")

STATUS_MARK = {"ok": "ok", "breaching": "BREACHING", "warning": "warning", "imbalance": "IMBALANCE",
               "alert": "ALERT", "missing": "no chart"}


def clock(t: float | None) -> str:
    if not t:
        return ""

    return datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def ago(t: float | None, now: float) -> str:
    if not t:
        return "never"

    seconds = max(0, now - t)

    if seconds < 90:
        return f"{seconds:.0f}s ago"

    if seconds < 5400:
        return f"{seconds / 60:.0f}m ago"

    return f"{seconds / 3600:.1f}h ago"


def _number(value) -> str:
    if value is None:
        return ""

    if abs(value) >= 100 or float(value).is_integer():
        return f"{value:,.0f}"

    return f"{value:,.3g}"


def _threshold(summary: dict) -> str:
    if summary.get("above") is not None:
        return f"> {_number(summary['above'])}"

    if summary.get("below") is not None:
        return f"< {_number(summary['below'])}"

    return ""


# ---------------------------------------------------------------------
# Fleet
# ---------------------------------------------------------------------


def fleet_header(snapshot: dict) -> str:
    beat = snapshot["heartbeat"]
    now = snapshot["now"]
    packs = snapshot["packs"]
    breaching = sum(1 for view in packs.values() for k in (view.get("kpis") or {}).values()
                    if k.get("status") in ("breaching", "imbalance"))
    polled = [view.get("kpi_wall") for view in packs.values() if view.get("kpi_wall")]
    freshest = f"{time.time() - max(polled):.0f}s ago" if polled else "not yet"
    poller = "running" if beat.get("running") else "**stopped**"

    lines = [f"**{len(packs)}** deployment(s) · **{breaching}** KPI(s) breaching · poller {poller}, "
             f"last KPI read {freshest} · tenant time {clock(now)}"]

    paused = [f"{name}: {view['detection_paused']}" for name, view in packs.items()
              if view.get("detection_paused")]

    if paused:
        lines.append("Detection paused for " + "; ".join(paused))

    if beat.get("last_error"):
        lines.append(f"Last poller error: `{beat['last_error']}`")

    return "\n\n".join(lines)


def fleet_rows(snapshot: dict, store) -> list:
    open_by_group = {(i["pack"], i["process_group"]): i for i in store.incidents()
                     if i["status"] not in CLOSED_STATUSES}
    rows = []

    for name in sorted(snapshot["packs"]):
        view = snapshot["packs"][name]

        for key, summary in sorted((view.get("kpis") or {}).items(),
                                   key=lambda item: (item[1].get("scenario") or "", item[0])):
            group = group_name(summary["scenario"]) if summary.get("scenario") else ""
            incident = open_by_group.get((name, group))
            rows.append([
                name, group, f"{summary.get('component')} · {summary.get('metric')}",
                STATUS_MARK.get(summary.get("status"), summary.get("status") or ""),
                _number(summary.get("latest")), _threshold(summary),
                f"{summary.get('trend_per_min') or 0:+,.1f}",
                f"{incident['id']} ({incident['status']})" if incident else "",
            ])

    return rows


def kpi_choices(snapshot: dict) -> list:
    """[(label, "pack|kpi key")] for the chart picker, breaching first."""

    choices = []

    for name in sorted(snapshot["packs"]):
        for key, summary in (snapshot["packs"][name].get("kpis") or {}).items():
            label = f"{name} · {summary.get('component')} · {summary.get('metric')}"
            choices.append((summary.get("status") == "ok", label, f"{name}|{key}"))

    return [(label, value) for _, label, value in sorted(choices)]


def kpi_frame(snapshot: dict, choice: str | None):
    """The chosen KPI's series and threshold, for gr.LinePlot."""

    import pandas as pd

    empty = pd.DataFrame({"time": pd.to_datetime([], utc=True), "value": [], "series": []})

    if not choice or "|" not in choice:
        return empty

    name, key = choice.split("|", 1)
    view = snapshot["packs"].get(name) or {}
    points = (view.get("series") or {}).get(key) or []
    summary = (view.get("kpis") or {}).get(key) or {}

    if not points:
        return empty

    times = [datetime.fromtimestamp(t, tz=timezone.utc) for t, _ in points]
    frame = pd.DataFrame({"time": times, "value": [v for _, v in points],
                          "series": summary.get("metric") or "value"})
    line = summary.get("above") if summary.get("above") is not None else summary.get("below")

    if line is not None:
        frame = pd.concat([frame, pd.DataFrame({"time": times, "value": [float(line)] * len(times),
                                                "series": "threshold"})])

    return frame


# ---------------------------------------------------------------------
# Review queue
# ---------------------------------------------------------------------


def queue_choices(store) -> list:
    choices = []

    for incident in store.incidents(statuses=QUEUE_STATUSES):
        waiting = "awaiting review" if incident["status"] == "PENDING_REVIEW" else incident["status"].lower()
        choices.append((f"{incident['id']} · {incident['process_group']} · {waiting}", incident["id"]))

    return choices


def _latest_proposal(store, incident_id: str) -> dict | None:
    rows = [p for p in store.proposals(incident_id) if p["status"] != "SUPERSEDED"]

    return rows[-1] if rows else None


def review(store, client, incident_id: str | None) -> dict:
    """
    Everything the review panel shows for one incident:
    {evidence, proposal, diff, commands, steps, can_approve, pending, status}.
    """

    blank = {"evidence": "Select an incident.", "proposal": "", "diff": [], "commands": "",
             "steps": "", "can_approve": False, "pending": False, "status": ""}
    incident = store.incident(incident_id) if incident_id else None

    if not incident:
        return blank

    now = client.now()
    payload = graph.pending(incident_id)
    signals = incident.get("signals") or []
    evidence = [f"### {incident['id']}",
                f"**{incident['pack']}** · process group **{incident['process_group']}** · "
                f"status **{incident['status']}** · opened {ago(incident['opened_at'], now)}",
                "", f"> {incident.get('summary') or ''}", "", "**Signals**"]
    evidence += [f"- `{s.get('severity')}` {s.get('summary')}" for s in signals] or ["- none"]

    if payload:
        plan, proposal = payload.get("plan") or {}, payload.get("proposal") or {}
        playbooks = {h["id"]: h.get("heading") for h in payload.get("playbooks") or []}

    else:
        row = _latest_proposal(store, incident_id)
        plan = (row or {}).get("validation") or {}
        proposal = {"diagnosis": (row or {}).get("diagnosis"), "rationale": (row or {}).get("rationale"),
                    "citations": (row or {}).get("citations") or [], "steps": (row or {}).get("action") or []}
        playbooks = {}

    if not proposal.get("diagnosis") and not proposal.get("steps"):
        waiting = {"NEW": "Not diagnosed yet. Diagnose runs the agent (needs the LLM endpoint).",
                   "DIAGNOSING": "The agent is diagnosing this incident."}

        return dict(blank, evidence="\n".join(evidence),
                    proposal=waiting.get(incident["status"], "No proposal."), status=incident["status"])

    text = ["### Proposal",
            f"**Diagnosis.** {proposal.get('diagnosis') or ''}"]

    if proposal.get("root_cause_component"):
        text.append(f"**Root cause:** `{proposal['root_cause_component']}`")

    text += [f"**Rationale.** {proposal.get('rationale') or ''}"]

    if proposal.get("expected_effect"):
        text.append(f"**Expected effect.** {proposal['expected_effect']}")

    if proposal.get("confidence") is not None:
        text.append(f"**Confidence:** {proposal['confidence']:.2f}")

    citations = proposal.get("citations") or []

    if citations:
        text.append("**Playbooks cited:** " + "; ".join(
            f"`{c}`" + (f" ({playbooks[c]})" if playbooks.get(c) else "") for c in citations))

    family = plan.get("family") or "?"
    text.append(f"**Action family:** {family} · **risk:** {plan.get('risk') or '?'} · "
                f"**writes:** {'ARMED: approval changes the tenant' if config.ARMED else 'disarmed: approval is a dry run'}")

    if plan.get("problems"):
        text.append("**Validation failed:**\n" + "\n".join(f"- {p}" for p in plan["problems"]))

    if plan.get("warnings"):
        text.append("**Warnings:**\n" + "\n".join(f"- {w}" for w in plan["warnings"]))

    if payload and payload.get("error"):
        text.append(f"**Last decision not accepted:** {payload['error']}")

    diff = [[d.get("path"), json.dumps(d.get("before"), default=str), json.dumps(d.get("after"), default=str)]
            for d in plan.get("diff") or []]

    return {
        "evidence": "\n".join(evidence),
        "proposal": "\n\n".join(text),
        "diff": diff,
        "commands": "\n\n".join(plan.get("commands") or []),
        "steps": json.dumps(proposal.get("steps") or [], indent=2),
        "can_approve": bool(payload and plan.get("ok")),
        "pending": bool(payload),
        "status": incident["status"],
    }


def decide(incident_id: str | None, decision: str, reviewer: str = "", reason: str = "",
           steps_text: str = "") -> str:
    """Send a reviewer's decision into the graph. Returns a line for the UI."""

    if not incident_id:
        return "Select an incident first."

    if not graph.pending(incident_id):
        return f"{incident_id} is not waiting for a review."

    answer = {"decision": decision, "reviewer": reviewer.strip(), "reason": reason.strip()}

    if decision == "edit":
        try:
            steps = json.loads(steps_text or "[]")

        except json.JSONDecodeError as exc:
            return f"The steps are not valid JSON: {exc}"

        if isinstance(steps, dict):
            steps = [steps]

        answer["steps"] = steps

    state = graph.resume(incident_id, answer)
    waiting = graph.pending(incident_id)

    if waiting and waiting.get("error"):
        return f"Not accepted: {waiting['error']}"

    if decision == "edit":
        plan = (waiting or {}).get("plan") or {}

        return ("Edited steps validated; review them and approve." if plan.get("ok")
                else "Edited steps failed validation: " + "; ".join(plan.get("problems") or []))

    if decision == "reject":
        return f"{incident_id} rejected."

    result = state.get("result") or {}

    if result.get("status") == "DRY_RUN":
        return f"Dry run recorded (writes are disarmed): {len(result.get('argv') or [])} command(s), see History."

    if result.get("status") == "APPLIED":
        return f"Applied; verifying the KPIs over the next {config.VERIFY_WINDOW_SECONDS // 60} minutes."

    return f"{result.get('status') or 'Done'}: {result.get('error') or ''}".strip()


def diagnose(incident_id: str | None) -> str:
    if not incident_id:
        return "Select an incident first."

    ok, reason = llm.chat_available()

    if not ok:
        return f"The LLM is not available: {reason}"

    incident = get_store().incident(incident_id)

    if not incident or incident["status"] not in graph.RUNNABLE:
        return f"{incident_id} is {incident['status'] if incident else 'unknown'}; nothing to diagnose."

    graph.submit(incident)

    return f"Diagnosing {incident_id}; it appears here when the proposal is ready."


# ---------------------------------------------------------------------
# History
# ---------------------------------------------------------------------


def history_rows(store, limit: int = 200) -> list:
    rows = []

    for incident in store.incidents(limit=limit):
        actions = store.actions(incident_id=incident["id"])
        reviews = store.reviews(incident["id"])
        last = actions[-1] if actions else None
        rows.append([
            incident["id"], incident["pack"], incident["process_group"], incident["status"],
            incident.get("verdict") or "", clock(incident["opened_at"]),
            f"{last['action_type']} ({last['status']})" if last else "",
            ", ".join(sorted({r["reviewer"] or "?" for r in reviews})),
        ])

    return rows


def history_choices(store, limit: int = 200) -> list:
    return [(f"{i['id']} · {i['status']}", i["id"]) for i in store.incidents(limit=limit)]


def incident_detail(store, incident_id: str | None) -> str:
    incident = store.incident(incident_id) if incident_id else None

    if not incident:
        return "Select an incident."

    out = [f"### {incident['id']}",
           f"{incident['pack']} · {incident['process_group']} · **{incident['status']}**"
           + (f" · verdict **{incident['verdict']}**" if incident.get("verdict") else ""),
           f"Opened {clock(incident['opened_at'])}"
           + (f", closed {clock(incident['closed_at'])}" if incident.get("closed_at") else ""),
           "", f"> {incident.get('summary') or ''}"]

    for p in store.proposals(incident["id"]):
        out += ["", f"**Proposal {p['id']}** ({p['status']}, attempt {p.get('attempt')}, "
                    f"`{p.get('agent_version') or ''}`)",
                f"{p.get('diagnosis') or ''}", f"```json\n{json.dumps(p.get('action'), indent=2)}\n```"]

        problems = (p.get("validation") or {}).get("problems") or []

        if problems:
            out.append("Problems: " + "; ".join(problems))

    for r in store.reviews(incident["id"]):
        out.append(f"- **Review:** {r['decision']} by {r['reviewer'] or '?'} at {clock(r['created_at'])}"
                   + (f": {r['reason']}" if r.get("reason") else ""))

    for a in store.actions(incident_id=incident["id"]):
        commands = "\n".join("cdp " + " ".join(str(x) for x in argv) for argv in a.get("argv") or [])
        out += ["", f"**Action {a['id']}** {a['action_type']} → **{a['status']}**"
                    + (" (dry run)" if a["dry_run"] else "")
                    + (f": {a['error']}" if a.get("error") else ""),
                f"```shell\n{commands or '(no commands)'}\n```"]

    for o in store.outcomes(incident["id"]):
        details = o.get("details") or {}
        out += ["", f"**Outcome** for action {o['action_id']}: **{o['verdict']}** ({o.get('period')})"]
        out += [f"- `{k.get('kpi')}`: {k.get('verdict')}, {k.get('reason') or ''}"
                for k in details.get("kpis") or []]

        if details.get("reason"):
            out.append(f"- {details['reason']}")

    return "\n".join(out)


# ---------------------------------------------------------------------
# System
# ---------------------------------------------------------------------


def system(snapshot: dict, client) -> str:
    beat = snapshot["heartbeat"]
    ok, reason = llm.chat_available()
    described = llm.describe()

    rows = [
        ("DataFlow client", f"{client.kind}" + (" (simulated tenant)" if config.MOCK else "")),
        ("Writes", "**ARMED**: approvals change the tenant" if config.ARMED
         else "disarmed: approvals are recorded as dry runs (set SELFHEAL_ARMED=true in the project env)"),
        ("Service CRN", f"`{config.SERVICE_CRN or 'unset'}`"),
        ("Environment CRN", f"`{config.ENVIRONMENT_CRN or 'unset'}`"),
        ("LLM", f"{described['provider']} · `{described['model'] or 'unset'}` · "
                + ("ready" if ok else f"**unavailable**: {reason}")),
        ("Embeddings", f"`{described['embedder']}`"),
        ("Poller", f"{'running' if beat.get('running') else 'stopped'}, {beat.get('loops', 0)} loop(s); "
                   f"health every {config.POLL_HEALTH_SECONDS}s, KPIs every {config.POLL_KPI_SECONDS}s"),
        ("Last poller error", f"`{beat['last_error']}`" if beat.get("last_error") else "none"),
        ("Agent", f"{config.AGENT_WORKERS} worker(s), auto-diagnose "
                  f"{'on' if config.AUTO_DIAGNOSE else 'off'}"),
        ("Guardrails", f"cooldown {config.COOLDOWN_SECONDS // 60} min, at most "
                       f"{config.MAX_ACTIONS_PER_WINDOW} action(s) per {config.ACTION_WINDOW_SECONDS // 3600} h "
                       f"per deployment, prefix `{config.PREFIX}`"),
        ("Verify", f"settle {config.VERIFY_SETTLE_SECONDS}s, window {config.VERIFY_WINDOW_SECONDS}s"),
        ("Data", f"`{config.DB_PATH}`, checkpoints `{config.CHECKPOINT_PATH}`, Chroma `{config.CHROMA_DIR}`"),
    ]

    if config.MOCK:
        rows.append(("Mock clock", f"{config.MOCK_SPEED:g}x wall time, tenant time {clock(snapshot['now'])}"))

    return "| | |\n|---|---|\n" + "\n".join(f"| {k} | {v} |" for k, v in rows)
