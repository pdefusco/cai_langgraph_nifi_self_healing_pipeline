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
from nifi_healer.actions import executor, rollback
from nifi_healer.agent import graph
from nifi_healer.cdp.client import get_client
from nifi_healer.kpi import rollup
from nifi_healer.kpi.detectors import group_name
from nifi_healer.store.db import CLOSED_STATUSES, get_store

DEPLOYMENT_HEADERS = ["Pack", "State", "Flow", "CPU %", "Memory %", "Cores", "Active alerts",
                      "Groups not ok", "Open incidents", "KPIs read"]
GROUP_HEADERS = ["Pack", "Process group", "Status", "In", "Out", "Out / in", "Queued (watched)", "Incident"]
FLEET_HEADERS = ["Pack", "Process group", "KPI", "Status", "Latest", "Threshold", "Trend /min", "Incident"]
DIFF_HEADERS = ["Path", "Before", "After"]
HISTORY_HEADERS = ["Incident", "Pack", "Process group", "Status", "Verdict", "Windows", "Opened", "Action",
                   "Reviewed by", "Change"]
OUTCOME_HEADERS = ["KPI", "Decides", "Before (mean · last)", "After (mean · last)", "Change (last)",
                   "Trend /min", "Verdict", "Why"]

QUEUE_STATUSES = ("PENDING_REVIEW", "NEW", "DIAGNOSING", "APPLYING")

STATUS_MARK = {"ok": "ok", "breaching": "BREACHING", "warning": "warning", "imbalance": "IMBALANCE",
               "alert": "ALERT", "missing": "no chart"}

# A group shows its worst KPI's status.
_SEVERITY = {"breaching": 4, "imbalance": 4, "alert": 4, "warning": 3, "missing": 1, "ok": 0}


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


def _percent(ratio) -> str:
    return "—" if ratio is None else f"{ratio:.0%}"


def change(before, after, good: str = "down") -> str:
    """
    "417 → 0 (−100%, better)", judged in the KPI's good direction. An
    outcome's change is from the last bucket before the fix to the last of
    the window after it: the verdict reads slopes, so a draining backlog's
    means can match while its level falls.
    """

    if before is None or after is None:
        return "—"

    text = f"{_number(before)} → {_number(after)}"

    if abs(before) < 1e-9:
        return text if abs(after) < 1e-9 else f"{text} (from 0)"

    delta = (after - before) / abs(before)

    if abs(delta) < 0.005:
        return f"{text} (no change)"

    better = (delta < 0) == (good == "down")

    return f"{text} ({delta:+.0%}, {'better' if better else 'worse'})".replace("-", "−")


def _component(kpi_key: str) -> str:
    """ "s17-requests" from "S17:connection:s17-requests:queued_count"; the metric for a system KPI."""

    parts = (kpi_key or "").split(":")

    return (parts[2] or parts[3]) if len(parts) == 4 else kpi_key


def _deciding(details: dict) -> dict | None:
    """The deciding KPI that carries the verdict, else the first deciding one."""

    judged = details.get("kpis") or []
    deciding = [k for k in judged if k.get("primary") or k.get("signalled")] or judged

    return next((k for k in deciding if k.get("verdict") == details.get("verdict")),
                deciding[0] if deciding else None)


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


def _open_incidents(store) -> dict:
    return {(i["pack"], i["process_group"]): i for i in store.incidents() if i["status"] not in CLOSED_STATUSES}


def _alerts(facts: dict) -> str:
    counts = facts.get("alerts") or {}
    shown = [f"{counts[level]} {level}" for level in ("error", "warning", "info") if counts.get(level)]

    return ", ".join(shown) or "none"


def _system(summary: dict | None, suffix: str = "") -> str:
    """ "3.5 (30m mean 3.2, +0.1/min)" from a summarized system chart."""

    if not summary or summary.get("latest") is None:
        return "—"

    trend = summary.get("trend_per_min") or 0

    return (f"{_number(summary['latest'])}{suffix} (mean {_number(summary['mean'])}"
            + (f", {trend:+.2g}/min" if abs(trend) >= 0.01 else "") + ")")


def deployment_rows(snapshot: dict, store) -> list:
    """One row per deployment: its health and size, from the charts every deployment has."""

    incidents = _open_incidents(store)
    rows = []

    for name in sorted(snapshot["packs"]):
        view = snapshot["packs"][name]
        facts, flow, system = view.get("facts") or {}, view.get("flow") or {}, view.get("system") or {}
        statuses = {}

        for summary in (view.get("kpis") or {}).values():
            if summary.get("scenario"):
                worst = statuses.get(summary["scenario"], "ok")
                status = summary.get("status") or "ok"
                statuses[summary["scenario"]] = max(worst, status, key=lambda s: _SEVERITY.get(s, 2))

        cores = (system.get("cores") or {}).get("latest")
        rows.append([
            name, facts.get("state") or "", f"v{flow['flow_version']}" if flow.get("flow_version") else "",
            _system(system.get("cpu")), _system(system.get("memory")),
            _number(cores) if cores is not None else "—", _alerts(facts),
            f"{sum(1 for s in statuses.values() if s != 'ok')} of {len(statuses)}",
            str(sum(1 for pack, _ in incidents if pack == name)),
            ago(view.get("kpi_at"), snapshot["now"]),
        ])

    return rows


def group_rows(snapshot: dict, store) -> list:
    """
    One row per process group: its worst KPI, what its entries send on
    against what its exits receive (kpi/rollup.py), and what its watched
    queues hold.
    """

    incidents = _open_incidents(store)
    rows = []

    for name in sorted(snapshot["packs"]):
        view = snapshot["packs"][name]
        by_scenario = {}

        for summary in (view.get("kpis") or {}).values():
            if summary.get("scenario"):
                by_scenario.setdefault(summary["scenario"], []).append(summary)

        for scenario in sorted(by_scenario):
            summaries = by_scenario[scenario]
            status = max((s.get("status") or "ok" for s in summaries if not s.get("rollup")),
                         key=lambda s: _SEVERITY.get(s, 2), default="ok")
            flow = (view.get("groups") or {}).get(scenario) or {}
            queued = [s["latest"] for s in summaries
                      if s.get("metric") == "queued_count" and s.get("latest") is not None]
            group = group_name(scenario)
            incident = incidents.get((name, group))
            rows.append([
                name, group, STATUS_MARK.get(status, status),
                _number(flow["in"][-1][1]) if flow.get("in") else "—",
                _number(flow["out"][-1][1]) if flow.get("out") else "—",
                _percent(rollup.ratio(flow)) if flow.get("in") and flow.get("out") else "—",
                _number(sum(queued)) if queued else "—",
                f"{incident['id']} ({incident['status']})" if incident else "",
            ])

    return rows


def fleet_rows(snapshot: dict, store) -> list:
    """One row per KPI that alerts or decides; the roll-up's edge charts are in group_rows."""

    open_by_group = _open_incidents(store)
    rows = []

    for name in sorted(snapshot["packs"]):
        view = snapshot["packs"][name]

        for key, summary in sorted(((k, s) for k, s in (view.get("kpis") or {}).items() if not s.get("rollup")),
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
    """
    [(label, "pack|kpi key")] for the chart picker, breaching first, then
    each group's in vs out ("pack|group:S01").
    """

    choices = []

    for name in sorted(snapshot["packs"]):
        view = snapshot["packs"][name]

        for key, summary in (view.get("kpis") or {}).items():
            label = f"{name} · {summary.get('component')} · {summary.get('metric')}"
            choices.append((summary.get("status") == "ok", label, f"{name}|{key}"))

        for scenario, flow in sorted((view.get("groups") or {}).items()):
            if flow.get("in") and flow.get("out"):
                choices.append((True, f"{name} · {group_name(scenario)} · in vs out", f"{name}|group:{scenario}"))

    return [(label, value) for _, label, value in sorted(choices)]


def _group_frame(flow: dict):
    import pandas as pd

    frames = [pd.DataFrame({"time": [datetime.fromtimestamp(t, tz=timezone.utc) for t, _ in flow[side]],
                            "value": [v for _, v in flow[side]],
                            "series": f"{side} ({', '.join(flow['edges'][side])})"})
              for side in ("in", "out")]

    return pd.concat(frames)


def kpi_frame(snapshot: dict, choice: str | None):
    """The chosen KPI's series and threshold, for gr.LinePlot."""

    import pandas as pd

    empty = pd.DataFrame({"time": pd.to_datetime([], utc=True), "value": [], "series": []})

    if not choice or "|" not in choice:
        return empty

    name, key = choice.split("|", 1)
    view = snapshot["packs"].get(name) or {}

    if key.startswith("group:"):
        flow = (view.get("groups") or {}).get(key[len("group:"):]) or {}

        return _group_frame(flow) if flow.get("in") and flow.get("out") else empty

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


def queue_choices(store, keep: str | None = None) -> list:
    """The open incidents, plus `keep` (the one on screen) once it closes, so the view stays on it."""

    incidents = store.incidents(statuses=QUEUE_STATUSES)

    if keep and keep not in {i["id"] for i in incidents}:
        incidents += [i for i in [store.incident(keep)] if i]

    choices = []

    for incident in incidents:
        waiting = "awaiting review" if incident["status"] == "PENDING_REVIEW" else incident["status"].lower()
        choices.append((f"{incident['id']} · {incident['process_group']} · {waiting}", incident["id"]))

    return choices


def review_key(store, incident_id: str | None) -> str:
    """Changes whenever the review panel for the incident would show something new."""

    incident = store.incident(incident_id) if incident_id else None

    return f"{incident_id}|{incident and incident['status']}|{graph.run_state(incident_id) if incident else None}"


def _latest_proposal(store, incident_id: str) -> dict | None:
    rows = [p for p in store.proposals(incident_id) if p["status"] != "SUPERSEDED"]

    return rows[-1] if rows else None


APPLYING = ("**Applying the approved change to the tenant.** A flow edit imports a new flow version and "
            "deploys it, which takes several minutes; this panel updates when it is done, and History "
            "has the commands.")


def _last_action(store, incident_id: str) -> str:
    actions = store.actions(incident_id=incident_id)

    if not actions:
        return ""

    action = actions[-1]
    error = f": {action['error']}" if action.get("error") else ""

    return f"**Last action:** {action['action_type']} **{action['status']}**{error}"


ORPHANED = ("**This incident is marked for review, but its run has no paused review:** saving the "
            "review point failed (the database was locked). Diagnose picks the run up again; so does "
            "an app restart.")


def stale(store, incident: dict) -> str:
    """
    Why a paused review's proposal is out of date, or "": it was diagnosed
    on a flow version the deployment no longer runs, and not because this
    app's own approvals moved it (an approval plans across those). The
    deployed version is the poller's last read, so this costs no cdp call.
    """

    deployed = store.deployment(incident["deployment_crn"]) or {}

    if not executor.stale(store, incident, deployed.get("flow_version_crn") or ""):
        return ""

    versions = {v["crn"]: v["version"] for v in store.flow_versions(incident["pack"])}
    was = versions.get(incident.get("expected_flow_version_crn"))

    return (f"**Stale.** This was diagnosed on flow v{was or '?'}, and the deployment now runs "
            f"v{deployed.get('flow_version') or '?'}, which this app did not deploy: the proposal reads a "
            "flow that is no longer running, and a flow edit planned on it is refused. **Diagnose** "
            "replaces it with a new diagnosis of the running version.")


def review(store, client, incident_id: str | None) -> dict:
    """
    Everything the review panel shows for one incident:
    {evidence, proposal, diff, commands, steps, can_approve, can_diagnose,
    pending, status}.
    """

    blank = {"evidence": "Select an incident.", "proposal": "", "diff": [], "commands": "",
             "steps": "", "can_approve": False, "can_diagnose": False, "pending": False, "status": ""}
    incident = store.incident(incident_id) if incident_id else None

    if not incident:
        return blank

    now = client.now()
    payload = graph.pending(incident_id)
    orphaned = incident["status"] == "PENDING_REVIEW" and not payload
    signals = incident.get("signals") or []
    evidence = [f"### {incident['id']}",
                f"**{incident['pack']}** · process group **{incident['process_group']}** · "
                f"status **{incident['status']}** · opened {ago(incident['opened_at'], now)}",
                "", f"> {incident.get('summary') or ''}", "", "**Signals**"]
    evidence += [f"- `{s.get('severity')}` {s.get('summary')}" for s in signals] or ["- none"]

    if payload:
        plan, proposal = payload.get("plan") or {}, payload.get("proposal") or {}
        playbooks = {h["id"]: h.get("heading") for h in payload.get("playbooks") or []}
        cases = {h["id"]: h.get("headline") for h in payload.get("memory") or []}
        playbooks.update(cases)

    else:
        row = _latest_proposal(store, incident_id)
        plan = (row or {}).get("validation") or {}
        proposal = {"diagnosis": (row or {}).get("diagnosis"), "rationale": (row or {}).get("rationale"),
                    "citations": (row or {}).get("citations") or [], "steps": (row or {}).get("action") or []}
        playbooks, cases = {}, {}

    run = graph.run_state(incident_id)

    if not payload and not proposal.get("diagnosis") and not proposal.get("steps"):
        waiting = {"NEW": "Not diagnosed yet. Diagnose runs the agent (needs the LLM endpoint).",
                   "DIAGNOSING": "The agent is diagnosing this incident."}
        text = waiting.get(incident["status"], "No proposal.")
        stopped = graph.state(incident_id).get("error") if incident["status"] == "NEW" else ""

        if run and run[0] == "running":
            text = "The agent is diagnosing this incident; the proposal appears here when it is ready."

        elif run:
            text = (f"Queued for diagnosis: **{run[1]}** of {run[2]} waiting, "
                    f"{config.AGENT_WORKERS} diagnosed at a time.")

        elif stopped:
            text = (f"**The last run stopped:** {stopped[:400]}\n\n"
                    "Diagnose runs the agent again; so does an app restart.")

        elif orphaned:
            text = ORPHANED

        return dict(blank, evidence="\n".join(evidence), proposal=text, status=incident["status"],
                    can_diagnose=(incident["status"] in graph.RUNNABLE or orphaned) and run is None)

    if proposal.get("parse_error"):
        text = ["### No usable proposal",
                f"The agent's last reply could not be read as a proposal: {proposal['parse_error']}",
                "Write the steps under **Steps** and **Submit edit**, or **Reject** with a reason.",
                "**The agent's last reply:**",
                "```\n" + (proposal.get("rationale") or "(empty)").replace("```", "'''") + "\n```"]

    else:
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

    if cases:
        text.append("**Past cases shown to the agent:** " + "; ".join(
            f"`{c}` ({headline})" for c, headline in cases.items()))

    family = plan.get("family") or "?"
    text.append(f"**Action family:** {family} · **risk:** {plan.get('risk') or '?'} · "
                f"**writes:** {'ARMED: approval changes the tenant' if config.ARMED else 'disarmed: approval is a dry run'}")

    if plan.get("problems") and not proposal.get("parse_error"):
        text.append("**Validation failed:**\n" + "\n".join(f"- {p}" for p in plan["problems"]))

    if plan.get("warnings"):
        text.append("**Warnings:**\n" + "\n".join(f"- {w}" for w in plan["warnings"]))

    if payload and payload.get("error"):
        text.append(f"**Last decision not accepted:** {payload['error']}")

    if payload and graph.apply_failure(incident_id):
        text.append(f"**The last approval failed before it changed anything:** "
                    f"{graph.apply_failure(incident_id)[:400]}\n\nThe review is still open; approve again "
                    "once the cause is fixed.")

    if orphaned:
        text.append(ORPHANED)

    applying = bool(run and run[0] == "applying")
    outdated = stale(store, incident) if payload and not applying and run is None else ""

    if outdated:
        text.append(outdated)

    if applying:
        text.append(APPLYING)

    elif not payload and _last_action(store, incident_id):
        text.append(_last_action(store, incident_id))

    diff = [[d.get("path"), json.dumps(d.get("before"), default=str), json.dumps(d.get("after"), default=str)]
            for d in plan.get("diff") or []]

    return {
        "evidence": "\n".join(evidence),
        "proposal": "\n\n".join(text),
        "diff": diff,
        "commands": "\n\n".join(plan.get("commands") or []),
        "steps": json.dumps(proposal.get("steps") or [], indent=2),
        "can_approve": bool(payload and plan.get("ok")) and not applying,
        "can_diagnose": (orphaned or bool(outdated)) and run is None,
        "pending": bool(payload) and not applying,
        "status": incident["status"],
    }


def decide(incident_id: str | None, decision: str, reviewer: str = "", reason: str = "",
           steps_text: str = "") -> str:
    """Send a reviewer's decision into the graph. Returns a line for the UI."""

    if not incident_id:
        return "Select an incident first."

    if (graph.run_state(incident_id) or ("",))[0] == "applying":
        return f"{incident_id}'s approval is being applied; History shows the result."

    waiting = graph.pending(incident_id)

    if not waiting:
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

    if decision == "approve" and config.ARMED and (waiting.get("plan") or {}).get("ok"):
        refusal = executor.precheck(get_client(), get_store(), get_store().incident(incident_id))

        if refusal:
            return f"Not applied yet, {refusal} The review stays open: approve again then."

        graph.resume_in_background(incident_id, answer)

        return ("Approved; applying to the tenant in the background. This panel follows it, "
                "and History shows the result.")

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


def diagnose(incident_id: str | None, reviewer: str = "") -> str:
    if not incident_id:
        return "Select an incident first."

    ok, reason = llm.probe()

    if not ok:
        return f"The LLM is not available: {reason}"

    store = get_store()
    incident = store.incident(incident_id)
    outdated = stale(store, incident) if incident and graph.pending(incident_id) else ""

    if outdated:
        if graph.run_state(incident_id):
            return f"{incident_id} is already being worked on; this panel follows it."

        graph.submit(incident, rediagnose={"reviewer": reviewer.strip(),
                                           "reason": "stale: diagnosed on a flow version no longer deployed"})

    elif not incident or not graph.diagnosable(incident):
        return f"{incident_id} is {incident['status'] if incident else 'unknown'}; nothing to diagnose."

    else:
        graph.submit(incident)

    run = graph.run_state(incident_id)

    if run and run[0] == "queued":
        return (f"Queued {incident_id} for diagnosis, {run[1]} of {run[2]} waiting; "
                f"{config.AGENT_WORKERS} are diagnosed at a time.")

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
        found = store.outcomes(incident["id"])
        applied = [a for a in actions if a["status"] == "APPLIED" and not a["dry_run"]]
        mine = [o for o in found if applied and o["action_id"] == applied[-1]["id"]]
        outcome = shown_outcome(mine or found)
        shown = _deciding(outcome.get("details") or {}) if outcome else None
        label = store.labels(incident["id"]).get(applied[-1]["id"]) if applied else None
        verdict = " · ".join(x for x in (incident.get("verdict") or "", LABELS.get((label or {}).get("label"), ""))
                             if x)
        rows.append([
            incident["id"], incident["pack"], incident["process_group"], incident["status"],
            verdict, windows_text(mine) if applied else "", clock(incident["opened_at"]),
            f"{last['action_type']} ({last['status']})" if last else "",
            ", ".join(sorted({r["reviewer"] or "?" for r in reviews})),
            f"{_component(shown['kpi'])} "
            f"{change(shown['before'].get('latest'), shown['after'].get('latest'), shown.get('good', 'down'))}"
            if shown else "",
        ])

    return rows


def window_label(seconds) -> str:
    """900 -> `15 min`, 3600 -> `1 h`."""

    seconds = int(seconds or 0)

    return f"{seconds // 3600} h" if seconds >= 3600 and not seconds % 3600 else f"{seconds / 60:.0f} min"


def shown_outcome(found: list) -> dict | None:
    """Of one change's outcomes, the verdict it carries: its longest window judged, unless that one could not tell."""

    ordered = sorted(found, key=lambda o: o.get("window_seconds") or 0)
    clear = [o for o in ordered if o["verdict"] != "INCONCLUSIVE"]

    return (clear or ordered or [None])[-1]


def windows_text(found: list) -> str:
    """One action's verify windows: `15 min improved · 1 h no change · 12 h pending`."""

    judged = {o["window_seconds"]: o for o in found}
    parts = []

    for window in sorted(set(config.VERIFY_WINDOWS) | set(judged)):
        o = judged.get(window)
        verdict = o["verdict"].replace("_", " ").lower() if o else "pending"
        regressed = " (regressed)" if o and (o.get("details") or {}).get("regression") else ""
        parts.append(f"{window_label(window)} {verdict}{regressed}")

    return " · ".join(parts)


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

    if rollback.is_rollback(incident):
        undone = store.action(incident["rollback_of_action_id"]) or {}
        out += ["", f"**Rollback** of action {incident['rollback_of_action_id']} "
                    f"({undone.get('action_type') or '?'}) of {undone.get('incident_id') or '?'}."]

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

    labels = store.labels(incident["id"])

    for a in store.actions(incident_id=incident["id"]):
        commands = "\n".join("cdp " + " ".join(str(x) for x in argv) for argv in a.get("argv") or [])
        out += ["", f"**Action {a['id']}** {a['action_type']} → **{a['status']}**"
                    + (" (dry run)" if a["dry_run"] else "")
                    + (f": {a['error']}" if a.get("error") else ""),
                f"```shell\n{commands or '(no commands)'}\n```"]

        if a["id"] in labels:
            label = labels[a["id"]]
            out.append(f"**Reviewer's call:** {LABELS.get(label['label'], label['label'])} "
                       f"(by {label['reviewer'] or '?'} at {clock(label['created_at'])}): {label['reason']}")

        for undo in store.rollbacks_of(a["id"]):
            out.append(f"**Rollback proposed:** {undo['id']} ({undo['status']}), {clock(undo['opened_at'])}")

    superseded = store.superseded_outcomes(incident["id"])

    for o in store.outcomes(incident["id"]):
        details = o.get("details") or {}
        window = window_label(o.get("window_seconds"))
        out += ["", f"**Outcome** for action {o['action_id']}, {window} window: **{o['verdict']}**"
                    + (f" ({o['period']})" if o.get("period") else "")]

        if details.get("regression"):
            out.append("**Regression:** the incident was resolved by an earlier window; this one measured it worse.")

        if details.get("recheck"):
            again = details["recheck"]
            out.append(f"**Re-checked** by {again.get('by') or '?'} at {clock(again.get('at'))}"
                       + (f", measuring from {clock(again['from'])}" if details.get("measured_from") else "")
                       + (f"; it was {again['was']}" if again.get("was") else "") + ".")

            if again.get("skipped"):
                out.append("Skipped over: " + "; ".join(again["skipped"]) + ".")

        if details.get("started_at"):
            after = (f"from {_minutes(details.get('settle_seconds'))} after {clock(details['measured_from'])}"
                     if details.get("measured_from") else
                     f"from {_minutes(details.get('settle_seconds'))} after it finished")
            out.append(f"Before: the {_minutes(details.get('before_seconds') or details.get('window_seconds'))} "
                       f"up to {clock(details['started_at'])}. After: the {window} {after}.")

        out += outcome_table(details)

        if details.get("side_effects"):
            out.append("Side effects (worse, not deciding): "
                       + ", ".join(f"`{_component(k)}`" for k in details["side_effects"]))

        out += group_lines(details.get("group") or {})

        if details.get("reason"):
            out.append(f"- {details['reason']}")

        for old in (x for x in superseded if x["outcome_id"] == o["id"]):
            out.append(f"- Superseded: {old['verdict']}"
                       + (f" ({old['period']})" if old.get("period") else "")
                       + f", evaluated {clock(old['evaluated_at'])}"
                       + (f": {old['details']['reason']}" if (old.get("details") or {}).get("reason") else ""))

    return "\n".join(out)


# ---------------------------------------------------------------------
# Correcting an outcome: the reviewer's call, and re-checks
# ---------------------------------------------------------------------

LABELS = {"RIGHT_FIX": "right fix", "WRONG_FIX": "wrong fix", "UNCLEAR": "can't tell"}


def label_choices() -> list:
    return [(text, key) for key, text in LABELS.items()]


def window_choices() -> list:
    return [(window_label(w), w) for w in config.VERIFY_WINDOWS]


def correction_choices(store, incident_id: str | None) -> list:
    """The incident's fixes applied to the tenant, newest first: [(text, action id)]."""

    if not incident_id:
        return []

    applied = [a for a in store.actions(incident_id=incident_id) if a["status"] == "APPLIED" and not a["dry_run"]]

    return [(f"Action {a['id']} · {a['action_type']} · {clock(a['finished_at'])}", a["id"])
            for a in reversed(applied)]


def _applied(store, incident_id, action_id) -> tuple:
    """(action, "") when action_id is a fix of this incident applied to the tenant, else (None, why not)."""

    if not incident_id:
        return None, "Select an incident first."

    try:
        action = store.action(int(action_id))

    except (TypeError, ValueError):
        action = None

    if not action or action["incident_id"] != incident_id:
        return None, f"Pick one of {incident_id}'s applied actions."

    if action["status"] != "APPLIED" or action["dry_run"]:
        return None, f"Action {action['id']} was not applied to the tenant ({action['status']}), so it has no outcome."

    return action, ""


def label_fix(incident_id: str | None, action_id, label: str, reason: str, reviewer: str = "") -> str:
    """Record the reviewer's call on an applied fix; the measured verdicts stay as they are."""

    store = get_store()
    action, problem = _applied(store, incident_id, action_id)

    if problem:
        return problem

    if label not in LABELS:
        return f"Choose one of: {', '.join(LABELS.values())}."

    if not (reason or "").strip():
        return "A reason is required: what the measurement missed, or why the fix was right or wrong."

    store.add_label(incident_id, action["id"], label, reason.strip(), (reviewer or "").strip(), get_client().now())

    return f"Recorded action {action['id']} as **{LABELS[label]}**. Its measured verdicts are unchanged."


def parse_start(text: str, finished_at: float) -> float | None:
    """
    When a re-check measures from: blank for when the change finished;
    `+N` minutes after it; `HH:MM` UTC, the first such time at or after
    it; or `YYYY-MM-DD HH:MM` UTC. ValueError otherwise.
    """

    text = (text or "").strip()

    if not text:
        return None

    if text.startswith("+"):
        return finished_at + float(text[1:].strip().removesuffix("min").strip()) * 60

    # A minute is as fine as these go: the minute the change finished in
    # means when it finished.
    for form in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M"):
        try:
            found = datetime.strptime(text, form).replace(tzinfo=timezone.utc).timestamp()

            return max(found, finished_at) if found > finished_at - 60 else found

        except ValueError:
            pass

    at = datetime.strptime(text, "%H:%M")
    day = datetime.fromtimestamp(finished_at, timezone.utc)
    found = day.replace(hour=at.hour, minute=at.minute, second=0, microsecond=0).timestamp()

    return max(found, finished_at) if found > finished_at - 60 else found + 86400


def recheck(incident_id: str | None, action_id, window, start_text: str, reviewer: str = "",
            runner=None) -> str:
    """Re-measure one window of an applied fix through the poller (runner); its old verdict is kept."""

    store = get_store()
    action, problem = _applied(store, incident_id, action_id)

    if problem:
        return problem

    if runner is None:
        return "The poller is not running, so nothing can be measured."

    try:
        start = parse_start(start_text, action["finished_at"])

    except ValueError:
        return "Measure from: leave it blank, or give `+N` minutes, `HH:MM` or `YYYY-MM-DD HH:MM` (UTC)."

    try:
        window = int(window)

    except (TypeError, ValueError):
        return "Pick a window."

    outcome, why_not = runner.recheck(action["id"], window, start, (reviewer or "").strip())

    if outcome is None:
        return why_not

    was = (outcome.get("recheck") or {}).get("was")
    status = (store.incident(incident_id) or {}).get("status")

    return (f"Re-checked the {window_label(window)} window of action {action['id']}: **{outcome['verdict']}**"
            + (f" (was {was})" if was else "") + f". The incident is {status}.")


def _minutes(seconds) -> str:
    return f"{(seconds or 0) / 60:.0f} min"


def _cell(text) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ")


def outcome_table(details: dict) -> list:
    """The outcome's KPIs before and after, as markdown table lines."""

    judged = details.get("kpis") or []

    if not judged:
        return []

    lines = ["", "| " + " | ".join(OUTCOME_HEADERS) + " |", "|" + "---|" * len(OUTCOME_HEADERS)]

    for k in judged:
        before, after = k.get("before") or {}, k.get("after") or {}
        role = "primary" if k.get("primary") else "opened it" if k.get("signalled") else "no"
        parts = (k.get("kpi") or "").split(":")
        cells = [
            f"{_component(k.get('kpi'))} · {parts[3] if len(parts) == 4 else ''}", role,
            f"{_number(before.get('mean')) or '—'} · {_number(before.get('latest')) or '—'}",
            f"{_number(after.get('mean')) or '—'} · {_number(after.get('latest')) or '—'}",
            change(before.get("latest"), after.get("latest"), k.get("good", "down")),
            f"{before.get('trend_per_min') or 0:+,.1f} → {after.get('trend_per_min') or 0:+,.1f}",
            f"**{k.get('verdict')}**", k.get("reason") or "",
        ]
        lines.append("| " + " | ".join(_cell(c) for c in cells) + " |")

    return lines + [""]


def group_lines(group: dict) -> list:
    """The process group's in and out either side of the change (kpi/rollup.py)."""

    if not group:
        return []

    sides = []

    for side in ("in", "out"):
        edges = ", ".join(group.get("edges", {}).get(side) or [])
        sides.append(f"{side} ({edges}) "
                     + change(group[side]["before"].get("mean"), group[side]["after"].get("mean"), "up"))

    return [f"**Group in/out per bucket:** {'; '.join(sides)}; out / in "
            f"{_percent(group.get('ratio_before'))} → {_percent(group.get('ratio_after'))}"]


# ---------------------------------------------------------------------
# System
# ---------------------------------------------------------------------


def system(snapshot: dict, client) -> str:
    beat = snapshot["heartbeat"]
    ok, reason = llm.probe()
    described = llm.describe()

    rows = [
        ("DataFlow client", f"{client.kind}" + (" (simulated tenant)" if config.MOCK else "")),
        ("Writes", "**ARMED**: approvals change the tenant" if config.ARMED
         else "disarmed: approvals are recorded as dry runs (set SELFHEAL_ARMED=true in the project env)"),
        ("Service CRN", f"`{config.SERVICE_CRN or 'unset'}`"),
        ("Environment CRN", f"`{config.ENVIRONMENT_CRN or 'unset'}`"),
        ("LLM", f"{described['provider']} · `{described['model'] or 'unset'}` · "
                + ("ready: the endpoint accepts the token" if ok else f"**unavailable**: {reason}")
                + (" · tokens minted and renewed by the app" if llm.minting() else "")),
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


def propose_rollback(incident_id: str | None, action_id, reason: str, reviewer: str = "") -> str:
    """A reviewer's rollback proposal for an applied fix: a rollback incident, reviewed like any other."""

    store = get_store()
    action, problem = _applied(store, incident_id, action_id)

    if problem:
        return problem

    if not (reason or "").strip():
        return "A reason is required: why this fix should be undone."

    why = rollback.blocked(store, action, manual=True)

    if why:
        return why

    by = (reviewer or "").strip() or "a reviewer"
    incident = rollback.open_for(store, action, f"{by} asked: {reason.strip()}", get_client().now())
    graph.submit(incident)

    return (f"Opened {incident['id']} to roll back action {action['id']}. It goes to the Review Queue, "
            "to approve or reject like any fix.")
