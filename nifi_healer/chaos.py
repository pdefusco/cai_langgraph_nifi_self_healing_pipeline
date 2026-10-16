"""
The chaos injector: plants a scenario's fault again once it has cleared
and rested, so a soak (eval/soak.py) measures the agent on faults that
keep coming, not only on the one each pack was deployed with.

    tick(client, store)          # the poller, every CHAOS_TICK: plant at most one fault
    candidates(store, now)       # every scenario it could plant, the longest waiting first, and why not yet
    next_candidate(store, now)   # the one it would plant next
    pause / unpause / paused     # the System tab's switch

A scenario is planted only when its deployment is quiet (no open
incident, no pending review, no fix waiting to approve itself, no apply
in flight, the flow settled), its last episode ended (cleared, missed or
given up) at least CHAOS_REST ago, and its deployment has had fewer
than CHAOS_PER_DAY plants in the last day, the last CHAOS_SPACING ago.
A control is never planted: it has no fault.

Planting is provisioning.replant_steps then run_steps: the scenario's
process group swapped back to the baseline's and imported as a new
version, its own parameters back to their baseline values, the pack's
sizing. Each write passes the guardrails; disarmed (SELFHEAL_ARMED
false) it is a dry run, logged as DRY_RUN.

Every try is a row in chaos_injections: the soak's ground truth, which
the agent never reads. A row holds what was done (step labels and kinds,
parameter names), never parameter values or who.
"""

import config
from flows import packs
from nifi_healer import fleet, provisioning
from nifi_healer.cdp.client import STEADY_STATES, is_half_applied
from nifi_healer.store.db import OPEN_STATUSES

PLANTED, DRY_RUN, REFUSED, FAILED, UNCHANGED = "PLANTED", "DRY_RUN", "REFUSED", "FAILED", "UNCHANGED"
STATUSES = (PLANTED, DRY_RUN, REFUSED, FAILED, UNCHANGED)

# What counts toward the per-day cap and the spacing: every try that
# wrote, or would have.
COUNTED = (PLANTED, DRY_RUN, FAILED)

DAY = 86400
WHO = "chaos"

# An auto-approval's window still open (agent/autonomy.py; not imported:
# it imports the agent graph).
WAITING = "WAITING"

_PAUSED = "chaos_paused"
_SINCE = "chaos_since"

# A failure's detail keeps neither the command (it can carry parameter
# values) nor what the CLI printed.
_UNSAFE = ("  command", "  stderr", "  stdout")
_DETAIL = 300


def log(message: str) -> None:
    print(f"[chaos] {message}", flush=True)


# ---------------------------------------------------------------------
# The switch
# ---------------------------------------------------------------------


def paused(store) -> dict | None:
    """The setting row while a person has paused chaos ({updated_by, updated_at}), else None."""

    row = store.setting(_PAUSED)

    return row if row and row.get("value") == "true" else None


def pause(store, by: str, now: float) -> None:
    store.set_setting(_PAUSED, "true", by, now)


def unpause(store, by: str, now: float) -> None:
    store.set_setting(_PAUSED, "false", by, now)


def state(store) -> str:
    """off (SELFHEAL_CHAOS), paused, or on."""

    if not config.CHAOS:
        return "off"

    return "paused" if paused(store) else "on"


def since(store) -> float | None:
    """When chaos first ticked switched on: where the soak starts. None before."""

    row = store.setting(_SINCE)

    try:
        return float(row["value"]) if row else None

    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------
# What it could plant
# ---------------------------------------------------------------------


def _busy(store, entries: dict) -> dict:
    """{deployment CRN: why it is not quiet} for the fleet's busy deployments."""

    busy = {}

    for incident in store.incidents(OPEN_STATUSES, limit=100000):
        status = incident["status"].lower().replace("_", " ")
        busy.setdefault(incident["deployment_crn"], f"{incident['id']} is {status}")

    for row in store.auto_approvals((WAITING,)):
        incident = store.incident(row["incident_id"]) or {}

        if incident.get("deployment_crn"):
            busy.setdefault(incident["deployment_crn"], f"{incident['id']}'s fix is waiting to approve itself")

    for entry in entries.values():
        crn = entry["deployment_crn"]

        if any(a["status"] == "APPLYING" for a in store.actions(deployment_crn=crn)):
            busy.setdefault(crn, "a fix is being applied")

    return busy


def _ready_at(store, crn: str, scenario, last: dict | None, episodes: dict) -> float | None:
    """When the scenario has rested: from its last episode's end, its last try, or its last incident. None while open."""

    if last is None:
        closed = store.last_closed_incident(crn, scenario.group_name)

        return closed["closed_at"] + config.CHAOS_REST if closed else 0.0

    if last["status"] == PLANTED:
        found = episodes.get(last["id"])

        return found["ended_at"] + config.CHAOS_REST if found and found.get("ended_at") is not None else None

    return last["at"] + config.CHAOS_REST


def _minutes(seconds: float) -> str:
    return f"{max(seconds, 0) / 60:.0f} min"


def candidates(store, now: float) -> list:
    """
    Every scenario of the fleet's deployed packs but the controls:
    [{pack, deployment_crn, scenario, group, kind, family, ready_at,
    blocked}], the longest waiting first. `blocked` says why not now
    ("" when it may be planted); ready_at is None while its last plant's
    episode is open.
    """

    from nifi_healer.eval import soak

    entries = fleet.deployed_packs()
    busy = _busy(store, entries)
    episodes = {e["injection"]: e for e in soak.episodes(store, now)}
    out = []

    for name, entry in entries.items():
        pack = packs.PACKS.get(name)

        if pack is None:
            continue

        crn = entry["deployment_crn"]
        rows = store.injections(deployment_crn=crn)
        counted = [r for r in rows if r["status"] in COUNTED]
        today = [r for r in counted if now - r["at"] < DAY]

        for scenario in pack.scenarios:
            if scenario.family == "NONE" or scenario.kind == "control":
                continue

            mine = [r for r in rows if r["scenario"] == scenario.id]
            ready = _ready_at(store, crn, scenario, mine[-1] if mine else None, episodes)

            if crn in config.DENYLIST_DEPLOYMENT_CRNS:
                blocked = "its deployment is on the denylist"

            elif ready is None:
                blocked = "its last plant is still open"

            elif crn in busy:
                blocked = busy[crn]

            elif ready > now:
                blocked = f"resting another {_minutes(ready - now)}"

            elif len(today) >= config.CHAOS_PER_DAY:
                blocked = f"{len(today)} plants on this deployment in the last day (at most {config.CHAOS_PER_DAY})"

            elif counted and now - counted[-1]["at"] < config.CHAOS_SPACING:
                blocked = (f"the last plant on this deployment was {_minutes(now - counted[-1]['at'])} ago "
                           f"(at least {_minutes(config.CHAOS_SPACING)} apart)")

            else:
                blocked = ""

            out.append({"pack": name, "deployment_crn": crn, "scenario": scenario.id,
                        "group": scenario.group_name, "kind": scenario.kind, "family": scenario.family,
                        "ready_at": ready, "blocked": blocked})

    return sorted(out, key=lambda c: (c["ready_at"] is None, c["ready_at"] or 0.0, c["scenario"]))


def next_candidate(store, now: float) -> dict | None:
    """The scenario tick would plant next (the first unblocked), else the one nearest to it."""

    found = candidates(store, now)

    return next((c for c in found if not c["blocked"]), found[0] if found else None)


# ---------------------------------------------------------------------
# Planting
# ---------------------------------------------------------------------


def tick(client, store, wait=None) -> dict | None:
    """
    Plant at most one fault: the first candidate whose flow is settled.
    Returns its ledger row, else None. A scenario whose fault is still in
    place gets an UNCHANGED row (it rests from then), and the next is tried.
    """

    if not config.CHAOS or paused(store):
        return None

    now = client.now()

    if since(store) is None:
        store.set_setting(_SINCE, repr(now), WHO, now)
        log("on: the soak starts now")

    for candidate in candidates(store, now):
        if candidate["blocked"] or not _settled(client, candidate):
            continue

        row = _plant(client, store, candidate, wait)

        if row["status"] != UNCHANGED:
            return row

    return None


def _settled(client, candidate: dict) -> bool:
    entry = fleet.pack(candidate["pack"])
    deployment = client.deployment_facts(entry["deployment_crn"])
    flow = client.flow_facts(entry["deployment_crn"], entry["deployed_flow_crn"])

    return deployment.get("state") in STEADY_STATES and flow.get("state") in STEADY_STATES \
        and not is_half_applied(flow)


def _safe(error) -> str:
    """An error's message without the command or the CLI's output, short."""

    lines = [line for line in str(error).splitlines() if line.strip() and not line.startswith(_UNSAFE)]
    text = " ".join(line.strip() for line in lines) or type(error).__name__

    return text if len(text) <= _DETAIL else text[:_DETAIL - 1] + "…"


def _plant(client, store, candidate: dict, wait) -> dict:
    pack, scenario = candidate["pack"], candidate["scenario"]
    entry = fleet.pack(pack)
    crn = entry["deployment_crn"]
    at = client.now()
    before = client.flow_facts(crn, entry["deployed_flow_crn"]).get("flow_version_crn")
    row = dict(pack=pack, deployment_crn=crn, scenario=scenario, family=candidate["family"], at=at,
               from_flow_version_crn=before)

    def note(version_crn, version):
        # Before the version change, so drift handling knows it as this app's.
        store.note_flow_version(version_crn, pack, entry.get("flow_crn"), version, client.now(),
                                source="chaos", comments="nifi-healer")

    try:
        steps = provisioning.replant_steps(client, pack, scenario)

    except (SystemExit, Exception) as exc:
        return _record(store, client, row, FAILED, f"could not plan the plant: {_safe(exc)}")

    if not steps:
        return _record(store, client, row, UNCHANGED, "its fault is already in place")

    dry_run = not config.ARMED
    log(f"{'would plant' if dry_run else 'planting'} {scenario} on {pack}")

    try:
        records = provisioning.run_steps(client, pack, steps, dry_run=dry_run, wait=wait, tag="chaos",
                                         imported=note)
        error = ""

    except (SystemExit, Exception) as exc:
        records, error = [], _safe(exc)

    done = [{"label": r.get("step"), "kind": r.get("kind"), "performed": bool(r.get("performed"))}
            for r in records]
    to = next((r.get("imported") for r in records if r.get("kind") == "version" and r.get("performed")
               and r.get("imported")), None)
    last = records[-1] if records else {}

    if last.get("refused"):
        status, detail = REFUSED, f"refused: {_safe(last.get('reason') or '')}"

    elif error:
        status, detail = FAILED, error

    elif dry_run:
        status, detail = DRY_RUN, "writes are disarmed (SELFHEAL_ARMED)"

    elif len(records) >= len(steps) and last.get("performed"):
        status, detail = PLANTED, ""

    else:
        status, detail = FAILED, _safe((last.get("result") or {}).get("error") or "a step failed")

    return _record(store, client, dict(row, steps=done, to_flow_version_crn=to), status, detail)


def _record(store, client, row: dict, status: str, detail: str) -> dict:
    row = dict(row, status=status, detail=detail, finished_at=client.now())
    row["id"] = store.add_injection(**row)
    log(f"{row['scenario']} on {row['pack']}: {status}" + (f" ({detail})" if detail else ""))

    return row
