"""
Offline replay: every scenario, N times, on a fresh mock tenant, against
the configured LLM, scored the way the Scorecard scores the live agent.

    python -m nifi_healer.eval.replay                          # every scenario, once
    python -m nifi_healer.eval.replay --trials 3 --pack p1-throughput
    python -m nifi_healer.eval.replay --scenario S17 --oracle  # the design fix, no LLM
    python -m nifi_healer.eval.replay --trials 3 --no-memory   # the baseline: no past cases
    python -m nifi_healer.eval.replay --trials 3 --critic      # a second reading of every valid fix
    python -m nifi_healer.eval.replay --trials 3 --samples 3   # 3 first proposals, the valid one most agree on
    python -m nifi_healer.eval.replay --pack p12-warehouse --fixes 3   # up to 3 fixes a fault
    python -m nifi_healer.eval.replay --compare BEFORE_DIR AFTER_DIR   # two records side by side
    python -m nifi_healer.eval.replay --show DIR --pack p8-orders     # what the agent proposed, and why
    CAI job: cai_job.py    Arguments: -m nifi_healer.eval.replay --trials 3

Each trial round, over the packs the chosen scenarios are in:

  1. a fresh mock tenant: the packs imported and deployed, with three
     hours of faulty history (sim/bootstrap.py), on a stepped clock that
     moves only when the replay moves it, from a start set by the round
     (EPOCH): round n sees the same flows in every run, and the prompt
     hashes in the record show whether the agent read the same thing
  2. the poller backfills that history from the charts, as the app does,
     then opens an incident per fault, polling and moving the clock
     on for up to DETECT_SECONDS, or the longest detect_within of the
     scenarios (a slow burn shows after hours, a nightly batch once a
     day); the healthy control should open none
  3. the agent diagnoses each chosen scenario's incident, AGENT_WORKERS
     at a time, the clock stopped throughout (World.held), so it reads
     the KPIs as they were when the incident opened; one the LLM didn't
     answer (a timeout) is asked again, up to LLM_TRIES in all
  4. a valid proposal is approved as proposed, by REVIEWER, and applied
     to the mock; an invalid one is left at review
  5. the clock skips past the deciding verify windows (15 min, 1 h) and
     the poller judges each fix
  6. with `--fixes N` above 1, each scenario a fix was applied to is
     watched again: a new incident goes back to step 3, up to N fixes,
     and none within the detect window (after the incident cooldown)
     means the fault cleared. A compound fault needs a fix per fault;
     a masked one shows its second fault only once the first is fixed

It never reaches a live tenant. It refuses to approve unless the client
is the mock, and it keeps its own world, fleet, store, checkpoints and
playbook collections under REPLAY_DIR, apart from the app's. This is the
only place a proposal is approved without a reviewer.

After every round its record (the store's rows, and which incident each
trial was) is written to REPLAY_DIR/record.json; the Scorecard's Replay
source reads it. The agent version (prompt hash and model id) tells one
prompt or model from another; `--oracle` answers with each scenario's
design fix, which scores the harness and the simulation, not an agent.

The store is kept from run to run, so with memory on (rag/memory.py) a
round's agent sees the cases of every earlier round and run in this
REPLAY_DIR, except the oracle's; the Scorecard's Replay round grouping
shows whether it learns. `--no-memory` is the baseline (its agent
version ends /no-memory). Runs before 2026-10-14 could also leave out
the fix track records (/no-priors); the agent no longer sees them.

The agent reads the playbooks with the app's approved edits (the
Playbooks tab, rag/overlay.py), read from the app's store, so a replay
tests the agent as it stands (its version ends /playbooks:<edition>).
`--no-playbook-edits` reads the files alone.

`--critic` has a second LLM reading check every valid fix against the
evidence before it is approved (agent/nodes.critique_proposal); its
agent version ends /critic:<hash>. Without it the critic is off, whatever
SELFHEAL_CRITIC says, so a run is the agent it names.

`--samples N` draws N proposals at once on the first attempt and keeps
the valid fix most of them agree on (agent/nodes.diagnose_and_propose);
its agent version ends /samples:N. Without it one is drawn, whatever
SELFHEAL_SAMPLES says.

`--watch-all` gives every connection and processor of a group a
watch-only KPI (flows/scenarios/base.py watch_kpis, NFH-65), so the
readings cover the whole group. Without it a group has the KPIs its
scenario defines, whatever SELFHEAL_WATCH_ALL says.

The pacing guards (cooldown, actions per window) are off: every round is
a fresh tenant, and a pack's faults are fixed one after another, which on
a live tenant would take a day.
"""

import argparse
import hashlib
import json
import os
import random
import re
import sqlite3
import statistics
import sys
import textwrap
import time

import config
from flows import packs
from nifi_healer import fleet, llm
from nifi_healer.agent import graph, prompts
from nifi_healer.cdp import client as clients
from nifi_healer.cdp.mock_client import MockClient
from nifi_healer.eval import scorecard as sc
from nifi_healer.kpi import outcomes
from nifi_healer.rag import ingest, retriever
from nifi_healer.runtime.poller import Poller
from nifi_healer.sim import bootstrap, world as worlds
from nifi_healer.store import db

REVIEWER = "replay (auto-approved on the mock)"
HISTORY_SECONDS = 3 * 3600      # the 1 h window reads 1.5 h before the fix
DETECT_SECONDS = 1800
DETECT_STEP = 120
RECORD = "record.json"
LLM_TRIES = 3                   # diagnoses of an incident the LLM didn't answer, in a round
LLM_PAUSE_SECONDS = 30          # wall seconds before asking again
LUCK_DRAWS = 2000               # shuffles behind compare()'s luck band
LUCK_SHARE = 0.9                # of the shuffles the band holds

# Where each round's stepped clock starts (NFH-84): a run's rounds start at 10:00 UTC on 2026-10-01 and then
# ROUND_SHIFT later each, so they cover the day's schedules (a nightly batch, business hours), and with
# PHASE_STEP more history each, so the faults are caught at other points of their cycles. The nth run in a
# directory is that many days on, apart from the earlier runs in its store; the first run of any directory
# replays the same flows.
EPOCH = 1790848800.0
DAY = 86400
ROUND_SHIFT = 7 * 3600 + 97
PHASE_STEP = 37

# Every config setting isolate() changes.
ISOLATED = ("DF_CLIENT", "MOCK", "DATA_DIR", "DB_PATH", "CHECKPOINT_PATH", "FLEET_FILE", "MOCK_WORLD_FILE", "EDITS_DIR",
            "CHROMA_DIR", "SERVICE_CRN", "ENVIRONMENT_CRN", "ARMED", "COOLDOWN_SECONDS",
            "MAX_ACTIONS_PER_WINDOW", "VERIFY_WINDOWS", "PROPOSE_ROLLBACKS", "AUTO_DIAGNOSE", "MEMORY",
            "DRAFT_PLAYBOOK_EDITS", "AUTONOMY", "COUNT_WINDOW_SECONDS", "CRITIC", "SAMPLES", "WATCH_ALL")


def log(msg: str) -> None:
    print(f"[replay] {msg}", flush=True)


class OracleLLM:
    """
    Replies with the incident's scenario's design fix. For a compound
    fault, one fault's fix per incident, in order: a trial's second
    incident on the scenario gets the second fault's.
    """

    model_name = "oracle"

    def __init__(self):
        self.seen = {}          # {scenario id: its incident ids, in the order asked}

    def forget(self) -> None:
        """A new trial round: its incidents count from the first fault again."""

        self.seen = {}

    def steps(self, scenario, incident_id: str) -> list:
        if not scenario.faults:
            return list(scenario.expected)

        asked = self.seen.setdefault(scenario.id, [])

        if incident_id not in asked:
            asked.append(incident_id)

        return [scenario.faults[min(asked.index(incident_id), len(scenario.faults) - 1)]]

    def invoke(self, messages):
        text = "\n".join(prompts.text_of(m) for m in messages)

        if prompts.CRITIC_SYSTEM in text:  # the design fix agrees with the evidence
            return type("Reply", (), {"content": json.dumps({"objections": []})})()

        found = re.search(r"\b(INC-[\d-]+-(S\d\d)-\w+)", text)
        scenario = packs.SCENARIOS.get(found.group(2)) if found else None
        reply = {"diagnosis": f"the design fault of {scenario.title if scenario else 'an unknown scenario'}",
                 "root_cause_component": "", "steps": self.steps(scenario, found.group(1)) if scenario else
                 [{"action": "NO_ACTION"}], "rationale": "the scenario's design label",
                 "citations": re.findall(r"^\[([^\]]+)\]", text, re.M)[:1], "confidence": 1.0,
                 "expected_effect": "the scenario's KPIs recover"}

        return type("Reply", (), {"content": json.dumps(reply)})()


def isolate(directory: str):
    """Point this process at its own mock tenant under `directory`; returns its world."""

    os.makedirs(directory, exist_ok=True)
    config.DF_CLIENT, config.MOCK = "mock", True     # whatever the project environment says
    config.DATA_DIR = directory
    config.DB_PATH = os.path.join(directory, "healer.db")
    config.CHECKPOINT_PATH = os.path.join(directory, "checkpoints.sqlite")
    config.FLEET_FILE = os.path.join(directory, "fleet.json")
    config.MOCK_WORLD_FILE = os.path.join(directory, "mock_world.json")
    config.EDITS_DIR = os.path.join(directory, "edits")
    config.CHROMA_DIR = os.path.join(directory, "chroma")
    config.SERVICE_CRN, config.ENVIRONMENT_CRN = config.MOCK_SERVICE_CRN, config.MOCK_ENVIRONMENT_CRN
    config.ARMED = True                      # a write changes only the mock's own file
    config.COOLDOWN_SECONDS = 0
    config.MAX_ACTIONS_PER_WINDOW = 1000
    config.VERIFY_WINDOWS = config.VERIFY_WINDOWS[:2]   # the deciding ones
    config.PROPOSE_ROLLBACKS = False
    config.AUTO_DIAGNOSE = True              # the poller hands each new incident to detect()
    config.DRAFT_PLAYBOOK_EDITS = False      # nobody rejects here, and its edits are not the app's
    config.AUTONOMY = False                  # the replay approves every valid proposal itself
    config.COUNT_WINDOW_SECONDS = 1          # the mock charts processor counts per second, not per 5 minutes

    worlds._WORLD = None
    world = worlds.get_world()
    world.stepped = True                     # no wall time reaches the flows (World.now)
    clients.set_client(MockClient(world))
    db.set_store(db.Store(config.DB_PATH))
    graph.reset()
    ingest.reset_client()
    retriever.invalidate()

    return world


def _on_mock() -> bool:
    return config.MOCK and isinstance(clients.get_client(), MockClient)


def chosen(scenarios=(), pack_names=()) -> list:
    """The scenario ids to replay, in pack order: the named ones and those of the named packs, else all."""

    unknown = sorted(set(scenarios) - set(packs.SCENARIOS)) + sorted(set(pack_names) - set(packs.PACKS))

    if unknown:
        raise SystemExit(f"no such scenario or pack: {', '.join(unknown)}")

    every = [s.id for p in packs.PACKS.values() for s in p.scenarios]
    wanted = set(scenarios) | {s.id for name in pack_names for s in packs.PACKS[name].scenarios}

    return [s for s in every if s in wanted] if wanted else every


def window(ids: list, seconds: float = DETECT_SECONDS) -> float:
    """How long to wait for the incidents of `ids`: `seconds`, or the longest detect_within of them."""

    return max([seconds] + [packs.SCENARIOS[s].detect_within for s in ids])


def detect(world, client, store, ids: list, seconds: float = DETECT_SECONDS) -> dict:
    """
    {scenario id: its incident}, polling until every fault has one or
    window(ids, seconds) of sim time pass.
    """

    opened = {}
    faults = {s for s in ids if packs.SCENARIOS[s].family != "NONE"}
    poller = Poller(client, store, on_incident=lambda i: opened.setdefault(i.get("scenario"), i))
    deadline = client.now() + window(ids, seconds)

    # The app fills its store from the longer chart periods once it starts; without it every
    # reading the agent sees is one sample, flat, with no earlier peak.
    for name, entry in fleet.deployed_packs().items():
        poller.backfill(name, entry)

    while True:
        poller.poll_now()

        if faults <= opened.keys() or client.now() >= deadline:
            return {s: opened[s] for s in ids if s in opened}

        world.skip(DETECT_STEP)


def diagnose(world, incidents: list) -> None:
    """
    The agent's proposal for each incident, the clock stopped: an LLM takes
    a minute of wall time, twenty of sim time on the mock, and with no
    poller running meanwhile the agent would read empty KPIs.
    """

    with world.held():
        _diagnose(incidents)


def _diagnose(incidents: list) -> None:
    for tries in range(1, LLM_TRIES + 1):
        for future in [graph.submit(i) for i in incidents]:
            future.result()

        incidents = [i for i in incidents if graph.llm_failures(i["id"])]

        if not incidents or tries == LLM_TRIES:
            return

        log(f"the LLM didn't answer {len(incidents)} diagnosis(es); asking again in {LLM_PAUSE_SECONDS} s: "
            + ", ".join(i.get("scenario") or i["id"] for i in incidents))
        time.sleep(LLM_PAUSE_SECONDS)


def approve(incident_id: str) -> dict | None:
    """Approve a valid proposal as proposed; the action's result, or None when there was none to approve."""

    if not _on_mock():
        raise RuntimeError("the replay approves proposals on the mock tenant only")

    review = graph.pending(incident_id)

    if not review or not (review.get("plan") or {}).get("ok"):
        return None

    return graph.resume(incident_id, {"decision": "approve", "reviewer": REVIEWER}).get("result")


def judge(world, client, store, incidents: list) -> None:
    """Move the clock past each deciding verify window of the fixes applied, and judge them."""

    applied = [a for i in incidents for a in store.actions(incident_id=i["id"])
               if a["status"] == "APPLIED" and not a["dry_run"] and a.get("finished_at")]
    poller = Poller(client, store)

    for window in config.VERIFY_WINDOWS:
        if not applied:
            return

        due = max(outcomes.due_at(a["finished_at"], window, a["started_at"]) for a in applied)

        if due > client.now():
            world.skip(due - client.now() + 60)

        poller.evaluate_outcomes()


def prompt_hash(incident_id: str) -> str | None:
    """
    A short hash of the first prompt the agent was given on the incident,
    the random end of incident ids left out: two cases with the same hash
    showed the agent the same thing. None before it gathered its context.
    """

    state = graph.state(incident_id)

    if not state.get("context"):
        return None

    text = prompts.system_prompt() + prompts.user_prompt(state["incident"], state["context"],
                                                         state.get("playbooks") or [], memory=state.get("memory"))

    return hashlib.sha1(re.sub(r"\b(INC-\d{8}-\d{6}-\w+)-[0-9a-f]{4}\b", r"\1", text).encode()).hexdigest()[:8]


def attempt(number: int, fix: int, found: dict, world, client, store) -> tuple:
    """
    Diagnose, approve and judge each of {scenario id: incident}: (the
    scenarios a fix was applied to, {scenario id: prompt_hash}).
    """

    diagnose(world, list(found.values()))
    shown = {scenario: prompt_hash(incident["id"]) for scenario, incident in found.items()}
    applied = []

    for scenario, incident in found.items():
        result = approve(incident["id"])
        status = store.incident(incident["id"])["status"]
        log(f"round {number}, fix {fix}: {scenario} {incident['id']}: "
            + (f"approved, {result['status']}" if result else f"nothing to approve ({status})"))

        if result and result.get("status") == "APPLIED":
            applied.append(scenario)

    judge(world, client, store, list(found.values()))

    return applied, shown


def setting(number: int, day: int = 0) -> tuple:
    """(the sim time round `number` of the run `day` days on starts at, its seconds of history); see EPOCH."""

    return EPOCH + day * DAY + (number - 1) * ROUND_SHIFT % DAY, HISTORY_SECONDS + (number - 1) * PHASE_STEP


def trial_round(number: int, ids: list, world, client, store, detect_seconds: float = DETECT_SECONDS,
                fixes: int = 1, day: int = 0) -> list:
    """
    One trial of each scenario in `ids` on a fresh tenant, with up to `fixes`
    fixes each: [{round, scenario, incident, incidents, prompts, fixes,
    cleared, control, at, world}]. `incident` is the first, and `prompts`
    their prompt_hash; `cleared` is None unless `fixes` is above 1 (nothing
    was watched after the fix), and for a missed fault or a control.
    `world` is where in the day the round started, and its history: two
    trials with the same one, on the same flows and code, saw the same
    tenant.
    """

    if not _on_mock():
        raise RuntimeError("the replay runs on the mock tenant only")

    start, history = setting(number, day)
    bootstrap.build(history, list(dict.fromkeys(packs.pack_of(s).name for s in ids)), reset=True, epoch=start)
    at = client.now()
    found = detect(world, client, store, ids, detect_seconds)
    missed = [s for s in ids if s not in found and packs.SCENARIOS[s].family != "NONE"]
    log(f"round {number}: {len(found)} incident(s)" + (f"; no incident for {', '.join(missed)}" if missed else ""))
    trials = {s: {"round": number, "scenario": s, "incident": (found.get(s) or {}).get("id"), "incidents": [],
                  "prompts": [], "fixes": 0, "cleared": None, "control": packs.SCENARIOS[s].family == "NONE",
                  "at": at, "world": [(start - EPOCH) % DAY, history]}
              for s in ids}

    for fix in range(1, fixes + 1):
        applied, shown = attempt(number, fix, found, world, client, store)

        for scenario, incident in found.items():
            trials[scenario]["incidents"].append(incident["id"])
            trials[scenario]["prompts"].append(shown.get(scenario))
            trials[scenario]["fixes"] += scenario in applied

            if fixes > 1 and scenario not in applied:
                trials[scenario]["cleared"] = False

        if fixes == 1 or not applied:
            break

        # A closed incident's group opens no new one within the cooldown.
        found = detect(world, client, store, applied,
                       window(applied, detect_seconds) + config.INCIDENT_COOLDOWN_SECONDS)

        for scenario in applied:
            trials[scenario]["cleared"] = scenario not in found

        log(f"round {number}, after fix {fix}: " + ", ".join(
            f"{s} {'cleared' if s not in found else 'open again ' + found[s]['id']}" for s in applied))

    return [trials[s] for s in ids]


# ---------------------------------------------------------------------
# The record
# ---------------------------------------------------------------------


def load(directory: str = "") -> dict | None:
    """The replay's record.json, or None before the first round."""

    try:
        with open(os.path.join(directory or config.REPLAY_DIR, RECORD)) as handle:
            return json.load(handle)

    except (OSError, ValueError):
        return None


def save(directory: str, runs: list, now: float) -> None:
    path = os.path.join(directory, RECORD)
    body = {"format": 1, "now": now, "runs": runs, "record": db.get_store().record()}

    with open(path + ".tmp", "w") as handle:
        json.dump(body, handle, default=str)

    os.replace(path + ".tmp", path)


def _trials(snapshot: dict, since: float = 0.0) -> list:
    def name(run):
        return f"{run['id']} · {run.get('model') or '?'}" + (" · no memory" if run.get("memory") is False else "") \
            + (" · no priors" if run.get("priors") is False else "") + (" · critic" if run.get("critic") else "") \
            + (f" · {run['samples']} samples" if (run.get("samples") or 1) > 1 else "") \
            + (" · watch all" if run.get("watch_all") else "")

    # A stepped run's sim clock is on EPOCH's day, so it counts from when it ran.
    return [dict(t, run=name(run)) for run in snapshot.get("runs") or []
            for t in run.get("trials") or [] if max(t.get("at", 0), run.get("started_at") or 0) >= since]


def cases(snapshot: dict, since: float = 0.0) -> list:
    """
    The Scorecard's cases for the replay's trials, each with its run, round,
    fix, world and prompt hash (trial_round); the store's other incidents
    are not.
    """

    trials = {}

    for t in _trials(snapshot, since):
        shown = t.get("prompts") or []

        for fix, incident in enumerate(t.get("incidents") or [t.get("incident")], 1):
            if incident:
                trials[incident] = dict(t, fix=fix, prompt=shown[fix - 1] if fix <= len(shown) else None)

    return [dict(c, **{key: trials[c["incident"]].get(key) for key in ("run", "round", "fix", "world", "prompt")})
            for c in sc.cases(snapshot.get("record") or {}) if c["incident"] in trials]


def detection(snapshot: dict, since: float = 0.0) -> dict:
    """
    How many trials' faults opened an incident, how many control trials
    stayed quiet, and of the faults watched after their fixes (--fixes),
    how many cleared and after how many fixes (the median).
    """

    trials = _trials(snapshot, since)
    faults = [t for t in trials if not t["control"]]
    controls = [t for t in trials if t["control"]]
    watched = [t for t in faults if t.get("cleared") is not None]
    cleared = [t["fixes"] for t in watched if t["cleared"]]

    return {"runs": len({t["run"] for t in trials}), "faults": len(faults),
            "detected": sum(bool(t["incident"]) for t in faults),
            "missed": sorted({t["scenario"] for t in faults if not t["incident"]}),
            "controls": len(controls), "quiet": sum(not t["incident"] for t in controls),
            "watched": len(watched), "cleared": len(cleared),
            "fixes_to_clear": statistics.median(cleared) if cleared else None}


def report(snapshot: dict, run: str) -> str:
    mine = {"runs": [r for r in snapshot.get("runs") or [] if r["id"] == run], "record": snapshot["record"]}
    found = cases(mine)
    seen = detection(mine)
    rows = [f"{'scenario':<40} {'cases':>5} {'valid 1st':>9} {'design':>7} {'lever':>7} {'worked':>7}"]

    for name, s in sc.table(found, "Scenario"):
        rows.append(f"{name[:40]:<40} {s['incidents']:>5} {_pct(s['valid_first']):>9} {_pct(s['matched']):>7} "
                    f"{_pct(s['right_lever']):>7} {_pct(s['worked']):>7}")

    s = sc.summarize(found)
    rows.append(f"{'all':<40} {s['incidents']:>5} {_pct(s['valid_first']):>9} {_pct(s['matched']):>7} "
                f"{_pct(s['right_lever']):>7} {_pct(s['worked']):>7}")
    rows.append(f"detected {seen['detected']} of {seen['faults']} faults"
                + (f" (missed {', '.join(seen['missed'])})" if seen["missed"] else "")
                + f"; the control stayed quiet in {seen['quiet']} of {seen['controls']}")

    if seen["watched"]:
        rows.append(f"cleared {seen['cleared']} of {seen['watched']} detected faults"
                    + (f", after {seen['fixes_to_clear']:g} fix(es) (median)" if seen["cleared"] else ""))

    return "\n".join(rows)


def _pct(rate) -> str:
    return "-" if rate is None else f"{rate * 100:.0f}%"


# The columns of compare(), each worked out from a summary.
COMPARED = (
    ("cases", lambda s: str(s["incidents"])),
    ("expected", lambda s: _pct(s["match"][sc.EXPECTED] / sum(s["match"].values()) if any(s["match"].values())
                                else None)),
    ("value", lambda s: _pct(s["matched"])),
    ("lever", lambda s: _pct(s["right_lever"])),
    ("valid 1st", lambda s: _pct(s["valid_first"])),
    ("tries", lambda s: f"{(s['proposed'] + s['repairs']) / s['proposed']:.1f}" if s["proposed"] else "-"),
    ("worked", lambda s: _pct(s["worked"])),
    ("lift", lambda s: "-" if s["lift"] is None else f"{s['lift'] * 100:+.0f}%"),
)


# What tally() counts, per case: True, False, or None where it doesn't count.
TALLIED = (
    ("value", lambda c: c["match"] in sc.RIGHT_VALUE if c["proposed"] and c["match"] != sc.UNKNOWN else None),
    ("valid 1st", lambda c: bool(c["valid_first"]) if c["proposed"] else None),
    ("worked", lambda c: c["result"] == sc.WORKED if c["result"] in (sc.WORKED, sc.NO_HELP, sc.WORSE) else None),
)


def _counted(group: list, counted) -> list:
    return [v for v in map(counted, group) if v is not None]


def _change(pairs) -> float:
    """The after rate less the before rate, every case of every scenario pooled."""

    before, after = [v for b, _ in pairs for v in b], [v for _, a in pairs for v in a]

    return sum(after) / len(after) - sum(before) / len(before)


def _pairs(before: list, after: list) -> tuple:
    """
    ([(before case, after case)], the before cases left, the after cases
    left): a case pairs with the other record's case of the same world and
    fix, in order, so round n of a stepped run meets round n of the other
    (trial_round). Cases from before NFH-84 have no world and pair with none.
    """

    waiting = {}

    for c in after:
        if c.get("world") is not None:
            waiting.setdefault((tuple(c["world"]), c["fix"]), []).append(c)

    matched, left = [], []

    for c in before:
        found = waiting.get((tuple(c["world"]), c["fix"])) if c.get("world") is not None else None

        if found:
            matched.append((c, found.pop(0)))

        else:
            left.append(c)

    taken = {id(a) for _, a in matched}

    return matched, left, [c for c in after if id(c) not in taken]


def tally(name: str, counted, groups: dict) -> str:
    """
    One measure over {scenario: (before cases, after cases)}: the scenarios
    that did better, worse or the same, the change in the pooled rate, and
    how big a change luck alone makes. That is the band LUCK_SHARE of
    LUCK_DRAWS shuffles fall in, each shuffle dealing a scenario's cases
    out to before and after at random, as if one agent had run both times.
    A pair of cases on the same tenant (_pairs) is dealt together, one to
    each side, so the band holds the agent's own luck and not the tenant's.
    """

    scored = {}

    for scenario, (b, a) in groups.items():
        matched, b_left, a_left = _pairs(b, a)
        values = [(counted(x), counted(y)) for x, y in matched]
        pairs = [v for v in values if None not in v]
        b_left = _counted(b_left, counted) + [x for x, y in values if x is not None and y is None]
        a_left = _counted(a_left, counted) + [y for x, y in values if y is not None and x is None]

        if (pairs or b_left) and (pairs or a_left):
            scored[scenario] = (pairs, b_left, a_left)

    if not scored:
        return f"{name:<9} nothing to compare"

    def rate(values):
        return sum(values) / len(values)

    def sides(pairs, b_left, a_left):
        return [x for x, _ in pairs] + b_left, [y for _, y in pairs] + a_left

    split = {s: sides(*found) for s, found in scored.items()}
    better = [s for s, (b, a) in split.items() if rate(a) > rate(b)]
    worse = [s for s, (b, a) in split.items() if rate(a) < rate(b)]
    same = len(split) - len(better) - len(worse)
    seen = _change(split.values())
    rng = random.Random(0)
    shuffles = []

    for _ in range(LUCK_DRAWS):
        dealt = []

        for pairs, b_left, a_left in scored.values():
            flipped = [(y, x) if rng.random() < 0.5 else (x, y) for x, y in pairs]
            pool = rng.sample(b_left + a_left, len(b_left) + len(a_left))
            dealt.append(sides(flipped, pool[:len(b_left)], pool[len(b_left):]))

        shuffles.append(abs(_change(dealt)))

    luck = sorted(shuffles)[int(LUCK_SHARE * LUCK_DRAWS) - 1]
    before = rate([v for b, _ in split.values() for v in b])
    paired = sum(len(pairs) for pairs, _, _ in scored.values())

    def listed(found):
        return f" ({', '.join(found)})" if found else ""

    return (f"{name:<9} {len(better)} better{listed(better)}, {len(worse)} worse{listed(worse)}, {same} same; "
            f"{_pct(before)}→{_pct(before + seen)} over {len(split)} scenario(s), {seen * 100:+.0f} pts, "
            f"luck alone ±{luck * 100:.0f} pts" + (f" ({paired} paired)" if paired else "") + ": "
            + ("beyond luck" if abs(seen) > luck else "within luck"))


def _first_steps(snapshot: dict) -> dict:
    """{incident id: the steps of the agent's first proposal on it}."""

    first = {}

    for p in sorted(snapshot.get("record", {}).get("proposals") or [], key=lambda p: p["id"]):
        first.setdefault(p["incident_id"], json.dumps(p.get("action") or [], sort_keys=True))

    return first


def sameness(before: dict, after: dict, groups: dict) -> str:
    """
    Of the pairs of cases on the same tenant (_pairs), how many showed the
    agent the same prompt (prompt_hash), and of those how many it answered
    with the same first proposal: on one agent run twice, what the LLM
    alone changes.
    """

    matched = [pair for b, a in groups.values() for pair in _pairs(b, a)[0]]

    if not matched:
        return "same tenant: no pairs (a record from before NFH-84, or other rounds)"

    hashed = [(b, a) for b, a in matched if b.get("prompt") and a.get("prompt")]
    alike = [(b, a) for b, a in hashed if b["prompt"] == a["prompt"]]
    steps = [_first_steps(snapshot) for snapshot in (before, after)]
    answered = [(b, a) for b, a in alike if b["incident"] in steps[0] and a["incident"] in steps[1]]
    kept = sum(steps[0][b["incident"]] == steps[1][a["incident"]] for b, a in answered)

    return (f"same tenant: {len(matched)} pair(s), {len(alike)} of {len(hashed)} with the same prompt"
            + (f", and {kept} of {len(answered)} of those with the same first proposal" if answered else ""))


def compare(before: dict, after: dict) -> str:
    """
    Two replay records side by side, per scenario and in all, as
    before→after, both graded by the Scorecard as it is now: the design
    fix, the right value (design or acceptable), the right lever (also a
    value too small), valid on the first try, proposals per case, fixes
    that worked and the median outcome lift. "all" counts the scenarios
    both records have. Then, for the right value, valid on the first try
    and worked, the scenarios that did better and worse, and whether the
    change is beyond what luck alone makes (tally()), and how alike the
    cases on the same tenant were (sameness()). Agree before a run which
    of these decides whether a change stays.
    """

    sides = [cases(snapshot or {}) for snapshot in (before, after)]
    order = list(packs.SCENARIOS)
    ids = sorted({c["scenario"] for side in sides for c in side},
                 key=lambda s: (order.index(s) if s in order else len(order), s))
    titles = {c["scenario"]: c["scenario_title"] for side in sides for c in side}
    rows = [f"{'scenario':<32}" + "".join(f" {name:>11}" for name, _ in COMPARED)]

    def row(name, groups):
        shown = [[f(sc.summarize(g)) if g else "-" for _, f in COMPARED] for g in groups]
        return f"{name[:32]:<32}" + "".join(f" {b + '→' + a:>11}" for b, a in zip(*shown))

    for scenario in ids:
        rows.append(row(titles[scenario], [[c for c in side if c["scenario"] == scenario] for side in sides]))

    shared = [s for s in ids if all(any(c["scenario"] == s for c in side) for side in sides)]
    rows.append(row("all", [[c for c in side if c["scenario"] in shared] for side in sides]))
    alone = [f"only {name}: {', '.join(s for s in ids if s not in shared and any(c['scenario'] == s for c in side))}"
             for name, side in zip(("before", "after"), sides)
             if any(c["scenario"] not in shared for c in side)]
    rows.append(f"all: the {len(shared)} scenario(s) in both" + "".join(f"; {a}" for a in alone))
    groups = {s: tuple([c for c in side if c["scenario"] == s] for side in sides) for s in shared}
    rows += [tally(name, counted, groups) for name, counted in TALLIED]
    rows.append(sameness(before or {}, after or {}, groups))

    return "\n".join(rows)


def _step(step: dict) -> str:
    extra = {k: v for k, v in step.items() if k not in ("action", "target")}
    return f"{step.get('action')} {step.get('target') or ''} {json.dumps(extra) if extra else ''}".strip()


def _wrapped(label: str, text, width: int) -> str:
    text = " ".join(str(text or "").split())
    body = textwrap.fill(text[:width] + ("…" if len(text) > width else ""), 110,
                         initial_indent="      ", subsequent_indent="      ")
    return f"    {label}:\n{body}"


def show(snapshot: dict, ids=()) -> str:
    """
    What the agent proposed in a replay record, case by case: the design
    fix, then each attempt's steps, diagnosis and rationale, and the
    validation problems that sent it back, what the critic said of it
    (--critic), and how the drawn proposals voted (--samples), each draw
    with what it would have been sent back for. Only the scenarios in ids, if any.
    """

    proposals = {}

    for p in snapshot["record"].get("proposals") or []:
        proposals.setdefault(p["incident_id"], []).append(p)

    rows = []

    for case in sorted(cases(snapshot), key=lambda c: (c["scenario"], c["round"])):
        if ids and case["scenario"] not in ids:
            continue

        rows.append(f"\n=== {case['scenario_title']} · round {case['round']} · "
                    f"{case['match']} · {case['result'] or 'not judged'}")
        rows.append("  designed fix: " + "; ".join(_step(dict(s)) for s in packs.SCENARIOS[case["scenario"]].expected))

        for p in sorted(proposals.get(case["incident"], []), key=lambda p: p["id"]):
            vote = (p.get("validation") or {}).get("vote")
            rows.append(f"  attempt {p.get('attempt')} · {p.get('status')}"
                        + (f" · {vote['valid']} of {vote['samples']} drawn valid, {vote['agreeing']} agreeing"
                           if vote else ""))
            rows.append("    steps: " + ("; ".join(_step(s) for s in p.get("action") or []) or "none"))

            for d in (vote or {}).get("drawn") or []:
                rows.append("    drawn: " + ("; ".join(_step(s) for s in d["steps"]) or "none")
                            + (" · valid" if not d["problems"] else ""))

                if d["problems"]:
                    rows.append(_wrapped("  would be sent back", " | ".join(d["problems"]), 600))

            rows.append(_wrapped("diagnosis", p.get("diagnosis"), 600))
            rows.append(_wrapped("rationale", p.get("rationale"), 900))
            problems = (p.get("validation") or {}).get("problems") or []

            if problems:
                rows.append(_wrapped("sent back", " | ".join(problems), 900))

            if (p.get("validation") or {}).get("critique"):
                rows.append(_wrapped("second reading", _critique(p["validation"]["critique"]), 900))

    return "\n".join(rows).lstrip("\n")


def _critique(critique: dict) -> str:
    if critique.get("error"):
        return f"none: {critique['error']}"

    said = [f"{o['problem']} (\"{o['evidence']}\")" for o in critique.get("objections") or []]
    said += [f"unquoted, so dropped: {o['problem']}" for o in critique.get("dropped") or []]

    return " | ".join(said) or "no objections"


def live_edits(path: str) -> list:
    """The app's approved playbook edits, read from its store without writing to it; none if it has none."""

    if not path or not os.path.exists(path):
        return []

    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row

        try:
            return [dict(row) for row in connection.execute(
                "SELECT id, playbook, section_id, heading, old_text, new_text FROM playbook_edits "
                "WHERE status = 'APPLIED' ORDER BY id")]

        finally:
            connection.close()

    except sqlite3.Error as exc:  # an older store, before playbook edits
        log(f"no playbook edits read from {path}: {exc}")

        return []


def run(ids: list, trials: int = 1, directory: str = "", oracle: bool = False,
        detect_seconds: float = DETECT_SECONDS, memory: bool = True, playbook_edits: bool = True,
        fixes: int = 1, critic: bool = False, samples: int = 1, watch_all: bool = False) -> str:
    """
    `trials` rounds of `ids` in an isolated mock tenant, with up to `fixes`
    fixes a fault; returns the run's id. Calls isolate().
    The agent reads the playbooks with the app's approved edits, or the files alone without `playbook_edits`,
    with `critic` a second reading checks each valid fix (config.CRITIC; off unless asked for), and with
    `samples` above 1 that many first proposals are drawn and voted on (config.SAMPLES), and with `watch_all`
    every connection and processor has a KPI (config.WATCH_ALL).
    """

    edits = live_edits(config.DB_PATH) if playbook_edits else []
    directory = directory or config.REPLAY_DIR
    world = isolate(directory)
    config.MEMORY = memory
    config.CRITIC = critic
    config.SAMPLES = max(1, samples)
    config.WATCH_ALL = watch_all
    client, store = clients.get_client(), db.get_store()
    ingest.use_edits(edits)

    try:
        if oracle:
            llm.set_chat_model(OracleLLM())

        retriever.ensure()
        model = getattr(llm.chat_model(), "model_name", "") or config.LLM_MODEL_ID
        previous = load(directory) or {}
        runs = previous.get("runs") or []
        day = len(runs)
        entry = {"id": time.strftime("replay-%Y%m%d-%H%M%S", time.gmtime()), "model": model,
                 "agent_version": prompts.agent_version(model, retriever.edition()), "memory": memory,
                 "critic": critic, "samples": config.SAMPLES, "watch_all": watch_all, "scenarios": ids, "fixes": fixes, "trials": [], "started_at": time.time(), "finished_at": None}
        runs.append(entry)
        log(f"{entry['id']}: {trials} round(s) of {len(ids)} scenario(s) with {model}"
            + (f", up to {fixes} fixes each" if fixes > 1 else "") + ("" if memory else ", no memory")
            + (", with the critic" if critic else "")
            + (f", {config.SAMPLES} samples on the first attempt" if config.SAMPLES > 1 else "")
            + (", a KPI on every connection and processor" if watch_all else "")
            + (f", {len(edits)} approved playbook edit(s)" if edits else "") + f" in {directory}")
        now = previous.get("now") or 0.0

        for number in range(1, trials + 1):
            getattr(llm.chat_model(), "forget", lambda: None)()
            entry["trials"] += trial_round(number, ids, world, client, store, detect_seconds, fixes, day)
            now = max(now, client.now(), time.time())
            save(directory, runs, now)

        entry["finished_at"] = time.time()
        save(directory, runs, now)

        return entry["id"]

    finally:
        ingest.use_edits(None)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--trials", type=int, default=1, help="rounds of every chosen scenario")
    parser.add_argument("--scenario", action="append", default=[], help="only this scenario (repeatable)")
    parser.add_argument("--pack", action="append", default=[], help="only this pack's scenarios (repeatable)")
    parser.add_argument("--oracle", action="store_true", help="answer with the design fix instead of the LLM")
    parser.add_argument("--dir", default="", help=f"where the replay keeps its tenant (default {config.REPLAY_DIR})")
    parser.add_argument("--no-memory", action="store_true", help="show the agent no past cases (the baseline)")
    parser.add_argument("--critic", action="store_true",
                        help="have a second LLM reading check every valid fix before review (SELFHEAL_CRITIC)")
    parser.add_argument("--samples", type=int, default=1,
                        help="proposals drawn at once on the first attempt, the valid one most agree on kept "
                             "(SELFHEAL_SAMPLES)")
    parser.add_argument("--watch-all", action="store_true",
                        help="a watch-only KPI on every connection and processor of a group (SELFHEAL_WATCH_ALL)")
    parser.add_argument("--no-playbook-edits", action="store_true",
                        help="read the playbook files alone, without the app's approved edits")
    parser.add_argument("--detect-minutes", type=float, default=DETECT_SECONDS / 60,
                        help="simulated minutes to wait for every fault's incident, at least each "
                             "scenario's detect_within")
    parser.add_argument("--fixes", type=int, default=1,
                        help="fixes a fault may take: above 1, each fixed fault is watched for a new incident")
    parser.add_argument("--compare", nargs=2, metavar=("BEFORE", "AFTER"),
                        help="print two replay directories' records side by side, and run nothing")
    parser.add_argument("--show", metavar="DIR",
                        help="print what the agent proposed in a replay directory's record (only --scenario "
                             "and --pack, if given), and run nothing")
    args = parser.parse_args()

    if args.show:
        snapshot = load(args.show)

        if snapshot is None:
            parser.error(f"no replay record in {args.show}")

        print(show(snapshot, chosen(args.scenario, args.pack) if args.scenario or args.pack else ()), flush=True)

        return 0

    if args.compare:
        snapshots = [load(d) for d in args.compare]

        for directory, snapshot in zip(args.compare, snapshots):
            if snapshot is None:
                parser.error(f"no replay record in {directory}")

        print(compare(*snapshots), flush=True)

        return 0

    ids = chosen(args.scenario, args.pack)
    run_id = run(ids, max(1, args.trials), args.dir, args.oracle, args.detect_minutes * 60, not args.no_memory,
                 not args.no_playbook_edits, max(1, args.fixes), args.critic, max(1, args.samples),
                 args.watch_all)
    print(report(load(args.dir), run_id), flush=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
