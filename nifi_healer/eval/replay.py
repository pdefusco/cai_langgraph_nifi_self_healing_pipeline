"""
Offline replay: every scenario, N times, on a fresh mock tenant, against
the configured LLM, scored the way the Scorecard scores the live agent.

    python -m nifi_healer.eval.replay                          # every scenario, once
    python -m nifi_healer.eval.replay --trials 3 --pack p1-throughput
    python -m nifi_healer.eval.replay --scenario S17 --oracle  # the design fix, no LLM
    python -m nifi_healer.eval.replay --trials 3 --no-memory   # the baseline: no past cases
    CAI job: cai_job.py    Arguments: -m nifi_healer.eval.replay --trials 3

Each trial round, over the packs the chosen scenarios are in:

  1. a fresh mock tenant: the packs imported and deployed, with three
     hours of faulty history (sim/bootstrap.py)
  2. the poller opens an incident per fault, polling and moving the clock
     on for up to DETECT_SECONDS; the healthy control should open none
  3. the agent diagnoses each chosen scenario's incident, AGENT_WORKERS
     at a time
  4. a valid proposal is approved as proposed, by REVIEWER, and applied
     to the mock; an invalid one is left at review
  5. the clock skips past the deciding verify windows (15 min, 1 h) and
     the poller judges each fix

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
version ends /no-memory).

The pacing guards (cooldown, actions per window) are off: every round is
a fresh tenant, and a pack's faults are fixed one after another, which on
a live tenant would take a day.
"""

import argparse
import json
import os
import re
import sys
import time

import config
from flows import packs
from nifi_healer import llm
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

# Every config setting isolate() changes.
ISOLATED = ("DF_CLIENT", "MOCK", "DATA_DIR", "DB_PATH", "CHECKPOINT_PATH", "FLEET_FILE", "MOCK_WORLD_FILE", "EDITS_DIR",
            "CHROMA_DIR", "SERVICE_CRN", "ENVIRONMENT_CRN", "ARMED", "COOLDOWN_SECONDS",
            "MAX_ACTIONS_PER_WINDOW", "VERIFY_WINDOWS", "PROPOSE_ROLLBACKS", "AUTO_DIAGNOSE", "MEMORY")


def log(msg: str) -> None:
    print(f"[replay] {msg}", flush=True)


class OracleLLM:
    """Replies with the incident's scenario's design fix."""

    model_name = "oracle"

    def invoke(self, messages):
        text = "\n".join(prompts.text_of(m) for m in messages)
        found = re.search(r"\bINC-[\d-]+-(S\d\d)-", text)
        scenario = packs.SCENARIOS.get(found.group(1)) if found else None
        reply = {"diagnosis": f"the design fault of {scenario.title if scenario else 'an unknown scenario'}",
                 "root_cause_component": "", "steps": list(scenario.expected) if scenario else
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

    worlds._WORLD = None
    world = worlds.get_world()
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


def detect(world, client, store, ids: list, seconds: float = DETECT_SECONDS) -> dict:
    """{scenario id: its incident}, polling until every fault has one or `seconds` of sim time pass."""

    opened = {}
    faults = {s for s in ids if packs.SCENARIOS[s].family != "NONE"}
    poller = Poller(client, store, on_incident=lambda i: opened.setdefault(i.get("scenario"), i))
    deadline = client.now() + seconds

    while True:
        poller.poll_now()

        if faults <= opened.keys() or client.now() >= deadline:
            return {s: opened[s] for s in ids if s in opened}

        world.skip(DETECT_STEP)


def diagnose(incidents: list) -> None:
    for future in [graph.submit(i) for i in incidents]:
        future.result()


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


def trial_round(number: int, ids: list, world, client, store, detect_seconds: float = DETECT_SECONDS) -> list:
    """One trial of each scenario in `ids` on a fresh tenant: [{round, scenario, incident, control, at}]."""

    if not _on_mock():
        raise RuntimeError("the replay runs on the mock tenant only")

    bootstrap.build(HISTORY_SECONDS, list(dict.fromkeys(packs.pack_of(s).name for s in ids)), reset=True)
    at = client.now()
    found = detect(world, client, store, ids, detect_seconds)
    missed = [s for s in ids if s not in found and packs.SCENARIOS[s].family != "NONE"]
    log(f"round {number}: {len(found)} incident(s)" + (f"; no incident for {', '.join(missed)}" if missed else ""))

    diagnose(list(found.values()))

    for scenario, incident in found.items():
        result = approve(incident["id"])
        status = store.incident(incident["id"])["status"]
        log(f"round {number}: {scenario} {incident['id']}: "
            + (f"approved, {result['status']}" if result else f"nothing to approve ({status})"))

    judge(world, client, store, list(found.values()))

    return [{"round": number, "scenario": s, "incident": (found.get(s) or {}).get("id"),
             "control": packs.SCENARIOS[s].family == "NONE", "at": at} for s in ids]


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
        return f"{run['id']} · {run.get('model') or '?'}" + (" · no memory" if run.get("memory") is False else "")

    return [dict(t, run=name(run)) for run in snapshot.get("runs") or []
            for t in run.get("trials") or [] if t.get("at", 0) >= since]


def cases(snapshot: dict, since: float = 0.0) -> list:
    """The Scorecard's cases for the replay's trials, each with its run; the store's other incidents are not."""

    trials = {t["incident"]: t for t in _trials(snapshot, since) if t.get("incident")}

    return [dict(c, run=trials[c["incident"]]["run"], round=trials[c["incident"]]["round"])
            for c in sc.cases(snapshot.get("record") or {}) if c["incident"] in trials]


def detection(snapshot: dict, since: float = 0.0) -> dict:
    """How many trials' faults opened an incident, and how many control trials stayed quiet."""

    trials = _trials(snapshot, since)
    faults = [t for t in trials if not t["control"]]
    controls = [t for t in trials if t["control"]]

    return {"runs": len({t["run"] for t in trials}), "faults": len(faults),
            "detected": sum(bool(t["incident"]) for t in faults),
            "missed": sorted({t["scenario"] for t in faults if not t["incident"]}),
            "controls": len(controls), "quiet": sum(not t["incident"] for t in controls)}


def report(snapshot: dict, run: str) -> str:
    mine = {"runs": [r for r in snapshot.get("runs") or [] if r["id"] == run], "record": snapshot["record"]}
    found = cases(mine)
    seen = detection(mine)
    rows = [f"{'scenario':<40} {'cases':>5} {'valid 1st':>9} {'design':>7} {'worked':>7}"]

    for name, s in sc.table(found, "Scenario"):
        rows.append(f"{name[:40]:<40} {s['incidents']:>5} {_pct(s['valid_first']):>9} {_pct(s['matched']):>7} "
                    f"{_pct(s['worked']):>7}")

    s = sc.summarize(found)
    rows.append(f"{'all':<40} {s['incidents']:>5} {_pct(s['valid_first']):>9} {_pct(s['matched']):>7} "
                f"{_pct(s['worked']):>7}")
    rows.append(f"detected {seen['detected']} of {seen['faults']} faults"
                + (f" (missed {', '.join(seen['missed'])})" if seen["missed"] else "")
                + f"; the control stayed quiet in {seen['quiet']} of {seen['controls']}")

    return "\n".join(rows)


def _pct(rate) -> str:
    return "-" if rate is None else f"{rate * 100:.0f}%"


def run(ids: list, trials: int = 1, directory: str = "", oracle: bool = False,
        detect_seconds: float = DETECT_SECONDS, memory: bool = True) -> str:
    """`trials` rounds of `ids` in an isolated mock tenant; returns the run's id. Calls isolate()."""

    directory = directory or config.REPLAY_DIR
    world = isolate(directory)
    config.MEMORY = memory
    client, store = clients.get_client(), db.get_store()

    if oracle:
        llm.set_chat_model(OracleLLM())

    model = getattr(llm.chat_model(), "model_name", "") or config.LLM_MODEL_ID
    previous = load(directory) or {}
    runs = previous.get("runs") or []
    entry = {"id": time.strftime("replay-%Y%m%d-%H%M%S", time.gmtime()), "model": model,
             "agent_version": prompts.agent_version(model), "memory": memory, "scenarios": ids, "trials": [],
             "started_at": time.time(), "finished_at": None}
    runs.append(entry)
    log(f"{entry['id']}: {trials} round(s) of {len(ids)} scenario(s) with {model}"
        + ("" if memory else ", no memory") + f" in {directory}")
    now = previous.get("now") or 0.0

    for number in range(1, trials + 1):
        entry["trials"] += trial_round(number, ids, world, client, store, detect_seconds)
        now = max(now, client.now())
        save(directory, runs, now)

    entry["finished_at"] = time.time()
    save(directory, runs, now)

    return entry["id"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--trials", type=int, default=1, help="rounds of every chosen scenario")
    parser.add_argument("--scenario", action="append", default=[], help="only this scenario (repeatable)")
    parser.add_argument("--pack", action="append", default=[], help="only this pack's scenarios (repeatable)")
    parser.add_argument("--oracle", action="store_true", help="answer with the design fix instead of the LLM")
    parser.add_argument("--dir", default="", help=f"where the replay keeps its tenant (default {config.REPLAY_DIR})")
    parser.add_argument("--no-memory", action="store_true", help="show the agent no past cases (the baseline)")
    parser.add_argument("--detect-minutes", type=float, default=DETECT_SECONDS / 60,
                        help="simulated minutes to wait for every fault's incident")
    args = parser.parse_args()

    ids = chosen(args.scenario, args.pack)
    run_id = run(ids, max(1, args.trials), args.dir, args.oracle, args.detect_minutes * 60, not args.no_memory)
    print(report(load(args.dir), run_id), flush=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
