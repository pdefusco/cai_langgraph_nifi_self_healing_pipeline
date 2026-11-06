"""
How the app's own work grows with the flow, with no LLM (NFH-94).

    python -m nifi_healer.eval.bench                          # 50, 100, 250 and 500 processors
    python -m nifi_healer.eval.bench --sizes 100 500 --repeats 5
    CAI job: cai_job.py    Arguments: -m nifi_healer.eval.bench

Each size is S03's pipeline bottleneck as a size twin (flows/scenarios/
scale.py) in one process group of that many processors, deployed alone on
a fresh mock tenant in a temporary directory (local disk, for SQLite).
It times, in wall seconds, the median of --repeats where a stage can be
run again without changing what it measures:

  build             packs.build: the flow definition
  provision         import, deploy and KPIs through MockClient, no history
  backfill 3 h      World.backfill: three hours of the simulation
  sim step          one 15 s step, from an hour of them on a copy
  world save, load  the world file, written and read whole
  detect            replay.detect: polling, the clock moved on, until the
                    incident opens (its sim seconds are in the sizes)
  poll backfill     a new Poller filling the store from the charts
  poll              Poller.poll_now on the deployment, a minute apart
  read flow         MockClient.flow_definition of the deployed version
  inventory         flow/inventory.py, then its render for the prompt
  readings          kpi/readings.py for the incident, then its render
  plan              executor.plan of the design fix: validate_proposal
                    but the capacity check, the definition read again
  edit, validate    edits.apply and edits.validate_edit, inside plan
  capacity          capacity.check of the planned fix
  fleet tables      the App's deployment, group and fleet tables
  monitor           the Monitor's deployment chart, model and render
  fleet monitor     the Monitor's fleet overview, model and render

and the sizes: processors, connections, KPIs, definition and world file
bytes, and the prompt's two big parts, inventory and readings, in
characters and tokens at four characters each. Prints a table and writes
provision/out/bench_<time>.json.
"""

import argparse
import contextlib
import copy
import json
import os
import shutil
import statistics
import sys
import tempfile
import time

import config
from flows import packs
from flows.scenarios import p1_throughput, scale
from flows.scenarios.base import Pack
from nifi_healer import fleet
from nifi_healer.actions import capacity, catalog, executor
from nifi_healer.cdp.client import get_client
from nifi_healer.eval import replay
from nifi_healer.flow import edits
from nifi_healer.flow.inventory import inventory, render
from nifi_healer.kpi import readings
from nifi_healer.runtime.poller import Poller
from nifi_healer.sim import bootstrap, engine
from nifi_healer.sim.world import groups_from_definition, parameters_of
from nifi_healer.store.db import get_store
from nifi_healer.ui import monitor, views

SIZES = (50, 100, 250, 500)
REPEATS = 3
SIM_SECONDS = 3600          # the sim step's sample: an hour of steps
POLL_GAP = 60               # sim seconds between timed polls

STAGES = ("build", "provision", "backfill 3 h", "sim step", "world save", "world load", "detect",
          "poll backfill", "poll", "read flow", "inventory", "inventory render", "readings",
          "readings render", "plan", "edit", "validate", "capacity", "fleet tables", "monitor",
          "fleet monitor")


def pack_name(size: int) -> str:
    return f"bench-{size}"


def bench_pack(size: int) -> Pack:
    """S03's fault in a group of `size` processors, as scenario B<size>."""

    scenario = scale.twin(p1_throughput.S03, f"B{size:03d}", processors=size)

    return Pack(name=pack_name(size), title=f"Bench, {size} processors", scenarios=(scenario,), wave=3)


@contextlib.contextmanager
def registered(sizes):
    """The bench packs in packs.PACKS and packs.SCENARIOS for the block, as if the repo had them."""

    added = [bench_pack(n) for n in sizes]
    saved = dict(packs.PACKS), dict(packs.SCENARIOS)

    for pack in added:
        packs.PACKS[pack.name] = pack
        packs.SCENARIOS.update({s.id: s for s in pack.scenarios})

    try:
        yield added

    finally:
        for found, kept in zip((packs.PACKS, packs.SCENARIOS), saved):
            found.clear()
            found.update(kept)


def _timed(work, repeats: int = 1, between=None) -> tuple:
    """(the median wall seconds of `repeats` runs of work(), its last result); between() runs before each, untimed."""

    seconds, result = [], None

    for _ in range(repeats):
        if between:
            between()

        started = time.perf_counter()
        result = work()
        seconds.append(time.perf_counter() - started)

    return statistics.median(seconds), result


def measure(size: int, directory: str, repeats: int = REPEATS) -> dict:
    """
    {size, sizes: {...}, seconds: {stage: wall seconds}} for the bench pack
    of `size`, on a fresh mock tenant in `directory`. The pack must be
    registered().
    """

    name = pack_name(size)
    pack = packs.PACKS[name]
    scenario = pack.scenarios[0]
    world = replay.isolate(directory)
    client, store = get_client(), get_store()
    t, sizes = {}, {}

    t["build"], definition = _timed(lambda: packs.build(name), repeats)
    counted = replay._counted_flow(definition["flowContents"])
    sizes.update(processors=counted["processors"], connections=counted["connections"], kpis=len(pack.kpis),
                 definition_bytes=len(json.dumps(definition, indent=2)))

    t["provision"], _ = _timed(lambda: bootstrap.build(0, [name], reset=True, epoch=replay.EPOCH))
    entry = fleet.deployed_packs()[name]
    crn = entry["deployment_crn"]
    t["backfill 3 h"], _ = _timed(lambda: world.backfill(crn, replay.HISTORY_SECONDS))

    sim = copy.deepcopy(world.sims[crn])
    started = time.perf_counter()
    sim.advance(sim.t + SIM_SECONDS)
    t["sim step"] = (time.perf_counter() - started) / (SIM_SECONDS / engine.DT)

    t["world save"], _ = _timed(world.save, repeats)
    sizes["world_bytes"] = os.path.getsize(config.MOCK_WORLD_FILE)

    def load():
        world._read_mtime = None
        world._load()

    t["world load"], _ = _timed(load, repeats)

    began = client.now()
    t["detect"], found = _timed(lambda: replay.detect(world, client, store, [scenario.id]))
    sizes["detect_sim_seconds"] = client.now() - began

    poller = Poller(client, store)
    t["poll backfill"], _ = _timed(lambda: poller.backfill(name, entry))
    t["poll"], _ = _timed(lambda: poller.poll_now(name), repeats, between=lambda: world.skip(POLL_GAP))

    if scenario.id in found:
        _agent(t, sizes, client, store, store.incident(found[scenario.id]["id"]), list(scenario.expected), repeats)

    snapshot = poller.snapshot()
    t["fleet tables"], _ = _timed(lambda: (views.deployment_rows(snapshot, store), views.group_rows(snapshot, store),
                                           views.fleet_rows(snapshot, store)), repeats)
    t["monitor"], _ = _timed(lambda: monitor.render(monitor.model(store, name, entry, client.now())), repeats)
    t["fleet monitor"], _ = _timed(
        lambda: monitor.render_fleet(monitor.fleet_model(store, fleet.deployed_packs(), client.now())), repeats)

    return {"size": size, "sizes": sizes, "seconds": {stage: round(s, 6) for stage, s in t.items()}}


def _agent(t: dict, sizes: dict, client, store, incident: dict, steps: list, repeats: int) -> None:
    """What gather_context and validate_proposal do for `incident`, stage by stage, the design fix proposed."""

    deployment, flow, group = incident["deployment_crn"], incident["deployed_flow_crn"], incident["process_group"]
    version = client.flow_facts(deployment, flow)["flow_version_crn"]
    t["read flow"], definition = _timed(lambda: client.flow_definition(version), repeats)
    groups = client.flow_configuration(deployment, flow).get("parameterGroups") \
        or groups_from_definition(definition)

    t["inventory"], found = _timed(lambda: inventory(definition, group, groups), repeats)
    t["inventory render"], inventory_text = _timed(lambda: render(found), repeats)
    now = client.now()
    t["readings"], rows = _timed(lambda: readings.for_incident(incident, store, now), repeats)
    t["readings render"], readings_text = _timed(lambda: readings.render(rows, found), repeats)
    sizes.update(inventory_chars=len(inventory_text), readings_chars=len(readings_text),
                 prompt_tokens=round((len(inventory_text) + len(readings_text)) / 4))

    t["plan"], full = _timed(lambda: executor.plan(client, incident, steps, store), repeats)
    sizes["plan_ok"] = full["ok"]
    normal = catalog.merge_routes([catalog.normalize(s) for s in steps])
    t["edit"], (edited, _) = _timed(lambda: edits.apply(definition, group, normal), repeats)
    t["validate"], _ = _timed(lambda: edits.validate_edit(definition, edited, parameters_of(groups), group), repeats)
    t["capacity"], _ = _timed(lambda: capacity.check(full, incident, store, now, 1), repeats)


def run(sizes=SIZES, repeats: int = REPEATS, directory: str = "", out=None) -> list:
    """measure() each size, each in its own directory under `directory` (a temporary one by default, removed after)."""

    out = out or sys.stdout
    base = directory or tempfile.mkdtemp(prefix="bench-")
    os.makedirs(base, exist_ok=True)
    saved = {name: getattr(config, name) for name in replay.ISOLATED + ("FLOWS_OUT_DIR",)}
    found = []
    # Provisioning writes each built flow to flows/out, where only the repo's packs belong.
    config.FLOWS_OUT_DIR = os.path.join(base, "flows")

    try:
        with registered(sizes):
            for size in sizes:
                print(f"[bench] {size} processors ...", file=out, flush=True)
                path = os.path.join(base, pack_name(size))

                # The provisioning and the poller log every step; the bench's own lines are the table.
                with open(os.path.join(base, f"{pack_name(size)}.log"), "w") as log, contextlib.redirect_stdout(log):
                    found.append(measure(size, path, repeats))

    finally:
        for name, value in saved.items():
            setattr(config, name, value)

        if not directory:
            shutil.rmtree(base, ignore_errors=True)

    return found


def _ms(seconds) -> str:
    if seconds is None:
        return "—"

    return f"{seconds * 1000:.1f}" if seconds < 10 else f"{seconds * 1000:.0f}"


def table(found: list) -> str:
    """Stages down, sizes across, in milliseconds; then the sizes."""

    if not found:
        return ""

    width = max(len(s) for s in STAGES + tuple(found[0]["sizes"])) + 2
    head = "".ljust(width) + "".join(f"{r['size']:>11}" for r in found)
    lines = [head, "ms".ljust(width)]

    for stage in STAGES:
        lines.append(stage.ljust(width) + "".join(f"{_ms(r['seconds'].get(stage)):>11}" for r in found))

    lines.append("")

    for key in found[0]["sizes"]:
        lines.append(key.ljust(width) + "".join(f"{str(r['sizes'].get(key, '—')):>11}" for r in found))

    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--sizes", type=int, nargs="+", default=list(SIZES), help="processors in the group")
    parser.add_argument("--repeats", type=int, default=REPEATS, help="runs of each repeatable stage; the median")
    parser.add_argument("--dir", default="", help="keep the tenants here (default: a temporary directory)")
    parser.add_argument("--out", default="", help="the JSON report (default: provision/out/bench_<time>.json)")
    args = parser.parse_args()

    found = run(args.sizes, args.repeats, args.dir)
    print(table(found))
    path = args.out or os.path.join(config.PROVISION_OUT_DIR, f"bench_{time.strftime('%Y%m%d-%H%M%S')}.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "w") as handle:
        json.dump({"at": time.time(), "repeats": args.repeats, "results": found}, handle, indent=2)

    print(f"\n[bench] wrote {path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
