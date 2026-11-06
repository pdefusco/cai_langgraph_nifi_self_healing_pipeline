"""
Customer-sized flows (NFH-95): healthy filler, and size twins.

A *size twin* is a small scenario's fault in a much larger group. It
keeps the twin's processors, connections, KPIs, design fix and traps,
renamed to its own id, and adds healthy filler beside them: small flows
of their own that never touch the fault. Its fault, fix and traps are
the twin's, so `replay --compare --twins` measures what size alone does
to the agent.

    S46 = scale.twin(p1_throughput.S01, "S46", processors=100)

The filler is deterministic, seeded by the scenario id, and built only
from the helpers in common.py. Its shapes are the healthy patterns of the
other packs:

  chain     a source, a few pass-through stages and a sink
  lanes     a DistributeLoad over two to four lanes that join again
  validate  a DebugFlow that fails 1 in 20 into a connected DLQ
  rate      a ControlRate far above its load

Each source makes one to three 1 KB FlowFiles a second, far inside the
simulator's throughput cap per connection and its CPU per FlowFile, so
100 processors of filler cost the node about a tenth of a core.
"""

import dataclasses
import random

from flows.builder.lib import COLUMN_SPACING, ROW_SPACING
from flows.scenarios.base import Scenario
from flows.scenarios.common import debug_flow, dlq, generator, sink, stage

DOMAINS = (
    "Invoices", "Shipments", "Returns", "Refunds", "Inventory", "Prices", "Customers", "Reviews", "Coupons",
    "Carts", "Wishlists", "Suppliers", "Contracts", "Tickets", "Leads", "Campaigns", "Payroll", "Timesheets",
    "Assets", "Leases", "Claims", "Policies", "Quotes", "Vendors", "Stores", "Catalog", "Ledger", "Budgets",
    "Audits", "Alerts", "Devices", "Firmware", "Meters", "Routes", "Drivers", "Fleet", "Bookings", "Rooms",
    "Menus", "Recipes",
)
VERBS = ("Parse", "Validate", "Enrich", "Normalize", "Tag", "Classify", "Mask", "Dedupe", "Format", "Route",
         "Score", "Stamp", "Convert", "Annotate")
SHAPES = ("chain", "lanes", "validate", "rate")


class _Renamed:
    """A process group that renames what a twin's build adds to it: "S01 x" to "S46 x", "s01-x" to "s46-x"."""

    def __init__(self, pg, rename):
        self._pg, self._rename = pg, rename

    def processor(self, key, name, *args, **options):
        return self._pg.processor(key, self._rename(name), *args, **options)

    def connect(self, source, destination, relationships, name="", **options):
        return self._pg.connect(source, destination, relationships, self._rename(name), **options)

    def __getattr__(self, attribute):
        return getattr(self._pg, attribute)


def renamer(old: str, new: str):
    def rename(value):
        if isinstance(value, str):
            return value.replace(f"{old} ", f"{new} ").replace(f"{old.lower()}-", f"{new.lower()}-")

        if isinstance(value, dict):
            return {k: rename(v) for k, v in value.items()}

        if isinstance(value, (tuple, list)):
            return type(value)(rename(v) for v in value)

        return value

    return rename


class Twinned:
    """The build of a size twin: its twin's build, renamed, then filler up to `processors`."""

    def __init__(self, small: Scenario, scenario_id: str, processors: int):
        self.small, self.id, self.processors = small, scenario_id, processors
        self.rename = renamer(small.id, scenario_id)
        self.shapes = []

    def core(self, pg) -> None:
        self.small.build(_Renamed(pg, self.rename))

    def __call__(self, pg) -> None:
        self.core(pg)
        self.shapes = filler(pg, self.id, self.processors - len(pg.processors))


def twin(small: Scenario, scenario_id: str, *, processors: int, title: str = "") -> Scenario:
    """`small`'s fault as scenario `scenario_id`, in a group of about `processors` processors."""

    rename = renamer(small.id, scenario_id)
    kpis = tuple(dataclasses.replace(k, scenario=scenario_id, component=rename(k.component)) for k in small.kpis)

    return dataclasses.replace(
        small,
        id=scenario_id,
        title=title or f"{small.title} in {processors} Processors",
        build=Twinned(small, scenario_id, processors),
        kpis=kpis,
        expected=rename(small.expected),
        acceptable=rename(small.acceptable),
        traps=rename(small.traps),
        wrong=rename(small.wrong),
        faults=rename(small.faults),
        edges=rename(small.edges),
        twin=small.id,
    )


def filler(pg, scenario_id: str, count: int) -> list:
    """
    About `count` healthy processors in small flows of their own, each in
    its own column right of what `pg` already holds. Returns the shapes
    built, in order.
    """

    rng = random.Random(f"filler:{scenario_id}")
    prefix, slug = scenario_id, scenario_id.lower()
    built = []

    while count >= 3:
        shape = SHAPES[len(built) % len(SHAPES)]
        size = {"chain": 2 + rng.randint(2, 6), "lanes": 4 + rng.randint(2, 4), "validate": 6, "rate": 4}[shape]

        if count - size < 3:
            shape, size = "chain", count

        index = len(built)
        domain = DOMAINS[index % len(DOMAINS)] + (f" {index // len(DOMAINS) + 1}" if index >= len(DOMAINS) else "")
        flow = _Flow(pg, f"f{index}", f"{prefix} {domain}", f"{slug}-{domain.lower().replace(' ', '-')}",
                     column=index + 1)
        BUILDERS[shape](flow, size, rng)
        built.append(shape)
        count -= size

    return built


class _Flow:
    """One filler flow: keys, names and positions for its processors, and named connections."""

    def __init__(self, pg, key, name, slug, column):
        self.pg, self.key, self.name, self.slug, self.column = pg, key, name, slug, column
        self.rows = 0
        self.links = 0

    def place(self, helper, part, label, **options) -> str:
        key = f"{self.key}.{part}"
        made = helper(self.pg, key, f"{self.name} {label}", **options)
        made["position"] = {"x": self.column * COLUMN_SPACING, "y": self.rows * ROW_SPACING}
        self.rows += 1

        return key

    def source(self, rng, batch=None) -> str:
        key = f"{self.key}.gen"
        generator(self.pg, f"{self.name} Feed", period="1 sec", batch=batch or rng.randint(1, 2), key=key)
        self.pg.processors[key]["position"] = {"x": self.column * COLUMN_SPACING, "y": 0.0}
        self.rows = 1

        return key

    def connect(self, source, destination, relationships=("success",)) -> None:
        self.links += 1
        self.pg.connect(source, destination, list(relationships), f"{self.slug}-{self.links}")


def _stages(flow, rng, after, count) -> str:
    for verb in rng.sample(VERBS, count):
        key = flow.place(stage, verb.lower(), verb, period="0 sec")
        flow.connect(after, key)
        after = key

    return after


def _chain(flow, size, rng) -> None:
    last = _stages(flow, rng, flow.source(rng), size - 2)
    flow.connect(last, flow.place(sink, "store", "Store", period="0 sec"))


def _lanes(flow, size, rng) -> None:
    lanes = size - 4
    gen = flow.source(rng, batch=3)
    balance = flow.place(lambda pg, key, name: pg.processor(key, name, "DistributeLoad", {
        "Number of Relationships": str(lanes),
        "Distribution Strategy": "round robin",
    }, period="0 sec", tasks=1), "balance", "Balance")
    lane_keys = [flow.place(stage, f"lane{lane}", f"Lane {lane}", period="0 sec") for lane in range(1, lanes + 1)]
    join = flow.place(stage, "join", "Join Lanes", period="0 sec")
    flow.connect(gen, balance)

    for lane, key in enumerate(lane_keys, 1):
        flow.connect(balance, key, (str(lane),))
        flow.connect(key, join)

    flow.connect(join, flow.place(sink, "store", "Store", period="0 sec"))


def _validate(flow, size, rng) -> None:
    gen = flow.source(rng)
    parse = flow.place(stage, "parse", "Parse", period="0 sec")
    check = flow.place(debug_flow, "check", "Check", successes=19, failures=1, period="0 sec", tasks=1)
    reject = flow.place(lambda pg, key, name: dlq(pg, name, key=key), "dlq", "Reject DLQ")
    fmt = flow.place(stage, "format", "Format", period="0 sec")
    flow.connect(gen, parse)
    flow.connect(parse, check)
    flow.connect(check, reject, ("failure",))
    flow.connect(check, fmt)
    flow.connect(fmt, flow.place(sink, "store", "Store", period="0 sec"))


def _rate(flow, size, rng) -> None:
    gen = flow.source(rng)
    limit = flow.place(lambda pg, key, name: pg.processor(key, name, "ControlRate", {
        "Rate Control Criteria": "flowfile count",
        "Maximum Rate": "600",
        "Time Duration": "1 min",
    }, period="0 sec", tasks=1, auto_terminate=("failure",)), "limit", "Rate Limit")
    flow.connect(gen, limit)
    last = _stages(flow, rng, limit, 1)
    flow.connect(last, flow.place(sink, "store", "Store", period="0 sec"))


BUILDERS = {"chain": _chain, "lanes": _lanes, "validate": _validate, "rate": _rate}


def summary(scenario: Scenario) -> str:
    """What the filler beside a twin's fault is, in a sentence, for the README."""

    from flows.builder.lib import ProcessGroup

    build = scenario.build
    whole, core = ProcessGroup(scenario.id, scenario.group_name), ProcessGroup(scenario.id, scenario.group_name)
    build(whole)
    build.core(core)
    counts = {shape: build.shapes.count(shape) for shape in SHAPES if shape in build.shapes}
    names = {"chain": "chains", "lanes": "lane fan-outs", "validate": "validations into a DLQ",
             "rate": "rate limits"}

    return (f"Beside {scenario.twin}'s {len(core.processors)} processors, {len(whole.processors) - len(core.processors)} "
            f"healthy ones in {len(build.shapes)} small flows of their own: "
            + ", ".join(f"{n} {names[shape]}" for shape, n in counts.items()) + ".")
