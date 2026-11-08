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
  validate  a DebugFlow that fails 1 in 20 into a connected DLQ, which
            logs at INFO: a WARN bulletin would be Concerning Health
  rate      a ControlRate far above its load

Each source makes one to three 1 KB FlowFiles a second, far inside the
simulator's throughput cap per connection and its CPU per FlowFile, so
100 processors of filler cost the node about a tenth of a core.

A twin can also have a *decoy*: one more healthy flow, beside the fault,
that looks like it. Its queue has a chart-only KPI so the agent reads it,
a trap says why it is not the fault, and a wrong fix changes it:

  batch     a merge whose bin fills to 5,000 FlowFiles every 500 s and
            empties: a backlog that is only batching
  retry     a validator whose failures loop back into it, one in 20,
            with room for both: a self-loop that stays near empty

A *control* (`control()`) is a group of filler and nothing else, family
NONE: it must stay quiet however large the pack around it.

A *nested twin* (`nest()`, NFH-96) puts the twin's fault one or two
groups down, "Sxx Region" and in it "Sxx Site", whole, its source
included, so it behaves as in the small scenario. Filler is shared out
between the levels, and into each group a *crossing* runs: a source in
the group above, an input port, a few stages, an output port and a sink
above again, so the ports carry FlowFiles too. A control can be nested
the same way.

    S98 = scale.nest(p1_throughput.S01, "S98", depth=2, processors=60)
"""

import dataclasses
import random

from flows.builder.lib import COLUMN_SPACING, ROW_SPACING, ProcessGroup
from flows.scenarios.base import Kpi, Scenario, action
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
        return self._pg.connect(_unwrapped(source), _unwrapped(destination), relationships, self._rename(name),
                                **options)

    def group(self, label, name, *args, **options):
        return _Renamed(self._pg.group(label, self._rename(name), *args, **options), self._rename)

    def input_port(self, key, name):
        return self._pg.input_port(key, self._rename(name))

    def output_port(self, key, name):
        return self._pg.output_port(key, self._rename(name))

    def __getattr__(self, attribute):
        return getattr(self._pg, attribute)


def _unwrapped(end):
    """A connection end, a child group's port's with the child unwrapped."""

    if isinstance(end, tuple) and isinstance(end[0], _Renamed):
        return end[0]._pg, end[1]

    return end


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
    """The build of a size twin: its twin's build, renamed, then its decoy, then filler up to `processors`."""

    def __init__(self, small: Scenario, scenario_id: str, processors: int, decoy: str = ""):
        self.small, self.id, self.processors, self.decoy = small, scenario_id, processors, decoy
        self.rename = renamer(small.id, scenario_id)
        self.shapes = []

    def core(self, pg) -> None:
        self.small.build(_Renamed(pg, self.rename))

    def __call__(self, pg) -> None:
        self.core(pg)

        if self.decoy:
            DECOYS[self.decoy].build(_Flow(pg, "decoy", f"{self.id} {DECOY}", _slug(self.id, DECOY), column=1))

        self.shapes = filler(pg, self.id, self.processors - len(pg.processors), column=2 if self.decoy else 1)


def twin(small: Scenario, scenario_id: str, *, processors: int, title: str = "", decoy: str = "") -> Scenario:
    """
    `small`'s fault as scenario `scenario_id`, in a group of about
    `processors` processors; with `decoy` (DECOYS), beside a healthy flow
    that looks like it, whose KPI, trap and wrong fix follow the twin's.
    """

    rename = renamer(small.id, scenario_id)
    kpis = tuple(dataclasses.replace(k, scenario=scenario_id, component=rename(k.component)) for k in small.kpis)
    traps, wrong = rename(small.traps), rename(small.wrong)

    if decoy:
        made = DECOYS[decoy]
        name, slug = f"{scenario_id} {DECOY}", _slug(scenario_id, DECOY)
        kpis += (Kpi(scenario_id, "connection", f"{slug}-{made.queue}", "queued_count", alert=False, primary=False),)
        traps += (made.trap.format(name=name, slug=slug),)
        wrong += (made.wrong(name, rename(small.expected)),)

    return dataclasses.replace(
        small,
        id=scenario_id,
        title=title or f"{small.title} in {processors} Processors",
        build=Twinned(small, scenario_id, processors, decoy),
        kpis=kpis,
        expected=rename(small.expected),
        acceptable=rename(small.acceptable),
        traps=traps,
        wrong=wrong,
        faults=rename(small.faults),
        edges=rename(small.edges),
        twin=small.id,
    )


class Nested:
    """The build of a nested twin: its groups, its twin's build, renamed, in the deepest, then filler and crossings."""

    def __init__(self, small: Scenario, scenario_id: str, depth: int, processors: int):
        self.small, self.id, self.depth, self.processors = small, scenario_id, depth, processors
        self.rename = renamer(small.id, scenario_id)
        self.decoy = ""
        self.shapes = []

    def core(self, pg) -> None:
        self.small.build(_Renamed(pg, self.rename))

    def __call__(self, pg) -> None:
        groups = levels(pg, self.id, self.depth)
        self.core(groups[-1])
        self.shapes = fill_levels(groups, self.id, self.processors - count(groups[-1]))


def nest(small: Scenario, scenario_id: str, *, depth: int, processors: int, title: str = "") -> Scenario:
    """
    `small`'s fault as scenario `scenario_id`, `depth` - 1 groups down
    (2 or 3 levels), among about `processors` processors in all.
    """

    if not 2 <= depth <= len(LEVELS) + 1:
        raise ValueError(f"{scenario_id}: depth is 2 or 3, the group and one or two inside it")

    down = {2: "One Group Down", 3: "Two Groups Down"}[depth]

    return dataclasses.replace(twin(small, scenario_id, processors=processors),
                               title=title or f"{small.title} {down}",
                               build=Nested(small, scenario_id, depth, processors))


LEVELS = ("Region", "Site")
CROSSING_STAGES = 3          # a crossing's stages inside its group, beside its source and sink above


def count(pg) -> int:
    """The processors in a group and the groups inside it."""

    return len(pg.processors) + sum(count(child) for child in pg.groups)


def levels(pg, scenario_id: str, depth: int) -> list:
    """[pg, its "Sxx Region", that's "Sxx Site"], `depth` of them."""

    groups = [pg]

    for level in LEVELS[:depth - 1]:
        groups.append(groups[-1].group(level.lower(), f"{scenario_id} {level}"))

    return groups


def fill_levels(groups: list, scenario_id: str, processors: int, offset: int = 0) -> list:
    """
    About `processors` healthy processors shared out between `groups`, a
    crossing into each group but the first before its filler. Returns
    the shapes built, a crossing as "crossing".
    """

    rng = random.Random(f"crossing:{scenario_id}")
    left = processors - (CROSSING_STAGES + 2) * (len(groups) - 1)
    share, n, built = left // len(groups), offset, []

    for index, group in enumerate(groups):
        if index:
            _crossing(groups[index - 1], group, scenario_id, _domain(n), index, rng)
            built.append("crossing")
            n += 1

        shapes = filler(group, scenario_id, share + (left - share * len(groups) if not index else 0),
                        offset=n, seed=f":{index}" if index else "")
        built += shapes
        n += len(shapes)

    return built


def _crossing(parent, child, scenario_id: str, domain: str, index: int, rng) -> None:
    """A source in `parent`, into `child` through an input port, CROSSING_STAGES stages, out to a sink in `parent`."""

    key, level = f"x{index}", LEVELS[index - 1]
    outer = _Flow(parent, key, f"{scenario_id} {domain}", _slug(scenario_id, domain), column=0)
    inner = _Flow(child, key, outer.name, outer.slug, column=-1)
    # Laid out down the flows' own columns, the group under its source and the ports at the ends of
    # the stages: where the builder puts them by default they covered the source, a stage and filler.
    child.position = (0.0, ROW_SPACING)
    child.input_port(f"{key}.in", f"{scenario_id} In {level}")["position"] = {"x": -COLUMN_SPACING, "y": 0.0}
    out = child.output_port(f"{key}.out", f"{scenario_id} Out {level}")
    outer.connect(outer.source(rng), (child, f"{key}.in"))
    inner.links, inner.rows, after = outer.links, 1, f"{key}.in"

    for verb in rng.sample(VERBS, CROSSING_STAGES):
        made = inner.place(stage, verb.lower(), verb, period="0 sec")
        inner.connect(after, made, () if after == f"{key}.in" else ("success",))
        after = made

    out["position"] = {"x": -COLUMN_SPACING, "y": inner.rows * ROW_SPACING}
    inner.connect(after, f"{key}.out")
    outer.links, outer.rows = inner.links, 3  # a row's room below the group for its connections' labels
    outer.connect((child, f"{key}.out"), outer.place(sink, "store", "Store", period="0 sec"), ())


class Filled:
    """
    The build of a control: `processors` of filler and nothing else, its
    domains from DOMAINS[offset] on; at `depth` 2 or 3, in groups inside it too.
    """

    def __init__(self, scenario_id: str, processors: int, offset: int, depth: int = 1):
        self.id, self.processors, self.offset, self.depth = scenario_id, processors, offset, depth
        self.shapes = []

    def __call__(self, pg) -> None:
        if self.depth == 1:
            self.shapes = filler(pg, self.id, self.processors, offset=self.offset)

        else:
            self.shapes = fill_levels(levels(pg, self.id, self.depth), self.id, self.processors, self.offset)


ALERT = 2000        # a control's alert, on its first flow's first queue: near empty when healthy


def control(scenario_id: str, *, processors: int, offset: int = 0, title: str = "", depth: int = 1) -> Scenario:
    """
    A healthy group of about `processors` processors of filler, as control
    `scenario_id`; at `depth` 2 or 3, shared out with the groups inside it.
    """

    build = Filled(scenario_id, processors, offset, depth)
    group = ProcessGroup(scenario_id, scenario_id)
    build(group)
    domain = _domain(offset)
    traps = ["Every queue here is near empty and every rate limit far above its load: nothing needs changing."]

    if "validate" in build.shapes:
        traps.insert(0, "The DLQs take one FlowFile in 20 from their validations by design: that is not data loss.")

    if depth > 1:
        traps.append("The input and output ports only carry FlowFiles between the groups; there is nothing to "
                     "change on them, and the groups inside are as healthy as this one.")

    return Scenario(
        id=scenario_id,
        title=title or f"Healthy {domain} Group",
        symptom="healthy",
        fault=f"None. {count(group)} processors in {_flows(build.shapes)}, each far inside its capacity.",
        build=build,
        kpis=(Kpi(scenario_id, "connection", f"{_slug(scenario_id, domain)}-1", "queued_count", above=ALERT),),
        family="NONE",
        detect_within=7200,
        expected=(action("NO_ACTION"),),
        traps=tuple(traps),
    )


def _domain(n: int) -> str:
    return DOMAINS[n % len(DOMAINS)] + (f" {n // len(DOMAINS) + 1}" if n >= len(DOMAINS) else "")


def _slug(scenario_id: str, domain: str) -> str:
    return f"{scenario_id.lower()}-{domain.lower().replace(' ', '-')}"


def filler(pg, scenario_id: str, count: int, *, offset: int = 0, column: int = 1, seed: str = "") -> list:
    """
    About `count` healthy processors in small flows of their own, each in
    its own column from `column` on, its names and shapes from DOMAINS
    and SHAPES at `offset` on. Returns the shapes built, in order.
    """

    rng = random.Random(f"filler:{scenario_id}{seed}")
    built = []

    while count >= 3:
        shape = SHAPES[(len(built) + offset) % len(SHAPES)]
        size = {"chain": 2 + rng.randint(2, 6), "lanes": 4 + rng.randint(2, 4), "validate": 6, "rate": 4}[shape]

        if count - size < 3:
            shape, size = "chain", count

        index = len(built)
        domain = _domain(index + offset)
        flow = _Flow(pg, f"f{index}", f"{scenario_id} {domain}", _slug(scenario_id, domain), column=index + column)
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
    # At INFO: a healthy reject rate is no bulletin, so the deployment stays Good Health (NFH-98).
    reject = flow.place(lambda pg, key, name: dlq(pg, name, key=key, level="info"), "dlq", "Reject DLQ")
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


def _batch(flow) -> None:
    # 10/s into a bin of 5,000 to 6,000: the bin fills in 500 s and goes,
    # so its input connection climbs to 5,000 and drops, well inside its
    # 10,000 threshold.
    gen = flow.source(None, batch=10)
    fmt = flow.place(stage, "format", "Format", period="0 sec")
    merge = flow.place(lambda pg, key, name: pg.processor(key, name, "MergeContent", {
        "Merge Strategy": "Bin-Packing Algorithm",
        "Merge Format": "Binary Concatenation",
        "Minimum Number of Entries": "5000",
        "Maximum Number of Entries": "6000",
        "Max Bin Age": "15 min",
        "Maximum number of Bins": "5",
    }, period="0 sec", tasks=1, auto_terminate=("original", "failure")), "batch", "Batch")
    flow.connect(gen, fmt)
    flow.connect(fmt, merge)
    flow.connect(merge, flow.place(sink, "upload", "Upload", period="0 sec"), ("merged",))


def _retry(flow) -> None:
    # 2/s into a validator that fails one in 20 back into itself, at a
    # 0 sec schedule: it has room for the retries, so its loop stays near
    # empty.
    gen = flow.source(None, batch=2)
    send = flow.place(debug_flow, "send", "Send", successes=19, failures=1, period="0 sec", tasks=1)
    flow.connect(gen, send)
    flow.connect(send, send, ("failure",))
    flow.connect(send, flow.place(sink, "archive", "Archive", period="0 sec"))


@dataclasses.dataclass(frozen=True)
class Decoy:
    build: object           # (a _Flow) -> None
    queue: int              # its KPI's connection, by number within the flow
    trap: str               # formatted with the flow's name and slug
    wrong: object           # (the flow's name, the twin's expected fixes) -> the fix that changes the decoy


DECOY = "Exports"
DECOYS = {
    "batch": Decoy(
        _batch, 2,
        "{name} Batch holds up to 5,000 FlowFiles in `{slug}-2` while its bin fills, then sends them on "
        "every 500 s: that queue is batching, not a backlog.",
        lambda name, expected: action("SET_PROCESSOR_PROPERTY", f"{name} Batch",
                                      property="Minimum Number of Entries", value="1000"),
    ),
    "retry": Decoy(
        _retry, 2,
        "{name} Send loops its failures back into itself too, but one in 20 fails and it has room for the "
        "retries: `{slug}-2` stays near empty, and it is not the fault.",
        lambda name, expected: action("ROUTE_RELATIONSHIP_TO_DLQ", f"{name} Send", relationship="failure",
                                      dlq=next(a["dlq"] for a in expected if a.get("dlq"))),
    ),
}


SHAPE_NAMES = {"chain": ("chain", "chains"), "lanes": ("lane fan-out", "lane fan-outs"),
               "validate": ("validation into a DLQ", "validations into a DLQ"), "rate": ("rate limit", "rate limits"),
               "crossing": ("crossing into a group", "crossings into a group")}
DECOY_NAMES = {"batch": "a merge that batches", "retry": "a validator that retries its own failures"}


def _shapes(shapes: list) -> str:
    counts = {shape: shapes.count(shape) for shape in SHAPES + ("crossing",) if shape in shapes}

    return ", ".join(f"{n if n > 1 else 'one'} {SHAPE_NAMES[shape][n > 1]}" for shape, n in counts.items())


def _flows(shapes: list) -> str:
    return "one small flow of its own" if len(shapes) == 1 else f"{len(shapes)} small flows of their own"


def _nesting(scenario: Scenario) -> str:
    """Where its groups are, for the README: "" for a flat one."""

    depth = getattr(scenario.build, "depth", 1)
    names = " and in it ".join(f"{scenario.id} {level}" for level in LEVELS[:depth - 1])

    each = "each " if depth > 2 else ""

    return f" The group holds {names}, {each}fed by a crossing through its input and output ports." if names else ""


def summary(scenario: Scenario) -> str:
    """What the filler beside a twin's fault is, in a sentence, for the README."""

    build = scenario.build
    whole, core = ProcessGroup(scenario.id, scenario.group_name), ProcessGroup(scenario.id, scenario.group_name)
    build(whole)
    build.core(core)
    beside = count(whole) - count(core)
    decoy = ""

    if build.decoy:
        made = ProcessGroup(scenario.id, scenario.group_name)
        DECOYS[build.decoy].build(_Flow(made, "decoy", f"{scenario.id} {DECOY}", _slug(scenario.id, DECOY), 1))
        beside -= len(made.processors)
        decoy = f"a decoy of {len(made.processors)}, {scenario.id} {DECOY}, {DECOY_NAMES[build.decoy]}, and "

    if isinstance(build, Nested):
        where = " / ".join(f"{scenario.id} {level}" for level in LEVELS[:build.depth - 1])

        return (f"{scenario.twin}'s {count(core)} processors, whole, in {where}; around them, {beside} healthy ones "
                f"in {_flows(build.shapes)}: {_shapes(build.shapes)}.")

    return (f"Beside {scenario.twin}'s {count(core)} processors, {decoy}{beside} "
            f"healthy ones in {_flows(build.shapes)}: {_shapes(build.shapes)}.")


def walkthrough(scenario: Scenario) -> str:
    """A control's walkthrough for the README: what its filler is, and why nothing in it needs a fix."""

    group = ProcessGroup(scenario.id, scenario.group_name)
    scenario.build(group)
    alert = next(k for k in scenario.kpis if k.alert)

    return (f"{count(group)} healthy processors in {_flows(scenario.build.shapes)}: "
            f"{_shapes(scenario.build.shapes)}.{_nesting(scenario)} Each source makes one to three FlowFiles a "
            f"second, and every stage has room to spare. The alert, at {alert.above:,.0f} FlowFiles in "
            f"`{alert.component}`, should never fire, and the only right answer is no action.")
