"""
What a scenario and a pack declare.

A *scenario* is one process group with one injected fault (or none, for
the control). It declares:

  build(pg)          adds its processors and connections to `pg`
  kpis               the KPIs that make its charts exist (a chart only
                     exists for a component with a KPI configured)
  expected           the known-correct fix, as catalog actions. A design
                     label for evaluation (Phase 3), never shown to the agent.
  acceptable         other fixes that are also correct
  traps              plausible fixes that are wrong, and why
  wrong              some of those traps as catalog actions, which the
                     tests apply to check that they do not clear the fault
  kind               how the fault presents (KINDS): classic, one fault where
                     it shows; cascade, the symptom stages away from the
                     cause; compound, several faults; intermittent, it comes
                     and goes; slow_burn, it takes hours to show; control,
                     nothing is wrong
  faults             for a compound fault, the fixes that must all be
                     applied before it clears; one fault has none, and any
                     of `expected` or `acceptable` clears it
  detect_within      seconds from planting by which an incident should open
                     (a control: during which none should)
  clear_within       seconds after the last fix by which its KPIs are healthy

A *pack* is one deployment: a root process group holding several
scenarios, and its sizing. Its `wave` is the rollout batch it is
provisioned in: a provision script given no --pack covers wave 1 only,
so a later pack goes live only when it is named.

KPIs name their component; the metric is a logical name resolved to a
DataFlow metricId by nifi_healer/kpi/metrics.py, so an unverified id can
be corrected in one place (provision/out/metric_overrides.json).
"""

from dataclasses import dataclass, field
from typing import Callable


@dataclass(frozen=True)
class Kpi:
    scenario: str
    kind: str                # connection | processor | system
    component: str           # the component's name; "" for system metrics
    metric: str              # logical metric, see kpi/metrics.py METRICS
    above: float | None = None
    below: float | None = None
    tolerance_seconds: int = 60
    alert: bool = True       # False: chart only, read by detectors
    good: str = "down"       # which direction is better: down | up
    primary: bool = True     # primary KPIs decide the outcome verdict
    rollup: bool = False     # True: only feeds the group's in/out roll-up and the agent's readings

    def __post_init__(self):
        if self.kind not in ("connection", "processor", "system"):
            raise ValueError(f"{self.scenario}: bad KPI kind {self.kind!r}")

        if self.alert and self.above is None and self.below is None:
            raise ValueError(f"{self.scenario}: an alerting KPI needs a threshold")

        if self.rollup and (self.alert or self.primary):
            raise ValueError(f"{self.scenario}: a roll-up KPI neither alerts nor decides")

        if self.good not in ("down", "up"):
            raise ValueError(f"{self.scenario}: good must be 'down' or 'up'")

    @property
    def key(self) -> str:
        return f"{self.scenario}:{self.kind}:{self.component}:{self.metric}"


KINDS = ("classic", "cascade", "compound", "intermittent", "slow_burn", "control")


@dataclass(frozen=True)
class Scenario:
    id: str                  # "S01"
    title: str               # becomes the process group name: "S01 Slow Consumer"
    symptom: str             # playbook tag: backlog, backpressure, failure_loop, ...
    fault: str               # one sentence: what is wrong
    build: Callable
    kpis: tuple
    family: str              # FLOW_EDIT | PARAMETER | SIZING | NONE
    expected: tuple = ()
    acceptable: tuple = ()
    traps: tuple = ()
    wrong: tuple = ()
    parameters: dict = field(default_factory=dict)
    edges: tuple = ()        # (entry names, exit names), when the topology misleads
    kind: str = ""           # one of KINDS; "" is control for family NONE, else classic
    faults: tuple = ()
    detect_within: int = 1800
    clear_within: int = 900

    def __post_init__(self):
        if not self.kind:
            object.__setattr__(self, "kind", "control" if self.family == "NONE" else "classic")

        if self.kind not in KINDS:
            raise ValueError(f"{self.id}: kind must be one of {', '.join(KINDS)}, got {self.kind!r}")

        if (self.kind == "control") != (self.family == "NONE"):
            raise ValueError(f"{self.id}: a control, and only a control, has family NONE")

        if self.kind == "compound" and len(self.faults) < 2:
            raise ValueError(f"{self.id}: a compound fault lists the two or more fixes it needs in faults")

        if self.faults and self.kind != "compound":
            raise ValueError(f"{self.id}: only a compound fault lists faults")

    @property
    def group_name(self) -> str:
        return f"{self.id} {self.title}"

    def flow_edges(self) -> dict:
        """
        {"in": entry processor names, "out": exit processor names, "split":
        the processors that send on more FlowFiles than they take in}.

        An entry takes nothing from another processor and sends to one; an
        exit sends to no other processor (a self-loop is not a way out),
        including a DLQ nothing is routed to yet. What leaves through
        auto-terminated relationships is not counted.
        """

        if self.id not in _EDGES:
            from flows.builder.lib import ProcessGroup
            from flows.builder.processors import SPLITTING, kind_of

            group = ProcessGroup(self.id, self.group_name)
            self.build(group)
            names = {p["identifier"]: p["name"] for p in group.processors.values()}
            links = [(c["source"]["id"], c["destination"]["id"]) for c in group.connections
                     if c["source"]["id"] != c["destination"]["id"]]
            fed = {d for _, d in links}
            feeding = {s for s, _ in links}
            _EDGES[self.id] = {
                "in": [n for i, n in names.items() if i in feeding and i not in fed],
                "out": [n for i, n in names.items() if i not in feeding],
                "split": [p["name"] for p in group.processors.values() if kind_of(p) in SPLITTING],
            }

        found = {side: list(names) for side, names in _EDGES[self.id].items()}

        if self.edges:
            found.update({"in": list(self.edges[0]), "out": list(self.edges[1])})

        return found

    def edge_kpis(self) -> tuple:
        """
        Chart-only KPIs for what the group takes in (its entries' FlowFiles
        Out) and puts out (its exits' FlowFiles In), and for what each split
        sends on, the intake of the stages after it, when no KPI of the
        scenario already charts them.
        """

        edges = self.flow_edges()
        have = {k.key for k in self.kpis}
        wanted = [Kpi(self.id, "processor", name, "flowfiles_out", alert=False, good="up",
                      primary=False, rollup=True) for name in edges["in"] + edges["split"]]
        wanted += [Kpi(self.id, "processor", name, "flowfiles_in", alert=False, good="up",
                       primary=False, rollup=True) for name in edges["out"]]

        return tuple(k for k in wanted if k.key not in have)


# Inferred edges by scenario id: building a group is cheap, but not free.
_EDGES = {}


@dataclass(frozen=True)
class Pack:
    name: str                # "p1-throughput"; the flow and deployment are PREFIX + name
    title: str
    scenarios: tuple
    cluster_size: str = "EXTRA_SMALL"
    nodes: int = 1
    parameter_context: str | None = None
    has_listen_components: bool = False
    wave: int = 1

    def scenario(self, scenario_id: str) -> Scenario:
        for scenario in self.scenarios:
            if scenario.id == scenario_id:
                return scenario

        raise KeyError(scenario_id)

    @property
    def kpis(self) -> list:
        """Every scenario's own KPIs, then the chart-only ones its in/out roll-up needs."""

        return [k for s in self.scenarios for k in s.kpis + s.edge_kpis()]

    @property
    def parameters(self) -> dict:
        merged = {}

        for scenario in self.scenarios:
            merged.update(scenario.parameters)

        return merged


def action(kind: str, target: str = "", **fields) -> dict:
    """A catalog action, as `expected` and `acceptable` record them."""

    return {"action": kind, "target": target, **fields}
