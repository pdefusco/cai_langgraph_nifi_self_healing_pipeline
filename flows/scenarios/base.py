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

A *pack* is one deployment: a root process group holding several
scenarios, and its sizing.

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

    def __post_init__(self):
        if self.kind not in ("connection", "processor", "system"):
            raise ValueError(f"{self.scenario}: bad KPI kind {self.kind!r}")

        if self.alert and self.above is None and self.below is None:
            raise ValueError(f"{self.scenario}: an alerting KPI needs a threshold")

        if self.good not in ("down", "up"):
            raise ValueError(f"{self.scenario}: good must be 'down' or 'up'")

    @property
    def key(self) -> str:
        return f"{self.scenario}:{self.kind}:{self.component}:{self.metric}"


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
    parameters: dict = field(default_factory=dict)

    @property
    def group_name(self) -> str:
        return f"{self.id} {self.title}"


@dataclass(frozen=True)
class Pack:
    name: str                # "p1-throughput"; the flow and deployment are PREFIX + name
    title: str
    scenarios: tuple
    cluster_size: str = "EXTRA_SMALL"
    nodes: int = 1
    parameter_context: str | None = None
    has_listen_components: bool = False

    def scenario(self, scenario_id: str) -> Scenario:
        for scenario in self.scenarios:
            if scenario.id == scenario_id:
                return scenario

        raise KeyError(scenario_id)

    @property
    def kpis(self) -> list:
        return [k for s in self.scenarios for k in s.kpis]

    @property
    def parameters(self) -> dict:
        merged = {}

        for scenario in self.scenarios:
            merged.update(scenario.parameters)

        return merged


def action(kind: str, target: str = "", **fields) -> dict:
    """A catalog action, as `expected` and `acceptable` record them."""

    return {"action": kind, "target": target, **fields}
