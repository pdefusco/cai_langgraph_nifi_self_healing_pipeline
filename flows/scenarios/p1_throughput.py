"""
Pack p1-throughput: consumers that cannot keep up.

All four show the same headline symptom, a growing queue, and differ in
which lever actually fixes it. That is the point: the agent has to read
the processor settings, not just the alert.
"""

from flows.scenarios.base import Kpi, Pack, Scenario, action
from flows.scenarios.common import generator, sink, stage


def build_s01(pg):
    # 3/s in, 1 task x 1 run/s = 1/s out.
    generator(pg, "S01 Source", period="1 sec", batch=3)
    sink(pg, "consumer", "S01 Consumer", period="1 sec", tasks=1)
    pg.connect("gen", "consumer", ["success"], "s01-backlog")


S01 = Scenario(
    id="S01",
    title="Slow Consumer",
    symptom="backlog",
    fault="The consumer runs one concurrent task while its input arrives three times faster.",
    build=build_s01,
    kpis=(
        Kpi("S01", "connection", "s01-backlog", "queued_count", above=500),
        Kpi("S01", "processor", "S01 Consumer", "flowfiles_in", alert=False, good="up", primary=False),
    ),
    family="FLOW_EDIT",
    expected=(action("SET_CONCURRENT_TASKS", "S01 Consumer", value=4),),
    acceptable=(action("SET_RUN_SCHEDULE", "S01 Consumer", value="250 ms"),),
    traps=(
        "Raising the back-pressure threshold on s01-backlog only lets the backlog grow larger.",
        "Restarting the deployment does not drain a backlog; it pauses the consumer.",
    ),
)


def build_s02(pg):
    # 1/s in, 4 tasks x 1 run per 10 s = 0.4/s out. Even 8 tasks (the
    # catalog maximum) only reach 0.8/s, so concurrency cannot fix it.
    generator(pg, "S02 Source", period="1 sec", batch=1)
    sink(pg, "drain", "S02 Drain", period="10 sec", tasks=4)
    pg.connect("gen", "drain", ["success"], "s02-backlog")


S02 = Scenario(
    id="S02",
    title="Slow Run Schedule",
    symptom="backlog",
    fault="The drain is scheduled every 10 seconds, so it is idle most of the time.",
    build=build_s02,
    kpis=(
        Kpi("S02", "connection", "s02-backlog", "queued_count", above=500),
        Kpi("S02", "processor", "S02 Drain", "flowfiles_in", alert=False, good="up", primary=False),
    ),
    family="FLOW_EDIT",
    expected=(action("SET_RUN_SCHEDULE", "S02 Drain", value="1 sec"),),
    acceptable=(action("SET_RUN_SCHEDULE", "S02 Drain", value="0 sec"),),
    traps=("SET_CONCURRENT_TASKS to the maximum of 8 still leaves the drain at 0.8/s against 1/s in.",),
)


def build_s03(pg):
    # 2/s in. Enrich 4/s, Transform 1/s, Publish 4/s: Transform is the
    # bottleneck, so only s03-to-transform grows.
    generator(pg, "S03 Source", period="1 sec", batch=2)
    stage(pg, "enrich", "S03 Enrich", period="1 sec", tasks=4)
    stage(pg, "transform", "S03 Transform", period="1 sec", tasks=1)
    sink(pg, "publish", "S03 Publish", period="1 sec", tasks=4)
    pg.connect("gen", "enrich", ["success"], "s03-to-enrich")
    pg.connect("enrich", "transform", ["success"], "s03-to-transform")
    pg.connect("transform", "publish", ["success"], "s03-to-publish")


S03 = Scenario(
    id="S03",
    title="Pipeline Bottleneck",
    symptom="backlog",
    fault="The middle stage of a three-stage pipeline has a quarter of its neighbours' capacity.",
    build=build_s03,
    kpis=(
        Kpi("S03", "connection", "s03-to-enrich", "queued_count", above=300, primary=False),
        Kpi("S03", "connection", "s03-to-transform", "queued_count", above=300),
        Kpi("S03", "connection", "s03-to-publish", "queued_count", above=300, primary=False),
    ),
    family="FLOW_EDIT",
    expected=(action("SET_CONCURRENT_TASKS", "S03 Transform", value=3),),
    acceptable=(action("SET_RUN_SCHEDULE", "S03 Transform", value="250 ms"),),
    traps=("Adding tasks to S03 Enrich or S03 Publish changes nothing; neither has a backlog.",),
)


def build_s04(pg):
    # SIB's oscillator: 600 FlowFiles every 10 minutes, drained at 1/s,
    # so the queue saws between 600 and 0 and breaches 300 half the time.
    generator(pg, "S04 Burst Source", period="600 sec", batch=600)
    sink(pg, "drain", "S04 Drain", period="1 sec", tasks=1)
    pg.connect("gen", "drain", ["success"], "s04-burst")


S04 = Scenario(
    id="S04",
    title="Bursty Oscillator",
    symptom="backlog",
    fault="Bursts of 600 drain at one per second, so every burst waits up to ten minutes.",
    build=build_s04,
    kpis=(Kpi("S04", "connection", "s04-burst", "queued_count", above=300),),
    family="FLOW_EDIT",
    expected=(action("SET_CONCURRENT_TASKS", "S04 Drain", value=4),),
    acceptable=(action("SET_RUN_SCHEDULE", "S04 Drain", value="250 ms"),),
    traps=("A sawtooth is not a leak: judge the fix by the peak and the drain time, not one bucket.",),
)


PACK = Pack(
    name="p1-throughput",
    title="Throughput",
    scenarios=(S01, S02, S03, S04),
)
