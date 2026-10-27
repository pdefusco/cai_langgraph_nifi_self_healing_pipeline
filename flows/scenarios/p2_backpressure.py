"""
Pack p2-backpressure: queues whose limits, or whose producers, are wrong.

S05 and S06 have thresholds too low for perfectly normal bursts. S07 and
S08 have producers that outrun any consumer the catalog can build, where
raising a threshold would only hide the problem behind a larger queue.
"""

from flows.scenarios.base import Kpi, Pack, Scenario, action
from flows.scenarios.common import generator, sink


def build_s05(pg):
    # Bursts of 50 every 30 s into a queue that back-pressures at 10. The
    # writer (2/s) keeps up on average; the queue is simply too small to
    # hold one burst, so the source is blocked for most of each cycle.
    generator(pg, "S05 Burst Source", period="30 sec", batch=50)
    sink(pg, "writer", "S05 Writer", period="500 ms", tasks=1)
    pg.connect("gen", "writer", ["success"], "s05-buffer", object_threshold=10)


S05 = Scenario(
    id="S05",
    title="Object Threshold Too Low",
    symptom="backpressure",
    fault="The buffer back-pressures at 10 FlowFiles, below the size of one normal burst.",
    build=build_s05,
    kpis=(
        Kpi("S05", "connection", "s05-buffer", "percent_full", above=80),
        Kpi("S05", "connection", "s05-buffer", "queued_count", alert=False, primary=False),
    ),
    family="FLOW_EDIT",
    expected=(action("SET_BACKPRESSURE", "s05-buffer", object_threshold=1000),),
    acceptable=(action("SET_BACKPRESSURE", "s05-buffer", object_threshold=10000),),
    traps=("More writer tasks drain faster but the next burst still hits a 10-FlowFile limit.",),
)


def build_s06(pg):
    # Ten 1 MB files every 10 s into a queue limited to 2 MB.
    generator(pg, "S06 Large File Source", period="10 sec", batch=10, size="1 MB")
    sink(pg, "consumer", "S06 Consumer", period="1 sec", tasks=1)
    pg.connect("gen", "consumer", ["success"], "s06-large-files", size_threshold="2 MB")


S06 = Scenario(
    id="S06",
    title="Data Size Threshold Too Low",
    symptom="backpressure",
    fault="The queue back-pressures at 2 MB, so two of the 1 MB files fill it.",
    build=build_s06,
    kpis=(
        Kpi("S06", "connection", "s06-large-files", "percent_full", above=80),
        Kpi("S06", "connection", "s06-large-files", "queued_bytes", alert=False, primary=False),
    ),
    family="FLOW_EDIT",
    expected=(action("SET_BACKPRESSURE", "s06-large-files", size_threshold="1 GB"),),
    acceptable=(action("SET_BACKPRESSURE", "s06-large-files", size_threshold="100 MB"),),
    traps=("Raising the object threshold does nothing: the limit being hit is the data size.",),
)


def build_s07(pg):
    # One FlowFile every 10 s, copied 100 times: 10.1/s into a sink that
    # runs once a second.
    generator(pg, "S07 Source", period="10 sec", batch=1)
    pg.processor("fanout", "S07 Fan Out", "DuplicateFlowFile", {"Number of Copies": "100"},
                 period="0 sec", tasks=1)
    sink(pg, "sink", "S07 Sink", period="1 sec", tasks=1)
    pg.connect("gen", "fanout", ["success"], "s07-to-fanout")
    pg.connect("fanout", "sink", ["success"], "s07-fanout")


S07 = Scenario(
    id="S07",
    title="Fan-out Flood",
    symptom="backpressure",
    fault="A 100-way fan-out feeds a sink that runs once a second.",
    build=build_s07,
    kpis=(
        Kpi("S07", "connection", "s07-fanout", "queued_count", above=1000),
        Kpi("S07", "connection", "s07-to-fanout", "queued_count", alert=False, primary=False),
    ),
    family="FLOW_EDIT",
    expected=(action("SET_RUN_SCHEDULE", "S07 Sink", value="0 sec"),),
    # The FlowFiles waiting behind the fan-out hold its queue at the limit: 100 ms, 90 ms, 50 ms (20/s), 4 tasks and
    # 8 tasks didn't help in any oracle round, 20 ms (50/s) did in every one.
    acceptable=(action("SET_RUN_SCHEDULE", "S07 Sink", value="20 ms"),),
    traps=(
        "Lowering Number of Copies changes what the flow produces; the fan-out is the business logic.",
        "A larger threshold on s07-fanout just stores the flood.",
    ),
)


def build_s08(pg):
    # A generator on a 0 s schedule produces as fast as NiFi lets it.
    generator(pg, "S08 Runaway Source", period="0 sec", batch=1)
    sink(pg, "consumer", "S08 Consumer", period="1 sec", tasks=2)
    pg.connect("gen", "consumer", ["success"], "s08-flood")


S08 = Scenario(
    id="S08",
    title="Runaway Producer",
    symptom="backpressure",
    fault="The source runs on a 0 second schedule and produces hundreds of FlowFiles a second.",
    build=build_s08,
    kpis=(
        Kpi("S08", "connection", "s08-flood", "percent_full", above=80),
        Kpi("S08", "connection", "s08-flood", "queued_count", alert=False, primary=False),
    ),
    family="FLOW_EDIT",
    expected=(action("SET_RUN_SCHEDULE", "S08 Runaway Source", value="1 sec"),),
    acceptable=(),
    traps=("No consumer the catalog allows (8 tasks) keeps up with a 0 s generator.",),
)


PACK = Pack(
    name="p2-backpressure",
    title="Back pressure",
    scenarios=(S05, S06, S07, S08),
)
