"""
Pack p4-dataloss-latency: faults that raise no obvious alarm.

S13 and S14 lose data quietly: no queue grows, no processor errors. Only
a conservation check (FlowFiles in vs out, upstream vs downstream) shows
it, which is why their KPIs are mostly chart-only processor counters.
S15 and S16 hold data back instead of losing it.
"""

from flows.scenarios.base import Kpi, Pack, Scenario, action
from flows.scenarios.common import debug_flow, dlq, generator, sink, stage


def build_s13(pg):
    # 3/s into a consumer doing 1/s, behind a 30 s expiration: the queue
    # levels off near 90 and 2/s silently expire.
    generator(pg, "S13 Source", period="1 sec", batch=3)
    sink(pg, "consumer", "S13 Consumer", period="1 sec", tasks=1)
    pg.connect("gen", "consumer", ["success"], "s13-expiring", expiration="30 sec")


S13 = Scenario(
    id="S13",
    title="Silent Expiration",
    symptom="data_loss",
    fault="A 30 second FlowFile expiration discards whatever the slow consumer has not reached.",
    build=build_s13,
    kpis=(
        Kpi("S13", "connection", "s13-expiring", "queued_count", alert=False, primary=False),
        Kpi("S13", "processor", "S13 Source", "flowfiles_out", alert=False, good="up", primary=False),
        Kpi("S13", "processor", "S13 Consumer", "flowfiles_in", alert=False, good="up"),
    ),
    family="FLOW_EDIT",
    expected=(
        action("SET_FLOWFILE_EXPIRATION", "s13-expiring", value="0 sec"),
        action("SET_CONCURRENT_TASKS", "S13 Consumer", value=4),
    ),
    acceptable=(),
    traps=(
        "Removing the expiration alone turns silent loss into a growing backlog.",
        "The queue looks healthy (flat, well under threshold); that is the symptom, not reassurance.",
    ),
)


def build_s14(pg):
    # 20% of records fail validation and the failure relationship is
    # auto-terminated: they vanish without a trace.
    generator(pg, "S14 Source", period="1 sec", batch=1)
    debug_flow(pg, "validator", "S14 Validator", successes=4, failures=1,
               period="0 sec", tasks=1, auto_terminate=("failure",))
    sink(pg, "publish", "S14 Publish", period="0 sec")
    dlq(pg, "S14 DLQ", connected=False)
    pg.connect("gen", "validator", ["success"], "s14-input")
    pg.connect("validator", "publish", ["success"], "s14-valid")


S14 = Scenario(
    id="S14",
    title="Auto-terminated Failures",
    symptom="data_loss",
    fault="The validator's failure relationship is auto-terminated, so rejected records disappear.",
    build=build_s14,
    kpis=(
        Kpi("S14", "processor", "S14 Validator", "flowfiles_in", alert=False, good="up", primary=False),
        Kpi("S14", "processor", "S14 Validator", "flowfiles_out", alert=False, good="up"),
    ),
    family="FLOW_EDIT",
    expected=(action("ROUTE_RELATIONSHIP_TO_DLQ", "S14 Validator", relationship="failure", dlq="S14 DLQ"),),
    acceptable=(),
    traps=("Routing failure back to the validator turns silent loss into a retry storm (see S09).",),
)


def build_s15(pg):
    # One FlowFile per 5 s into a bin that waits for 1000 entries and has
    # no age limit: the first batch leaves after 83 minutes.
    generator(pg, "S15 Source", period="5 sec", batch=1)
    pg.processor("batcher", "S15 Batcher", "MergeContent", {
        "Merge Strategy": "Bin-Packing Algorithm",
        "Merge Format": "Binary Concatenation",
        "Minimum Number of Entries": "1000",
        "Maximum Number of Entries": "1000",
        "Maximum number of Bins": "5",
    }, period="0 sec", tasks=1, auto_terminate=("failure", "original"))
    sink(pg, "publish", "S15 Publish", period="0 sec")
    pg.connect("gen", "batcher", ["success"], "s15-to-merge")
    pg.connect("batcher", "publish", ["merged"], "s15-merged")


S15 = Scenario(
    id="S15",
    title="Bin Never Fills",
    symptom="latency",
    fault="MergeContent waits for 1000 entries with no Max Bin Age, at one FlowFile per 5 seconds.",
    build=build_s15,
    kpis=(
        Kpi("S15", "processor", "S15 Batcher", "flowfiles_out", below=0.001,
            tolerance_seconds=900, good="up"),
        Kpi("S15", "processor", "S15 Batcher", "flowfiles_in", alert=False, good="up", primary=False),
    ),
    family="FLOW_EDIT",
    expected=(action("SET_PROCESSOR_PROPERTY", "S15 Batcher", property="Max Bin Age", value="60 sec"),),
    acceptable=(
        action("SET_PROCESSOR_PROPERTY", "S15 Batcher", property="Max Bin Age", value="5 min"),
    ),
    traps=("Minimum Number of Entries 1 ends batching altogether; the batch size is intended.",),
)


def build_s16(pg):
    # The enricher was disabled during maintenance and never re-enabled.
    generator(pg, "S16 Source", period="1 sec", batch=1)
    stage(pg, "enricher", "S16 Enricher", period="0 sec", tasks=1, scheduled_state="DISABLED")
    sink(pg, "publish", "S16 Publish", period="0 sec")
    pg.connect("gen", "enricher", ["success"], "s16-to-enrich")
    pg.connect("enricher", "publish", ["success"], "s16-enriched")


S16 = Scenario(
    id="S16",
    title="Disabled Processor",
    symptom="stuck",
    fault="A processor in the middle of the flow is DISABLED, so everything queues in front of it.",
    build=build_s16,
    kpis=(Kpi("S16", "connection", "s16-to-enrich", "queued_count", above=500),),
    family="FLOW_EDIT",
    expected=(action("SET_SCHEDULED_STATE", "S16 Enricher", value="ENABLED"),),
    acceptable=(),
    traps=("Concurrency or schedule changes on a DISABLED processor have no effect.",),
)


PACK = Pack(
    name="p4-dataloss-latency",
    title="Data loss and latency",
    scenarios=(S13, S14, S15, S16),
)
