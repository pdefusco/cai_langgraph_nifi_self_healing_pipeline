"""
Pack p10-iot: sensor readings through enrichment and validation, with
the fault deep in the pipeline.

S26's device lookup calls an endpoint that is gone, and routes every
failure back to itself: the retry loop fills, the lookup stops, and the
backlog reaches ingest four hops upstream. S27 rejects 15% of readings
at a schema check whose failure relationship is auto-terminated, so they
vanish: no queue grows, and only the counts at the two ends disagree.
"""

from flows.scenarios.base import Kpi, Pack, Scenario, action
from flows.scenarios.common import debug_flow, dlq, generator, invoke_http, sink, stage

READING = '{"device": "d-17", "site": "plant-3", "temp_c": 21.4, "ts": 1760000000}'


def build_s26(pg):
    # 50/s of readings. Nothing listens on the lookup's port, so every
    # call is refused at once and goes to Failure, which loops back: the
    # loop holds everything that arrives and is full in 200 s. The lookup
    # is then back-pressured, and each queue before it fills in 200 s:
    # ingest is full about 17 minutes in.
    generator(pg, "S26 Sensor Readings", period="1 sec", batch=50, text=READING)
    pg.processor("parse", "S26 Parse Reading", "EvaluateJsonPath", {
        "Destination": "flowfile-attribute",
        "Return Type": "auto-detect",
        "device.id": "$.device",
    }, period="0 sec", tasks=1)
    stage(pg, "units", "S26 Convert Units", period="0 sec")
    stage(pg, "site", "S26 Tag Site", period="0 sec")
    invoke_http(pg, "lookup", "S26 Enrich Device", "http://localhost:9999/devices",
                connect_timeout="5 secs", period="0 sec", tasks=1, auto_terminate=("Original",))
    stage(pg, "window", "S26 Window Stats", period="0 sec")
    stage(pg, "anomaly", "S26 Detect Anomaly", period="0 sec")
    pg.processor("merge", "S26 Merge Readings", "MergeContent", {
        "Merge Strategy": "Bin-Packing Algorithm",
        "Merge Format": "Binary Concatenation",
        "Minimum Number of Entries": "1000",
        "Maximum Number of Entries": "5000",
        "Max Bin Age": "1 min",
        "Maximum number of Bins": "5",
    }, period="0 sec", tasks=1, auto_terminate=("original", "failure"))
    sink(pg, "store", "S26 Store Readings", period="0 sec")
    dlq(pg, "S26 Device DLQ")
    pg.connect("gen", "parse", ["success"], "s26-ingest")
    pg.connect("parse", "units", ["matched"], "s26-parsed")
    pg.connect("parse", "dlq", ["unmatched", "failure"], "s26-unparsed")
    pg.connect("units", "site", ["success"], "s26-converted")
    pg.connect("site", "lookup", ["success"], "s26-to-enrich")
    pg.connect("lookup", "lookup", ["Failure", "No Retry", "Retry"], "s26-retry-loop")
    pg.connect("lookup", "window", ["Response"], "s26-enriched")
    pg.connect("window", "anomaly", ["success"], "s26-windowed")
    pg.connect("anomaly", "merge", ["success"], "s26-checked")
    pg.connect("merge", "store", ["merged"], "s26-merged")


S26 = Scenario(
    id="S26",
    title="Retry Storm On Dead Endpoint",
    symptom="failure_loop",
    fault=("The device lookup's endpoint refuses every call, and Failure, Retry and No Retry all "
           "loop back to it: the loop fills and the backlog reaches ingest."),
    build=build_s26,
    kpis=(
        Kpi("S26", "connection", "s26-ingest", "queued_count", above=8000),
        Kpi("S26", "connection", "s26-retry-loop", "queued_count", alert=False, primary=False),
        Kpi("S26", "connection", "s26-to-enrich", "queued_count", alert=False, primary=False),
    ),
    family="FLOW_EDIT",
    kind="cascade",
    expected=(action("ROUTE_RELATIONSHIP_TO_DLQ", "S26 Enrich Device",
                     relationship=["Failure", "No Retry", "Retry"], dlq="S26 Device DLQ"),),
    acceptable=(),
    traps=(
        "More tasks on the lookup do nothing: it is blocked by its own full loop, not slow.",
        "A higher threshold on the loop only buys time; the endpoint refuses every call.",
    ),
    wrong=(
        action("SET_CONCURRENT_TASKS", "S26 Enrich Device", value=4),
        action("SET_CONCURRENT_TASKS", "S26 Parse Reading", value=4),
    ),
)


def build_s27(pg):
    # 10/s through six stages to a schema check that rejects 3 in 20
    # (15%) and auto-terminates them: the store gets 8.5/s and nothing
    # queues. Its reject DLQ exists but nothing is routed to it.
    generator(pg, "S27 Meter Readings", period="1 sec", batch=10, text=READING)
    pg.processor("parse", "S27 Parse Meter", "EvaluateJsonPath", {
        "Destination": "flowfile-attribute",
        "Return Type": "auto-detect",
        "meter.id": "$.device",
    }, period="0 sec", tasks=1)
    stage(pg, "units", "S27 Convert Units", period="0 sec")
    stage(pg, "site", "S27 Tag Site", period="0 sec")
    stage(pg, "dedupe", "S27 Dedupe Readings", period="0 sec")
    debug_flow(pg, "schema", "S27 Schema Check", successes=17, failures=3,
               period="0 sec", tasks=1, auto_terminate=("failure",))
    stage(pg, "window", "S27 Window Stats", period="0 sec")
    stage(pg, "anomaly", "S27 Detect Anomaly", period="0 sec")
    sink(pg, "store", "S27 Store Readings", period="0 sec")
    dlq(pg, "S27 Parse DLQ")
    dlq(pg, "S27 Reject DLQ", connected=False, key="rejects")
    pg.connect("gen", "parse", ["success"], "s27-ingest")
    pg.connect("parse", "units", ["matched"], "s27-parsed")
    pg.connect("parse", "dlq", ["unmatched", "failure"], "s27-unparsed")
    pg.connect("units", "site", ["success"], "s27-converted")
    pg.connect("site", "dedupe", ["success"], "s27-tagged")
    pg.connect("dedupe", "schema", ["success"], "s27-to-check")
    pg.connect("schema", "window", ["success"], "s27-valid")
    pg.connect("window", "anomaly", ["success"], "s27-windowed")
    pg.connect("anomaly", "store", ["success"], "s27-checked")


S27 = Scenario(
    id="S27",
    title="Silent Drop In Validation",
    symptom="data_loss",
    fault=("The schema check, six stages in, auto-terminates its failure relationship, so 15% of "
           "readings vanish without a queue growing or an error."),
    build=build_s27,
    kpis=(
        Kpi("S27", "processor", "S27 Meter Readings", "flowfiles_out", alert=False, good="up",
            primary=False),
        Kpi("S27", "processor", "S27 Store Readings", "flowfiles_in", alert=False, good="up",
            primary=False),
        # Decides: once rejects reach it, they are accounted for, not lost.
        Kpi("S27", "processor", "S27 Reject DLQ", "flowfiles_in", alert=False, good="up"),
    ),
    family="FLOW_EDIT",
    kind="cascade",
    expected=(action("ROUTE_RELATIONSHIP_TO_DLQ", "S27 Schema Check", relationship="failure",
                     dlq="S27 Reject DLQ"),),
    acceptable=(),
    traps=(
        "The loss shows at the store, three stages after the cause; the stages between are healthy.",
        "More tasks on the schema check reject just as many; it is not slow.",
    ),
    wrong=(
        action("SET_CONCURRENT_TASKS", "S27 Schema Check", value=4),
        action("SET_CONCURRENT_TASKS", "S27 Store Readings", value=4),
    ),
)


PACK = Pack(
    name="p10-iot",
    title="IoT pipeline cascades",
    scenarios=(S26, S27),
    wave=2,
)
