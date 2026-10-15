"""
Pack p8-orders: an order pipeline whose fault shows stages away from it.

Both scenarios are realistic multi-stage pipelines (cascades). S22's
merge never fills its bin, and the back pressure it holds reaches the
ingest queue five hops upstream, which is where the alert is. S23's
carrier throttle passes a third of the shipments; the alert is the low
rate at the end of the pipeline, while the backlog builds slowly in
front of the throttle and spreads back to ingest.
"""

from flows.scenarios.base import Kpi, Pack, Scenario, action
from flows.scenarios.common import dlq, generator, sink, stage

ORDER = '{"order_id": "o-1042", "region": "emea", "card": "4111111111111111", "total": 59.90}'
SHIPMENT = '{"shipment_id": "sh-77", "carrier": "dhl", "weight_kg": 2.4, "zip": "10115"}'


def build_s22(pg):
    # 100/s of orders. The batch waits for 20,000 orders, but its input
    # connection stops at 10,000 (the bin's FlowFiles count toward it), so
    # it only empties at its 30 minute age. Each queue upstream fills in
    # turn, 100 s apiece: ingest is full about 500 s after a flush.
    generator(pg, "S22 Order Feed", period="1 sec", batch=100, text=ORDER)
    pg.processor("extract", "S22 Extract Order", "EvaluateJsonPath", {
        "Destination": "flowfile-attribute",
        "Return Type": "auto-detect",
        "order.region": "$.region",
    }, period="0 sec", tasks=1)
    stage(pg, "tag", "S22 Tag Region", period="0 sec")
    stage(pg, "normalize", "S22 Normalize Currency", period="0 sec")
    pg.processor("mask", "S22 Mask Card", "ReplaceText", {
        "Regular Expression": "[0-9]{12}([0-9]{4})",
        "Replacement Value": "************$1",
        "Replacement Strategy": "Regex Replace",
        "Evaluation Mode": "Line-by-Line",
    }, period="0 sec", tasks=1)
    pg.processor("batch", "S22 Batch Orders", "MergeContent", {
        "Merge Strategy": "Bin-Packing Algorithm",
        "Merge Format": "Binary Concatenation",
        "Minimum Number of Entries": "20000",
        "Maximum Number of Entries": "25000",
        "Max Bin Age": "30 min",
        "Maximum number of Bins": "5",
    }, period="0 sec", tasks=1, auto_terminate=("original", "failure"))
    pg.processor("compress", "S22 Compress Batch", "CompressContent", {
        "Mode": "compress", "Compression Format": "gzip", "Compression Level": "1",
    }, period="0 sec", tasks=1, auto_terminate=("failure",))
    stage(pg, "name", "S22 Name Batch File", period="0 sec")
    sink(pg, "publish", "S22 Publish Orders", period="0 sec")
    dlq(pg, "S22 Unparsed DLQ")
    pg.connect("gen", "extract", ["success"], "s22-ingest")
    pg.connect("extract", "tag", ["matched"], "s22-extracted")
    pg.connect("extract", "dlq", ["unmatched", "failure"], "s22-unparsed")
    pg.connect("tag", "normalize", ["success"], "s22-tagged")
    pg.connect("normalize", "mask", ["success"], "s22-normalized")
    pg.connect("mask", "batch", ["success"], "s22-to-batch")
    pg.connect("mask", "dlq", ["failure"], "s22-mask-failed")
    pg.connect("batch", "compress", ["merged"], "s22-batches")
    pg.connect("compress", "name", ["success"], "s22-compressed")
    pg.connect("name", "publish", ["success"], "s22-named")


S22 = Scenario(
    id="S22",
    title="Order Merge Deadlock",
    symptom="backpressure",
    fault=("The batch waits for 20,000 orders but its input connection holds only 10,000, so it "
           "empties only at its 30 minute bin age and the back pressure reaches ingest."),
    build=build_s22,
    kpis=(
        Kpi("S22", "connection", "s22-ingest", "queued_count", above=8000),
        Kpi("S22", "connection", "s22-normalized", "queued_count", alert=False, primary=False),
        Kpi("S22", "connection", "s22-to-batch", "queued_count", alert=False, primary=False),
        Kpi("S22", "processor", "S22 Batch Orders", "flowfiles_out", alert=False, good="up",
            primary=False),
    ),
    family="FLOW_EDIT",
    kind="cascade",
    expected=(action("SET_PROCESSOR_PROPERTY", "S22 Batch Orders", property="Minimum Number of Entries",
                     value="5000"),),
    acceptable=(
        action("SET_BACKPRESSURE", "s22-to-batch", object_threshold=30000),
        action("SET_PROCESSOR_PROPERTY", "S22 Batch Orders", property="Max Bin Age", value="1 min"),
    ),
    traps=(
        "The alert is on ingest, but S22 Extract Order is back-pressured, not slow: more tasks do nothing.",
        "A higher ingest threshold only lets the backlog grow; the merge still never fills its bin.",
    ),
    wrong=(
        action("SET_CONCURRENT_TASKS", "S22 Extract Order", value=4),
        action("SET_BACKPRESSURE", "s22-ingest", object_threshold=50000),
    ),
)


def build_s23(pg):
    # 30/s of shipments into a carrier throttle of 600 a minute (10/s):
    # confirmations run at a third of the intake. The surplus, 20/s, fills
    # the throttle's 2,000 input in 100 s, then each queue upstream in
    # 500 s, reaching ingest about 25 minutes in.
    generator(pg, "S23 Shipments", period="1 sec", batch=30, text=SHIPMENT)
    pg.processor("extract", "S23 Extract Carrier", "EvaluateJsonPath", {
        "Destination": "flowfile-attribute",
        "Return Type": "auto-detect",
        "shipment.carrier": "$.carrier",
    }, period="0 sec", tasks=1)
    stage(pg, "address", "S23 Normalize Address", period="0 sec")
    stage(pg, "price", "S23 Price Label", period="0 sec")
    pg.processor("throttle", "S23 Carrier Throttle", "ControlRate", {
        "Rate Control Criteria": "flowfile count",
        "Maximum Rate": "600",
        "Time Duration": "1 min",
    }, period="0 sec", tasks=1, auto_terminate=("failure",))
    stage(pg, "book", "S23 Book Pickup", period="0 sec")
    pg.processor("hash", "S23 Hash Label", "CryptographicHashContent", {"Hash Algorithm": "SHA-256"},
                 period="0 sec", tasks=1, auto_terminate=("failure",))
    stage(pg, "notify", "S23 Notify Customer", period="0 sec")
    sink(pg, "confirm", "S23 Ship Confirmations", period="0 sec")
    dlq(pg, "S23 Bad Record DLQ")
    pg.connect("gen", "extract", ["success"], "s23-ingest")
    pg.connect("extract", "address", ["matched"], "s23-extracted")
    pg.connect("extract", "dlq", ["unmatched", "failure"], "s23-bad-records")
    pg.connect("address", "price", ["success"], "s23-addressed")
    pg.connect("price", "throttle", ["success"], "s23-to-throttle", object_threshold=2000)
    pg.connect("throttle", "book", ["success"], "s23-throttled")
    pg.connect("book", "hash", ["success"], "s23-booked")
    pg.connect("hash", "notify", ["success"], "s23-labelled")
    pg.connect("notify", "confirm", ["success"], "s23-notified")


S23 = Scenario(
    id="S23",
    title="Throttled Fulfilment",
    symptom="throughput",
    fault="The carrier throttle allows 600 shipments a minute (10/s) against 30/s coming in.",
    build=build_s23,
    kpis=(
        Kpi("S23", "processor", "S23 Ship Confirmations", "flowfiles_in", below=20, good="up"),
        Kpi("S23", "connection", "s23-to-throttle", "queued_count", alert=False, primary=False),
        Kpi("S23", "connection", "s23-ingest", "queued_count", above=8000, primary=False),
    ),
    family="FLOW_EDIT",
    kind="cascade",
    detect_within=900,
    clear_within=1800,      # the backlog drains at 20/s
    expected=(action("SET_PROCESSOR_PROPERTY", "S23 Carrier Throttle", property="Maximum Rate",
                     value="3000"),),
    acceptable=(
        action("SET_PROCESSOR_PROPERTY", "S23 Carrier Throttle", property="Time Duration", value="10 sec"),
    ),
    traps=(
        "ControlRate's limit is per node, not per task: more tasks on the throttle pass no more.",
        "Speeding up the stages after the throttle does nothing; they are starved, not slow.",
    ),
    wrong=(
        action("SET_CONCURRENT_TASKS", "S23 Carrier Throttle", value=4),
        action("SET_CONCURRENT_TASKS", "S23 Book Pickup", value=4),
    ),
)


PACK = Pack(
    name="p8-orders",
    title="Order pipeline cascades",
    scenarios=(S22, S23),
    wave=2,
)
