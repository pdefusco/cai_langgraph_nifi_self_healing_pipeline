"""
Pack p11-payments: payment authorization and statement archiving, with
the fault one stage behind where it shows.

S28 balances payments over three authorization lanes, and the third
lane's tokenizer is disabled: the lane backs up into the balancer's
queue for it, while the other two lanes look healthy and take the rest.
S29 compresses statements at level 9, which is CPU-bound well below the
incoming bytes: the backlog reaches ingest, and the CPU shows why.
"""

from flows.scenarios.base import Kpi, Pack, Scenario, action
from flows.scenarios.common import dlq, generator, sink, stage

PAYMENT = '{"payment": "pay-5521", "merchant": "m-88", "amount": 42.10, "currency": "EUR"}'
LANES = (1, 2, 3)


def build_s28(pg):
    # 12/s balanced round robin over three lanes of 10/s each (one task
    # every 100 ms). Lane 3's tokenizer is disabled, so its authorizer's
    # output fills at 4/s, then the authorizer is blocked and the
    # balancer's queue for lane 3 fills too, each of 2,000 in 500 s: the
    # alert is about 17 minutes in. The balancer then skips lane 3, and
    # lanes 1 and 2 carry 6/s each, within capacity.
    generator(pg, "S28 Payment Requests", period="1 sec", batch=12, text=PAYMENT)
    pg.processor("parse", "S28 Parse Payment", "EvaluateJsonPath", {
        "Destination": "flowfile-attribute",
        "Return Type": "auto-detect",
        "payment.id": "$.payment",
    }, period="0 sec", tasks=1)
    stage(pg, "fraud", "S28 Fraud Score", period="0 sec")
    pg.processor("balance", "S28 Balance Lanes", "DistributeLoad", {
        "Number of Relationships": str(len(LANES)),
        "Distribution Strategy": "round robin",
    }, period="0 sec", tasks=1)
    stage(pg, "capture", "S28 Capture Payment", period="0 sec")
    sink(pg, "ledger", "S28 Ledger Write", period="0 sec")
    dlq(pg, "S28 Parse DLQ")
    pg.connect("gen", "parse", ["success"], "s28-ingest")
    pg.connect("parse", "fraud", ["matched"], "s28-parsed")
    pg.connect("parse", "dlq", ["unmatched", "failure"], "s28-unparsed")
    pg.connect("fraud", "balance", ["success"], "s28-scored")

    for n in LANES:
        stage(pg, f"auth{n}", f"S28 Authorize Lane {n}", period="100 ms")
        stage(pg, f"token{n}", f"S28 Tokenize Lane {n}", period="100 ms",
              scheduled_state="DISABLED" if n == 3 else "ENABLED")
        pg.connect("balance", f"auth{n}", [str(n)], f"s28-to-lane-{n}", object_threshold=2000)
        pg.connect(f"auth{n}", f"token{n}", ["success"], f"s28-lane-{n}-authorized",
                   object_threshold=2000)
        pg.connect(f"token{n}", "capture", ["success"], f"s28-lane-{n}-tokenized")

    pg.connect("capture", "ledger", ["success"], "s28-captured")


S28 = Scenario(
    id="S28",
    title="Lane Stopped Behind Load Balancer",
    symptom="backpressure",
    fault=("Lane 3's tokenizer is disabled: the lane backs up into the balancer's queue for it, "
           "and the payments balanced to it before it filled are stranded there."),
    build=build_s28,
    kpis=(
        Kpi("S28", "connection", "s28-to-lane-3", "queued_count", above=1500),
        Kpi("S28", "connection", "s28-lane-3-authorized", "queued_count", alert=False, primary=False),
        Kpi("S28", "connection", "s28-to-lane-1", "queued_count", alert=False, primary=False),
        Kpi("S28", "processor", "S28 Ledger Write", "flowfiles_in", alert=False, good="up",
            primary=False),
    ),
    family="FLOW_EDIT",
    kind="cascade",
    expected=(action("SET_SCHEDULED_STATE", "S28 Tokenize Lane 3", value="ENABLED"),),
    acceptable=(),
    traps=(
        "The alert is on the balancer's queue for lane 3, but S28 Authorize Lane 3 is "
        "back-pressured, not slow; the stopped processor is one stage on.",
        "The ledger rate is normal: lanes 1 and 2 carry the load, so nothing looks lost.",
    ),
    wrong=(
        action("SET_CONCURRENT_TASKS", "S28 Authorize Lane 3", value=4),
        action("SET_BACKPRESSURE", "s28-to-lane-3", object_threshold=10000),
    ),
)


def build_s29(pg):
    # 100/s of 200 KB statements, 20 MB/s, into a compressor at level 9:
    # 8 MB/s on its one core, 40/s. Each queue before it is full at its
    # 1 GB size threshold, 5,000 statements, in under 90 s at the 60/s
    # surplus: ingest is full within 6 minutes. Level 1 does 60 MB/s,
    # 300/s.
    generator(pg, "S29 Statement Batches", period="1 sec", batch=100, size="200 KB")
    stage(pg, "validate", "S29 Validate Statement", period="0 sec")
    stage(pg, "tag", "S29 Tag Account", period="0 sec")
    stage(pg, "render", "S29 Render Statement", period="0 sec")
    pg.processor("compress", "S29 Compress Statement", "CompressContent", {
        "Mode": "compress", "Compression Format": "gzip", "Compression Level": "9",
    }, period="0 sec", tasks=1, auto_terminate=("failure",))
    pg.processor("hash", "S29 Hash Statement", "CryptographicHashContent", {
        "Hash Algorithm": "SHA-256",
    }, period="0 sec", tasks=1, auto_terminate=("failure",))
    stage(pg, "index", "S29 Index Statement", period="0 sec")
    stage(pg, "region", "S29 Route By Region", period="0 sec")
    stage(pg, "delivery", "S29 Record Delivery", period="0 sec")
    sink(pg, "archive", "S29 Archive Statements", period="0 sec")
    pg.connect("gen", "validate", ["success"], "s29-ingest")
    pg.connect("validate", "tag", ["success"], "s29-validated")
    pg.connect("tag", "render", ["success"], "s29-tagged")
    pg.connect("render", "compress", ["success"], "s29-to-compress")
    pg.connect("compress", "hash", ["success"], "s29-compressed")
    pg.connect("hash", "index", ["success"], "s29-hashed")
    pg.connect("index", "region", ["success"], "s29-indexed")
    pg.connect("region", "delivery", ["success"], "s29-routed")
    pg.connect("delivery", "archive", ["success"], "s29-delivered")


S29 = Scenario(
    id="S29",
    title="Compression Level Bottleneck",
    symptom="backlog",
    fault=("Statements are compressed at level 9, 8 MB/s per core, and 20 MB/s arrive: the "
           "backlog reaches ingest three stages back."),
    build=build_s29,
    kpis=(
        Kpi("S29", "connection", "s29-ingest", "queued_count", above=4000),
        Kpi("S29", "connection", "s29-to-compress", "queued_count", alert=False, primary=False),
        Kpi("S29", "processor", "S29 Compress Statement", "flowfiles_out", alert=False, good="up",
            primary=False),
        Kpi("S29", "system", "", "cpu", alert=False, primary=False),
    ),
    family="FLOW_EDIT",
    kind="cascade",
    expected=(action("SET_PROCESSOR_PROPERTY", "S29 Compress Statement",
                     property="Compression Level", value="1"),),
    acceptable=(action("SET_PROCESSOR_PROPERTY", "S29 Compress Statement",
                       property="Compression Level", value="3"),),
    traps=(
        "A second task doubles the compressor to 16 MB/s, still short of 20 MB/s, and takes the "
        "node's other core.",
        "The stages before the compressor are back-pressured, not slow.",
    ),
    wrong=(
        action("SET_CONCURRENT_TASKS", "S29 Compress Statement", value=2),
        action("SET_CONCURRENT_TASKS", "S29 Validate Statement", value=4),
    ),
)


PACK = Pack(
    name="p11-payments",
    title="Payment pipeline cascades",
    scenarios=(S28, S29),
    wave=2,
)
