"""
Pack p13-billing: compound faults across two families.

S32 has a billing throttle whose rate comes from the parameter context
`nfh-p13-billing`, set a tenth of its production value, and behind it an
invoice poster whose run schedule is too slow: a parameter fault and a
flow fault in one group, which no single proposal can fix (a proposal
stays within one family). S33 is masked: a disabled meter gate stalls
the usage records, and once it is enabled, a load-test copy count left
in the parameter context floods the rater behind it.

The correct values are in playbooks/parameter-misconfiguration.md.
"""

from flows.scenarios.base import Kpi, Pack, Scenario, action
from flows.scenarios.common import dlq, generator, sink, stage

CONTEXT = "nfh-p13-billing"

INVOICE = '{"invoice": "inv-20931", "account": "acc-118", "amount": 310.00, "terms": "net30"}'
USAGE = '{"account": "acc-118", "meter": "api-calls", "units": 1200, "period": "2026-10"}'


def build_s32(pg):
    # 10/s of invoices into a throttle of #{s32.max.rate} a second (5,
    # where production is 50), then a poster running one task every
    # 500 ms (2/s). The throttle's queue fills at 5/s, 2,000 in under 7
    # minutes; the poster's at 3/s, in 11. Raising the rate alone sends
    # 10/s at the poster; speeding the poster alone leaves the throttle.
    generator(pg, "S32 Invoice Feed", period="1 sec", batch=10, text=INVOICE)
    pg.processor("parse", "S32 Parse Invoice", "EvaluateJsonPath", {
        "Destination": "flowfile-attribute",
        "Return Type": "auto-detect",
        "invoice.account": "$.account",
    }, period="0 sec", tasks=1)
    stage(pg, "tax", "S32 Apply Tax", period="0 sec")
    stage(pg, "currency", "S32 Convert Currency", period="0 sec")
    pg.processor("throttle", "S32 Billing Throttle", "ControlRate", {
        "Rate Control Criteria": "flowfile count",
        "Maximum Rate": "#{s32.max.rate}",
        "Time Duration": "1 sec",
    }, period="0 sec", tasks=1, auto_terminate=("failure",))
    stage(pg, "number", "S32 Number Invoice", period="0 sec")
    stage(pg, "post", "S32 Post Invoice", period="500 ms")
    stage(pg, "render", "S32 Render Statement", period="0 sec")
    stage(pg, "notify", "S32 Notify Account", period="0 sec")
    sink(pg, "ledger", "S32 Ledger Entries", period="0 sec")
    dlq(pg, "S32 Unparsed DLQ")
    pg.connect("gen", "parse", ["success"], "s32-ingest")
    pg.connect("parse", "tax", ["matched"], "s32-parsed")
    pg.connect("parse", "dlq", ["unmatched", "failure"], "s32-unparsed")
    pg.connect("tax", "currency", ["success"], "s32-taxed")
    pg.connect("currency", "throttle", ["success"], "s32-to-throttle", object_threshold=2000)
    pg.connect("throttle", "number", ["success"], "s32-throttled")
    pg.connect("number", "post", ["success"], "s32-to-post", object_threshold=2000)
    pg.connect("post", "render", ["success"], "s32-posted")
    pg.connect("render", "notify", ["success"], "s32-rendered")
    pg.connect("notify", "ledger", ["success"], "s32-notified")


RATE = action("UPDATE_PARAMETER", CONTEXT, parameter="s32.max.rate", value="50")
POST = action("SET_RUN_SCHEDULE", "S32 Post Invoice", value="0 sec")

S32 = Scenario(
    id="S32",
    title="Throttle Parameter And Slow Poster",
    symptom="backlog",
    fault=("s32.max.rate is 5, a tenth of production, and S32 Post Invoice runs every 500 ms (2/s) "
           "against 10/s of invoices: a parameter fault and a flow fault."),
    build=build_s32,
    kpis=(
        Kpi("S32", "connection", "s32-to-throttle", "queued_count", above=1500),
        Kpi("S32", "connection", "s32-to-post", "queued_count", above=1500),
        Kpi("S32", "processor", "S32 Ledger Entries", "flowfiles_in", alert=False, good="up",
            primary=False),
    ),
    family="PARAMETER",
    kind="compound",
    faults=(RATE, POST),
    expected=(RATE,),
    acceptable=(
        POST,
        action("SET_CONCURRENT_TASKS", "S32 Post Invoice", value=8),
    ),
    traps=(
        "One proposal cannot fix both: the throttle's rate is a parameter, the poster's schedule a "
        "flow edit.",
        "Editing the throttle's Maximum Rate in the flow replaces the parameter reference and forks "
        "the configuration.",
        "ControlRate's limit is per node, not per task: more tasks on the throttle pass no more.",
    ),
    wrong=(
        action("SET_CONCURRENT_TASKS", "S32 Billing Throttle", value=4),
        action("SET_BACKPRESSURE", "s32-to-post", object_threshold=10000),
    ),
    parameters={"s32.max.rate": "5"},
)


def build_s33(pg):
    # Two usage records a second. The meter gate is disabled, so its queue
    # fills to 2,000 in under 17 minutes and the feed stops. Enabled, each
    # record is copied #{s33.copies} times (50, the load-test value;
    # production is 1): 100/s at a rater doing 20/s (one task every
    # 50 ms), whose queue fills in well under a minute.
    generator(pg, "S33 Usage Feed", period="1 sec", batch=2, text=USAGE)
    stage(pg, "gate", "S33 Meter Gate", period="0 sec", scheduled_state="DISABLED")
    pg.processor("parse", "S33 Parse Usage", "EvaluateJsonPath", {
        "Destination": "flowfile-attribute",
        "Return Type": "auto-detect",
        "usage.meter": "$.meter",
    }, period="0 sec", tasks=1)
    pg.processor("fanout", "S33 Fan Out Usage", "DuplicateFlowFile", {"Number of Copies": "#{s33.copies}"},
                 period="0 sec", tasks=1)
    stage(pg, "rate", "S33 Rate Usage", period="50 ms")
    stage(pg, "tier", "S33 Apply Tier", period="0 sec")
    stage(pg, "credit", "S33 Apply Credits", period="0 sec")
    stage(pg, "aggregate", "S33 Aggregate Account", period="0 sec")
    sink(pg, "invoice", "S33 Usage Invoiced", period="0 sec")
    dlq(pg, "S33 Unparsed DLQ")
    pg.connect("gen", "gate", ["success"], "s33-to-gate", object_threshold=2000)
    pg.connect("gate", "parse", ["success"], "s33-gated")
    pg.connect("parse", "fanout", ["matched"], "s33-parsed")
    pg.connect("parse", "dlq", ["unmatched", "failure"], "s33-unparsed")
    pg.connect("fanout", "rate", ["success"], "s33-copies", object_threshold=2000)
    pg.connect("rate", "tier", ["success"], "s33-rated")
    pg.connect("tier", "credit", ["success"], "s33-tiered")
    pg.connect("credit", "aggregate", ["success"], "s33-credited")
    pg.connect("aggregate", "invoice", ["success"], "s33-aggregated")


GATE = action("SET_SCHEDULED_STATE", "S33 Meter Gate", value="ENABLED")
COPIES = action("UPDATE_PARAMETER", CONTEXT, parameter="s33.copies", value="1")

S33 = Scenario(
    id="S33",
    title="Masked Copy Flood",
    symptom="backpressure",
    fault=("The meter gate is disabled, which stalls the usage records and hides that s33.copies "
           "is still 50, the load-test value, which floods the rater once they flow."),
    build=build_s33,
    kpis=(
        Kpi("S33", "connection", "s33-to-gate", "queued_count", above=1500),
        # Opens the second incident, once the gate is enabled; not the first's verdict.
        Kpi("S33", "connection", "s33-copies", "queued_count", above=1500, primary=False),
    ),
    family="FLOW_EDIT",
    kind="compound",
    faults=(GATE, COPIES),
    expected=(GATE,),
    acceptable=(COPIES,),
    traps=(
        "Nothing reaches the fan-out while the gate is disabled: the copy count shows only once the "
        "records flow again.",
        "Speeding up S33 Rate Usage processes 50x the intended records downstream.",
    ),
    wrong=(
        action("SET_CONCURRENT_TASKS", "S33 Meter Gate", value=4),
        action("SET_CONCURRENT_TASKS", "S33 Rate Usage", value=8),
    ),
    parameters={"s33.copies": "50"},
)


PACK = Pack(
    name="p13-billing",
    title="Billing compound faults",
    scenarios=(S32, S33),
    parameter_context=CONTEXT,
    wave=2,
)
