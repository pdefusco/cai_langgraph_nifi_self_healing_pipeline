"""
Pack p12-warehouse: compound faults, where one fix is not enough.

S30 has two slow stages in series: picking and labelling each run below
the intake, so fixing either one leaves the other's queue full. S31 is
masked: a disabled validator stalls the manifests, and only once it is
enabled does the customs check behind it show that it drops 15% of
them. Both are flow edits, but the second fault of S31 cannot be seen
until the first is fixed.
"""

from flows.scenarios.base import Kpi, Pack, Scenario, action
from flows.scenarios.common import debug_flow, dlq, generator, sink, stage

ORDER = '{"order_id": "wo-311", "lines": 3, "zone": "B4", "priority": "standard"}'
MANIFEST = '{"manifest": "mf-904", "dock": 7, "country": "CH", "hs_code": "8471.30"}'


def build_s30(pg):
    # 20/s of orders. Picking runs one task every 125 ms (8/s), labelling
    # one every 200 ms (5/s). The pick queue fills at 12/s, 2,000 in under
    # 3 minutes; the label queue at 3/s, 2,000 in 11. Speeding up picking
    # alone sends 20/s at the labeller, and the reverse leaves picking as
    # slow as it was.
    generator(pg, "S30 Order Feed", period="1 sec", batch=20, text=ORDER)
    pg.processor("parse", "S30 Parse Order", "EvaluateJsonPath", {
        "Destination": "flowfile-attribute",
        "Return Type": "auto-detect",
        "order.zone": "$.zone",
    }, period="0 sec", tasks=1)
    stage(pg, "slot", "S30 Assign Slot", period="0 sec")
    stage(pg, "pick", "S30 Pick Items", period="125 ms")
    stage(pg, "pack", "S30 Pack Order", period="0 sec")
    stage(pg, "weigh", "S30 Weigh Parcel", period="0 sec")
    stage(pg, "label", "S30 Label Parcel", period="200 ms")
    stage(pg, "route", "S30 Route Carrier", period="0 sec")
    stage(pg, "notify", "S30 Notify Dispatch", period="0 sec")
    sink(pg, "ship", "S30 Ship Parcels", period="0 sec")
    dlq(pg, "S30 Unparsed DLQ")
    pg.connect("gen", "parse", ["success"], "s30-ingest")
    pg.connect("parse", "slot", ["matched"], "s30-parsed")
    pg.connect("parse", "dlq", ["unmatched", "failure"], "s30-unparsed")
    pg.connect("slot", "pick", ["success"], "s30-to-pick", object_threshold=2000)
    pg.connect("pick", "pack", ["success"], "s30-picked", object_threshold=2000)
    pg.connect("pack", "weigh", ["success"], "s30-packed", object_threshold=2000)
    pg.connect("weigh", "label", ["success"], "s30-to-label", object_threshold=2000)
    pg.connect("label", "route", ["success"], "s30-labelled")
    pg.connect("route", "notify", ["success"], "s30-routed")
    pg.connect("notify", "ship", ["success"], "s30-notified")


PICK = action("SET_RUN_SCHEDULE", "S30 Pick Items", value="0 sec")
LABEL = action("SET_RUN_SCHEDULE", "S30 Label Parcel", value="0 sec")

S30 = Scenario(
    id="S30",
    title="Two Slow Stages In Series",
    symptom="backlog",
    fault=("Picking (8/s) and labelling (5/s) each run below the 20/s intake, so the queues in "
           "front of both are full, and fixing one leaves the other."),
    build=build_s30,
    kpis=(
        Kpi("S30", "connection", "s30-to-pick", "queued_count", above=1500),
        Kpi("S30", "connection", "s30-to-label", "queued_count", above=1500),
        Kpi("S30", "processor", "S30 Ship Parcels", "flowfiles_in", alert=False, good="up",
            primary=False),
    ),
    family="FLOW_EDIT",
    kind="compound",
    faults=(PICK, LABEL),
    expected=(PICK,),
    # 3 tasks pick 24/s and 5 label 25/s, as does a 40 ms schedule on either: the least that cleared both in
    # every oracle round (2 picking or 4 labelling didn't).
    acceptable=(
        LABEL,
        action("SET_RUN_SCHEDULE", "S30 Pick Items", value="40 ms"),
        action("SET_RUN_SCHEDULE", "S30 Label Parcel", value="40 ms"),
        action("SET_CONCURRENT_TASKS", "S30 Pick Items", value=3),
        action("SET_CONCURRENT_TASKS", "S30 Label Parcel", value=5),
    ),
    traps=(
        "Two queues are full: a fix for one slow stage only moves the backlog to the other.",
        "The stages between them are back-pressured, not slow; more tasks there pass no more.",
        "A larger pick queue only holds a larger backlog.",
    ),
    wrong=(
        action("SET_BACKPRESSURE", "s30-to-pick", object_threshold=10000),
        action("SET_CONCURRENT_TASKS", "S30 Parse Order", value=4),
    ),
)


def build_s31(pg):
    # 10/s of manifests. The validator is disabled, so its queue fills in
    # 200 s and the feed stops; nothing reaches the customs check. Enabled,
    # it passes everything to a check that rejects 3 in 20 (15%) and
    # auto-terminates them: the store gets 8.5/s, and the customs DLQ is
    # not connected.
    generator(pg, "S31 Manifest Feed", period="1 sec", batch=10, text=MANIFEST)
    stage(pg, "validate", "S31 Validate Manifest", period="0 sec", scheduled_state="DISABLED")
    pg.processor("parse", "S31 Parse Manifest", "EvaluateJsonPath", {
        "Destination": "flowfile-attribute",
        "Return Type": "auto-detect",
        "manifest.country": "$.country",
    }, period="0 sec", tasks=1)
    stage(pg, "dock", "S31 Assign Dock", period="0 sec")
    debug_flow(pg, "customs", "S31 Check Customs", successes=17, failures=3,
               period="0 sec", tasks=1, auto_terminate=("failure",))
    stage(pg, "carrier", "S31 Tag Carrier", period="0 sec")
    stage(pg, "rate", "S31 Rate Freight", period="0 sec")
    stage(pg, "book", "S31 Book Slot", period="0 sec")
    sink(pg, "store", "S31 Store Manifest", period="0 sec")
    dlq(pg, "S31 Parse DLQ")
    dlq(pg, "S31 Customs DLQ", connected=False, key="customs_dlq")
    pg.connect("gen", "validate", ["success"], "s31-to-validate", object_threshold=2000)
    pg.connect("validate", "parse", ["success"], "s31-validated")
    pg.connect("parse", "dock", ["matched"], "s31-parsed")
    pg.connect("parse", "dlq", ["unmatched", "failure"], "s31-unparsed")
    pg.connect("dock", "customs", ["success"], "s31-to-customs")
    pg.connect("customs", "carrier", ["success"], "s31-cleared")
    pg.connect("carrier", "rate", ["success"], "s31-tagged")
    pg.connect("rate", "book", ["success"], "s31-rated")
    pg.connect("book", "store", ["success"], "s31-booked")


ENABLE = action("SET_SCHEDULED_STATE", "S31 Validate Manifest", value="ENABLED")
ROUTE = action("ROUTE_RELATIONSHIP_TO_DLQ", "S31 Check Customs", relationship="failure",
               dlq="S31 Customs DLQ")

S31 = Scenario(
    id="S31",
    title="Masked Failure Behind A Stall",
    symptom="backpressure",
    fault=("The validator is disabled, which stalls the manifests and hides that the customs check "
           "behind it auto-terminates 15% of them."),
    build=build_s31,
    kpis=(
        Kpi("S31", "connection", "s31-to-validate", "queued_count", above=1500),
        Kpi("S31", "processor", "S31 Assign Dock", "flowfiles_out", alert=False, good="up",
            primary=False),
        Kpi("S31", "processor", "S31 Store Manifest", "flowfiles_in", alert=False, good="up",
            primary=False),
        # Decides, with the queue: once rejects reach it, they are accounted for, not lost.
        Kpi("S31", "processor", "S31 Customs DLQ", "flowfiles_in", alert=False, good="up"),
    ),
    family="FLOW_EDIT",
    kind="compound",
    faults=(ENABLE, ROUTE),
    expected=(ENABLE,),
    acceptable=(ROUTE,),
    traps=(
        "Nothing reaches the customs check while the validator is disabled: its loss shows only "
        "once the manifests flow again.",
        "More tasks or a larger queue in front of a disabled processor move nothing.",
        "More tasks on the customs check reject just as many; it is not slow.",
    ),
    wrong=(
        action("SET_CONCURRENT_TASKS", "S31 Validate Manifest", value=4),
        action("SET_BACKPRESSURE", "s31-to-validate", object_threshold=10000),
    ),
)


PACK = Pack(
    name="p12-warehouse",
    title="Warehouse compound faults",
    scenarios=(S30, S31),
    wave=2,
)
