"""
Packs p26-holdout-a and p27-holdout-b: faults nothing was tuned on (NFH-123).

Every fault here is a kind the catalog fixes and the playbooks cover,
in flows, domains, names and numbers no prompt, playbook or replay was
tuned against: carrier billing, parcels and pharmacy in one pack;
identity checks, onboarding and ad auctions in the other. Each pack has
a control as well. They are holdouts: the default replay leaves them
out and no playbook edit is drafted from their reviews, so what the
agent does on them is what it does on a fault it has not seen.

All but S108 are fixed with a low-risk flow edit, so autonomy may come
to make them; S108's DLQ route is medium risk and always goes to a
person.

Cron expressions are Quartz, in UTC.
"""

from flows.scenarios.base import Kpi, Pack, Scenario, action
from flows.scenarios.common import debug_flow, dlq, generator, sink, stage

CDR = '{"cdr": "c-80213", "msisdn": "+447700900123", "cell": "lac-221", "secs": 184, "type": "voice"}'
SCAN = '{"parcel": "p-3301-77", "depot": "d-14", "event": "out-for-delivery", "lat": 51.5, "lon": -0.12}'
SCRIPT = '{"rx": "rx-50981", "patient": "pt-2214", "ndc": "0002-7510", "qty": 30, "prescriber": "dr-118"}'
MANIFEST = '{"manifest": "m-7710", "vessel": "v-ocean-4", "containers": 12, "port": "rtm"}'
CHECK = '{"applicant": "ap-60412", "doc": "passport", "country": "NL", "selfie": true}'
# One in twenty has no value: {"applicant": , ...} is not JSON.
APPLICATION = """{"applicant": ${random():mod(20):equals(0):ifElse('','60412')}, "doc": "passport"}"""
BID = '{"auction": "a-99120", "slot": "top-banner", "floor": 0.42, "segment": "sports"}'
ALERT = '{"alert": "al-4410", "sensor": "door-12", "site": "wh-3", "state": "open"}'


# ---------------------------------------------------------------------
# p26-holdout-a
# ---------------------------------------------------------------------


def build_s103(pg):
    # 5/s from the switches and 1/s of roaming records, 6/s in all, into
    # a rater with one task every 250 ms (4/s): its queue grows by 2/s,
    # 120 a minute, and crosses 3,000 about 25 minutes after deploy.
    # Every other stage runs on 0 sec and keeps up.
    generator(pg, "S103 Switch CDR Feed", period="1 sec", batch=5, text=CDR)
    generator(pg, "S103 Roaming CDR Feed", period="1 sec", batch=1, text=CDR, key="roaming")
    stage(pg, "partner", "S103 Map Roaming Partner", period="0 sec")
    stage(pg, "decode", "S103 Decode CDR", period="0 sec")
    stage(pg, "normalise", "S103 Normalise Number", period="0 sec")
    stage(pg, "lookup", "S103 Lookup Subscriber", period="0 sec")
    stage(pg, "rate", "S103 Rate Call", period="250 ms", tasks=1)
    stage(pg, "discount", "S103 Apply Discount", period="0 sec")
    stage(pg, "tax", "S103 Tax Call", period="0 sec")
    stage(pg, "aggregate", "S103 Aggregate Usage", period="0 sec")
    sink(pg, "bill", "S103 Write Billing Record", period="0 sec")
    pg.connect("gen", "decode", ["success"], "s103-switch")
    pg.connect("roaming", "partner", ["success"], "s103-roaming")
    pg.connect("partner", "decode", ["success"], "s103-roaming-mapped")
    pg.connect("decode", "normalise", ["success"], "s103-decoded")
    pg.connect("normalise", "lookup", ["success"], "s103-normalised")
    pg.connect("lookup", "rate", ["success"], "s103-to-rate")
    pg.connect("rate", "discount", ["success"], "s103-rated")
    pg.connect("discount", "tax", ["success"], "s103-discounted")
    pg.connect("tax", "aggregate", ["success"], "s103-taxed")
    pg.connect("aggregate", "bill", ["success"], "s103-aggregated")


S103 = Scenario(
    id="S103",
    title="Call Rating Behind",
    symptom="backlog",
    fault=("S103 Rate Call does 4/s, one task every 250 ms, against 6/s of switch and roaming records: "
           "its queue grows by 120 a minute."),
    build=build_s103,
    kpis=(
        Kpi("S103", "connection", "s103-to-rate", "queued_count", above=3000),
        Kpi("S103", "connection", "s103-normalised", "queued_count", alert=False, primary=False),
        Kpi("S103", "processor", "S103 Write Billing Record", "flowfiles_in", alert=False, good="up",
            primary=False),
    ),
    family="FLOW_EDIT",
    kind="cascade",
    clear_within=1800,
    expected=(action("SET_CONCURRENT_TASKS", "S103 Rate Call", value=3),),
    acceptable=(action("SET_RUN_SCHEDULE", "S103 Rate Call", value="0 sec"),),
    traps=(
        "Two tasks match the input at 8/s but take over half an hour to drain a full queue.",
        "S103 Lookup Subscriber feeds the queue; it is keeping up, not the cause.",
        "A higher threshold on s103-to-rate holds a bigger backlog; the rater is no faster.",
    ),
    wrong=(
        action("SET_CONCURRENT_TASKS", "S103 Lookup Subscriber", value=4),
        action("SET_BACKPRESSURE", "s103-to-rate", object_threshold=50000),
    ),
)


def build_s104(pg):
    # 3 scans/s into a geofence check with two tasks every 5 s (0.4/s):
    # its queue grows by 2.6/s and crosses 2,000 about 13 minutes in.
    # Even 8 tasks (the catalog maximum) on that schedule do 1.6/s.
    generator(pg, "S104 Parcel Scans", period="1 sec", batch=3, text=SCAN)
    stage(pg, "parse", "S104 Parse Scan", period="0 sec")
    stage(pg, "barcode", "S104 Validate Barcode", period="0 sec")
    stage(pg, "depot", "S104 Resolve Depot", period="0 sec")
    stage(pg, "geofence", "S104 Geofence Check", period="5 sec", tasks=2)
    stage(pg, "eta", "S104 Update ETA", period="0 sec")
    stage(pg, "notify", "S104 Notify Customer", period="0 sec")
    stage(pg, "archive", "S104 Archive Scan", period="0 sec")
    sink(pg, "publish", "S104 Publish Tracking", period="0 sec")
    generator(pg, "S104 Depot Heartbeat", period="10 sec", batch=1, text=SCAN, key="heartbeat")
    sink(pg, "beat", "S104 Log Heartbeat", period="0 sec")
    pg.connect("gen", "parse", ["success"], "s104-scans")
    pg.connect("parse", "barcode", ["success"], "s104-parsed")
    pg.connect("barcode", "depot", ["success"], "s104-valid")
    pg.connect("depot", "geofence", ["success"], "s104-to-geofence")
    pg.connect("geofence", "eta", ["success"], "s104-geofenced")
    pg.connect("eta", "notify", ["success"], "s104-eta")
    pg.connect("notify", "archive", ["success"], "s104-notified")
    pg.connect("archive", "publish", ["success"], "s104-archived")
    pg.connect("heartbeat", "beat", ["success"], "s104-heartbeats")


S104 = Scenario(
    id="S104",
    title="Geofence Check On A Slow Schedule",
    symptom="backlog",
    fault=("S104 Geofence Check runs its two tasks every 5 seconds, 0.4/s against 3 scans/s, so it is idle "
           "most of the time while its queue grows."),
    build=build_s104,
    kpis=(
        Kpi("S104", "connection", "s104-to-geofence", "queued_count", above=2000),
        Kpi("S104", "processor", "S104 Geofence Check", "flowfiles_out", alert=False, good="up",
            primary=False),
    ),
    family="FLOW_EDIT",
    kind="cascade",
    expected=(action("SET_RUN_SCHEDULE", "S104 Geofence Check", value="0 sec"),),
    acceptable=(action("SET_RUN_SCHEDULE", "S104 Geofence Check", value="100 ms"),),
    traps=(
        "8 tasks every 5 seconds is still 1.6/s against 3/s: the schedule is the limit, not the tasks.",
        "S104 Resolve Depot feeds the queue and keeps up with its own input.",
    ),
    wrong=(
        action("SET_CONCURRENT_TASKS", "S104 Geofence Check", value=8),
        action("SET_CONCURRENT_TASKS", "S104 Resolve Depot", value=4),
    ),
)


def build_s105(pg):
    # 41 prescriptions every 4 s (10.25/s) into a dispense writer with two
    # tasks every 200 ms (10/s): 0.25/s, 15 a minute, stays behind. The
    # queue crosses 2,500 nearly three hours after deploy.
    generator(pg, "S105 Prescription Intake", period="4 sec", batch=41, text=SCRIPT)
    stage(pg, "parse", "S105 Parse Prescription", period="0 sec")
    stage(pg, "prescriber", "S105 Verify Prescriber", period="0 sec")
    stage(pg, "interactions", "S105 Check Interactions", period="0 sec")
    stage(pg, "formulary", "S105 Check Formulary", period="0 sec")
    stage(pg, "price", "S105 Price Claim", period="0 sec")
    stage(pg, "write", "S105 Write Dispense Record", period="200 ms", tasks=2)
    stage(pg, "label", "S105 Print Label", period="0 sec")
    stage(pg, "notify", "S105 Notify Pharmacy", period="0 sec")
    sink(pg, "publish", "S105 Publish Fill", period="0 sec")
    pg.connect("gen", "parse", ["success"], "s105-intake")
    pg.connect("parse", "prescriber", ["success"], "s105-parsed")
    pg.connect("prescriber", "interactions", ["success"], "s105-verified")
    pg.connect("interactions", "formulary", ["success"], "s105-checked")
    pg.connect("formulary", "price", ["success"], "s105-covered")
    pg.connect("price", "write", ["success"], "s105-to-write")
    pg.connect("write", "label", ["success"], "s105-written")
    pg.connect("label", "notify", ["success"], "s105-labelled")
    pg.connect("notify", "publish", ["success"], "s105-notified")


S105 = Scenario(
    id="S105",
    title="Dispense Writer Slightly Behind",
    symptom="backlog",
    fault=("Prescriptions arrive at 10.25/s and S105 Write Dispense Record does 10/s: its queue grows by "
           "15 a minute and breaches its threshold hours after deploy."),
    build=build_s105,
    kpis=(
        Kpi("S105", "connection", "s105-to-write", "queued_count", above=2500),
        Kpi("S105", "processor", "S105 Write Dispense Record", "flowfiles_out", alert=False, good="up",
            primary=False),
    ),
    family="FLOW_EDIT",
    kind="slow_burn",
    detect_within=14400,
    expected=(action("SET_CONCURRENT_TASKS", "S105 Write Dispense Record", value=3),),
    acceptable=(action("SET_RUN_SCHEDULE", "S105 Write Dispense Record", value="100 ms"),),
    traps=(
        "The writer is busy and nearly keeps up; its output looks normal and only the queue trend shows it.",
        "A higher threshold moves the breach out by hours; the queue never stops growing.",
    ),
    wrong=(
        action("SET_BACKPRESSURE", "s105-to-write", object_threshold=20000),
        action("SET_CONCURRENT_TASKS", "S105 Price Claim", value=4),
    ),
)


def build_s106(pg):
    # Live manifests at 2/s. For minutes 15-19 of each hour the backfill
    # adds 15/s into a loader with one task every 100 ms (10/s): its queue
    # grows by 7/s for five minutes, to about 2,100 against an alert at
    # 3,000, and drains at 8/s in under five minutes.
    generator(pg, "S106 Manifest Backfill", period="* 15-19 * * * ?", batch=15, text=MANIFEST,
              strategy="CRON_DRIVEN")
    generator(pg, "S106 Live Manifests", period="1 sec", batch=2, text=MANIFEST, key="live")
    stage(pg, "parse", "S106 Parse Manifest", period="0 sec")
    stage(pg, "validate", "S106 Validate Containers", period="0 sec")
    stage(pg, "dedupe", "S106 Dedupe Manifest", period="0 sec")
    stage(pg, "route", "S106 Assign Berth", period="0 sec")
    stage(pg, "load", "S106 Load Manifest", period="100 ms", tasks=1)
    stage(pg, "index", "S106 Index Manifest", period="0 sec")
    stage(pg, "customs", "S106 Notify Customs", period="0 sec")
    sink(pg, "publish", "S106 Publish Arrivals", period="0 sec")
    pg.connect("gen", "parse", ["success"], "s106-backfill")
    pg.connect("live", "parse", ["success"], "s106-live")
    pg.connect("parse", "validate", ["success"], "s106-parsed")
    pg.connect("validate", "dedupe", ["success"], "s106-valid")
    pg.connect("dedupe", "route", ["success"], "s106-unique")
    pg.connect("route", "load", ["success"], "s106-to-load")
    pg.connect("load", "index", ["success"], "s106-loaded")
    pg.connect("index", "customs", ["success"], "s106-indexed")
    pg.connect("customs", "publish", ["success"], "s106-cleared")


S106 = Scenario(
    id="S106",
    title="Hourly Backfill That Drains",
    symptom="healthy",
    fault=("None. For five minutes each hour a manifest backfill fills the loader's queue to about 70% of "
           "its alert, and it drains within five minutes."),
    build=build_s106,
    kpis=(
        Kpi("S106", "connection", "s106-to-load", "queued_count", above=3000),
        Kpi("S106", "connection", "s106-unique", "queued_count", alert=False, primary=False),
        Kpi("S106", "processor", "S106 Load Manifest", "flowfiles_out", alert=False, good="up",
            primary=False),
    ),
    family="NONE",
    detect_within=7200,
    expected=(action("NO_ACTION"),),
    acceptable=(),
    traps=(
        "The queue climbs for five minutes and falls bucket after bucket after: it is draining.",
        "The loader is busy only while the backfill runs; more tasks would sit idle most of the hour.",
    ),
)


A = Pack(
    name="p26-holdout-a",
    title="Holdout: billing, parcels and pharmacy",
    scenarios=(S103, S104, S105, S106),
    wave=4,
    holdout=True,
)


# ---------------------------------------------------------------------
# p27-holdout-b
# ---------------------------------------------------------------------


def build_s107(pg):
    # 2 checks/s into an identity service that fails 2 in 10 transiently,
    # retried in place, but YIELD_PROCESSOR backs off the *processor*:
    # 5 s, doubling per retry of the same FlowFile up to 5 minutes, and
    # both tasks wait it out. It falls behind, and its queue grows.
    generator(pg, "S107 Identity Checks", period="1 sec", batch=2, text=CHECK)
    stage(pg, "parse", "S107 Parse Check", period="0 sec")
    stage(pg, "country", "S107 Resolve Country Rules", period="0 sec")
    debug_flow(pg, "verify", "S107 Verify Identity", successes=8, failures=2,
               period="0 sec", tasks=2, retried=("failure",), retry_count=5,
               backoff="YIELD_PROCESSOR", max_backoff="5 mins", yield_duration="5 sec")
    stage(pg, "liveness", "S107 Score Liveness", period="0 sec")
    stage(pg, "decide", "S107 Decide Outcome", period="0 sec")
    stage(pg, "record", "S107 Record Decision", period="0 sec")
    stage(pg, "notify", "S107 Notify Applicant", period="0 sec")
    sink(pg, "publish", "S107 Publish Verified", period="0 sec")
    pg.processor("errors", "S107 Verification Errors", "LogAttribute", {"Log Level": "warn"},
                 period="0 sec", tasks=1, auto_terminate=("success",))
    pg.connect("gen", "parse", ["success"], "s107-checks")
    pg.connect("parse", "country", ["success"], "s107-parsed")
    pg.connect("country", "verify", ["success"], "s107-to-verify")
    pg.connect("verify", "liveness", ["success"], "s107-verified")
    pg.connect("verify", "errors", ["failure"], "s107-failed")
    pg.connect("liveness", "decide", ["success"], "s107-scored")
    pg.connect("decide", "record", ["success"], "s107-decided")
    pg.connect("record", "notify", ["success"], "s107-recorded")
    pg.connect("notify", "publish", ["success"], "s107-notified")


S107 = Scenario(
    id="S107",
    title="Identity Service Backoff Stalls",
    symptom="failure_loop",
    fault=("S107 Verify Identity retries its transient failures with YIELD_PROCESSOR up to 5 minutes, so "
           "each failure stalls both tasks and the checks back up."),
    build=build_s107,
    kpis=(
        Kpi("S107", "connection", "s107-to-verify", "queued_count", above=500),
        Kpi("S107", "processor", "S107 Verify Identity", "flowfiles_in", alert=False, good="up",
            primary=False),
    ),
    family="FLOW_EDIT",
    kind="cascade",
    expected=(action("SET_RETRY_POLICY", "S107 Verify Identity", backoff="PENALIZE_FLOWFILE"),),
    acceptable=(
        action("SET_RETRY_POLICY", "S107 Verify Identity", backoff="PENALIZE_FLOWFILE",
               max_backoff="30 sec"),
    ),
    traps=(
        "More tasks do nothing while the processor is yielded; every task waits together.",
        "The queue is in front of the service, but the service is idle, not slow.",
    ),
    wrong=(
        action("SET_CONCURRENT_TASKS", "S107 Verify Identity", value=8),
        action("SET_CONCURRENT_TASKS", "S107 Resolve Country Rules", value=4),
    ),
)


def build_s108(pg):
    # One application in twenty is malformed: it fails, goes straight back
    # in and fails again, for good. The parser keeps up with its input
    # five times over, so the loop only grows, by one every 10 s.
    generator(pg, "S108 Onboarding Applications", period="1 sec", batch=2, text=APPLICATION)
    stage(pg, "normalise", "S108 Normalise Application", period="0 sec")
    pg.processor("parse", "S108 Parse KYC Document", "EvaluateJsonPath", {
        "Destination": "flowfile-attribute",
        "Return Type": "auto-detect",
        "kyc.applicant": "$.applicant",
    }, period="100 ms", tasks=1, auto_terminate=("unmatched",))
    stage(pg, "sanctions", "S108 Screen Sanctions", period="0 sec")
    stage(pg, "risk", "S108 Score Risk", period="0 sec")
    stage(pg, "address", "S108 Check Address", period="0 sec")
    stage(pg, "open", "S108 Open Account", period="0 sec")
    stage(pg, "notify", "S108 Notify Applicant", period="0 sec")
    sink(pg, "archive", "S108 Archive Application", period="0 sec")
    dlq(pg, "S108 KYC Review DLQ", connected=False)
    pg.connect("gen", "normalise", ["success"], "s108-applications")
    pg.connect("normalise", "parse", ["success"], "s108-to-parse")
    pg.connect("parse", "parse", ["failure"], "s108-failure-loop")
    pg.connect("parse", "sanctions", ["matched"], "s108-parsed")
    pg.connect("sanctions", "risk", ["success"], "s108-screened")
    pg.connect("risk", "address", ["success"], "s108-scored")
    pg.connect("address", "open", ["success"], "s108-checked")
    pg.connect("open", "notify", ["success"], "s108-opened")
    pg.connect("notify", "archive", ["success"], "s108-notified")


S108 = Scenario(
    id="S108",
    title="Malformed Applications Loop Forever",
    symptom="failure_loop",
    fault=("Malformed applications fail S108 Parse KYC Document and are routed straight back into it, so "
           "they pile up on the loop for good, retried on every run."),
    build=build_s108,
    kpis=(
        Kpi("S108", "connection", "s108-failure-loop", "queued_count", above=100),
        Kpi("S108", "connection", "s108-to-parse", "queued_count", above=500, primary=False),
    ),
    family="FLOW_EDIT",
    kind="cascade",
    expected=(action("ROUTE_RELATIONSHIP_TO_DLQ", "S108 Parse KYC Document", relationship="failure",
                     dlq="S108 KYC Review DLQ"),),
    acceptable=(),
    traps=(
        "More parser tasks or a shorter schedule retry the same malformed applications faster; none will "
        "ever parse.",
        "The parser keeps up with the good applications; it is not slow.",
    ),
    wrong=(
        action("SET_CONCURRENT_TASKS", "S108 Parse KYC Document", value=4),
        action("SET_RUN_SCHEDULE", "S108 Parse KYC Document", value="0 sec"),
    ),
)


def build_s109(pg):
    # Bid requests at 3/s all day. For minutes 0-14 of every third hour
    # the auction burst adds 15/s, into an impression recorder with two
    # tasks every 250 ms (8/s): its queue grows by 10/s, crosses 6,000
    # ten minutes in and reaches about 9,000. Once the burst ends it
    # drains at 5/s: back under 6,000 ten minutes later.
    generator(pg, "S109 Auction Burst", period="* 0-14 */3 * * ?", batch=15, text=BID,
              strategy="CRON_DRIVEN")
    generator(pg, "S109 Bid Requests", period="1 sec", batch=3, text=BID, key="bids")
    stage(pg, "parse", "S109 Parse Bid", period="0 sec")
    stage(pg, "bots", "S109 Filter Bots", period="0 sec")
    stage(pg, "dedupe", "S109 Dedupe Bid", period="0 sec")
    stage(pg, "segment", "S109 Enrich Segment", period="0 sec")
    stage(pg, "price", "S109 Price Bid", period="0 sec")
    stage(pg, "record", "S109 Record Impression", period="250 ms", tasks=2)
    stage(pg, "attribute", "S109 Attribute Campaign", period="0 sec")
    sink(pg, "publish", "S109 Publish Spend", period="0 sec")
    pg.connect("gen", "parse", ["success"], "s109-burst")
    pg.connect("bids", "parse", ["success"], "s109-bids")
    pg.connect("parse", "bots", ["success"], "s109-parsed")
    pg.connect("bots", "dedupe", ["success"], "s109-human")
    pg.connect("dedupe", "segment", ["success"], "s109-unique")
    pg.connect("segment", "price", ["success"], "s109-segmented")
    pg.connect("price", "record", ["success"], "s109-to-record")
    pg.connect("record", "attribute", ["success"], "s109-recorded")
    pg.connect("attribute", "publish", ["success"], "s109-attributed")


S109 = Scenario(
    id="S109",
    title="Auction Bursts Swamp The Recorder",
    symptom="backlog",
    fault=("For 15 minutes every third hour an auction burst adds 15/s to 3/s of bids, into a recorder that "
           "does 8/s: its queue fills, drains, and fills again with the next burst."),
    build=build_s109,
    kpis=(
        Kpi("S109", "connection", "s109-to-record", "queued_count", above=6000),
        Kpi("S109", "connection", "s109-unique", "queued_count", alert=False, primary=False),
        Kpi("S109", "processor", "S109 Record Impression", "flowfiles_out", alert=False, good="up",
            primary=False),
    ),
    family="FLOW_EDIT",
    kind="intermittent",
    detect_within=10800,
    clear_within=1800,
    expected=(action("SET_CONCURRENT_TASKS", "S109 Record Impression", value=6),),
    acceptable=(action("SET_RUN_SCHEDULE", "S109 Record Impression", value="0 sec"),),
    traps=(
        "Between bursts the queue drains on its own; an empty queue an hour later is not a fix.",
        "A higher threshold holds a bigger backlog for longer; the recorder is no faster.",
    ),
    wrong=(
        action("SET_BACKPRESSURE", "s109-to-record", object_threshold=50000),
        action("SET_CONCURRENT_TASKS", "S109 Enrich Segment", value=4),
    ),
)


def build_s110(pg):
    # One door alert every 2 s, copied 20 times, once per subscriber:
    # 10/s into a dispatcher on 0 sec that keeps up with ease. Its
    # FlowFiles Out is twenty times its input, by design.
    generator(pg, "S110 Sensor Alerts", period="2 sec", batch=1, text=ALERT)
    stage(pg, "parse", "S110 Parse Alert", period="0 sec")
    stage(pg, "site", "S110 Resolve Site", period="0 sec")
    pg.processor("fanout", "S110 Copy Per Subscriber", "DuplicateFlowFile", {"Number of Copies": "19"},
                 period="0 sec", tasks=1)
    stage(pg, "address", "S110 Address Subscriber", period="0 sec")
    stage(pg, "render", "S110 Render Message", period="0 sec")
    stage(pg, "throttle", "S110 Apply Quiet Hours", period="0 sec")
    stage(pg, "dispatch", "S110 Dispatch Message", period="0 sec")
    stage(pg, "audit", "S110 Audit Delivery", period="0 sec")
    sink(pg, "sent", "S110 Messages Sent", period="0 sec")
    pg.connect("gen", "parse", ["success"], "s110-alerts")
    pg.connect("parse", "site", ["success"], "s110-parsed")
    pg.connect("site", "fanout", ["success"], "s110-to-copy")
    pg.connect("fanout", "address", ["success"], "s110-copies")
    pg.connect("address", "render", ["success"], "s110-addressed")
    pg.connect("render", "throttle", ["success"], "s110-rendered")
    pg.connect("throttle", "dispatch", ["success"], "s110-to-dispatch")
    pg.connect("dispatch", "audit", ["success"], "s110-dispatched")
    pg.connect("audit", "sent", ["success"], "s110-audited")


S110 = Scenario(
    id="S110",
    title="Alert Fan-out Within Capacity",
    symptom="healthy",
    fault=("None. Each sensor alert is copied once per subscriber, twenty in all, and the dispatcher keeps "
           "up with the 10/s that makes."),
    build=build_s110,
    kpis=(
        Kpi("S110", "connection", "s110-to-dispatch", "queued_count", above=2000),
        Kpi("S110", "processor", "S110 Copy Per Subscriber", "flowfiles_in", alert=False, good="up",
            primary=False),
        Kpi("S110", "processor", "S110 Copy Per Subscriber", "flowfiles_out", alert=False, good="up",
            primary=False),
    ),
    family="NONE",
    detect_within=7200,
    expected=(action("NO_ACTION"),),
    acceptable=(),
    traps=(
        "Twenty FlowFiles out for each one in is the fan-out doing its job, not a flood.",
        "Number of Copies is the subscriber count; lowering it drops messages people should get.",
    ),
)


B = Pack(
    name="p27-holdout-b",
    title="Holdout: identity, onboarding and auctions",
    scenarios=(S107, S108, S109, S110),
    wave=4,
    holdout=True,
)
