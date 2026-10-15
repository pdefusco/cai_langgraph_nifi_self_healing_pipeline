"""
Pack p18-noisy-bursts: controls whose charts look alarming and are not.

S40's flash-sale bursts twice an hour push the loader's queue most of
the way to its threshold, and it drains well before the next one: a
trend warning, never a breach. S41 is S36 done right: the partner
export's bad records go to a connected DLQ, so a quarter of the
validator's input leaves on its failure relationship and none is lost.

Cron expressions are Quartz, in UTC.
"""

from flows.scenarios.base import Kpi, Pack, Scenario, action
from flows.scenarios.common import debug_flow, dlq, generator, sink, stage

EVENT = '{"event": "view", "sale": "flash-12", "user": "u-2071", "ts": 1760000000}'
ORDER = '{"order": "o-91402", "store": "st-08", "sku": "sku-117", "qty": 1}'


def build_s40(pg):
    # Web events trickle in at 2/s. For minutes 0-2 and 30-32 of each
    # hour the flash sale adds 30/s, into a loader with one task every
    # 100 ms (10/s): its queue grows at 22/s for 3 minutes, to about
    # 4,000 against a threshold of 5,000, and drains at 8/s in about 8
    # minutes, long before the next burst.
    generator(pg, "S40 Flash Sale Burst", period="* 0-2,30-32 * * * ?", batch=30, text=EVENT,
              strategy="CRON_DRIVEN")
    generator(pg, "S40 Web Events", period="1 sec", batch=2, text=EVENT, key="web")
    stage(pg, "parse", "S40 Parse Event", period="0 sec")
    stage(pg, "tag", "S40 Tag Sale", period="0 sec")
    stage(pg, "dedupe", "S40 Dedupe Events", period="0 sec")
    stage(pg, "enrich", "S40 Enrich User", period="0 sec")
    stage(pg, "load", "S40 Load Events", period="100 ms", tasks=1)
    stage(pg, "index", "S40 Index Events", period="0 sec")
    stage(pg, "aggregate", "S40 Aggregate Sale", period="0 sec")
    sink(pg, "publish", "S40 Publish Metrics", period="0 sec")
    pg.connect("gen", "parse", ["success"], "s40-burst")
    pg.connect("web", "parse", ["success"], "s40-web")
    pg.connect("parse", "tag", ["success"], "s40-parsed")
    pg.connect("tag", "dedupe", ["success"], "s40-tagged")
    pg.connect("dedupe", "enrich", ["success"], "s40-unique")
    pg.connect("enrich", "load", ["success"], "s40-to-load")
    pg.connect("load", "index", ["success"], "s40-loaded")
    pg.connect("index", "aggregate", ["success"], "s40-indexed")
    pg.connect("aggregate", "publish", ["success"], "s40-aggregated")


S40 = Scenario(
    id="S40",
    title="Bursts Within Capacity",
    symptom="healthy",
    fault=("None. Twice an hour a three-minute flash sale fills the loader's queue to about 80% of "
           "its threshold, and it drains within ten minutes."),
    build=build_s40,
    kpis=(
        Kpi("S40", "connection", "s40-to-load", "queued_count", above=5000, tolerance_seconds=60),
        Kpi("S40", "connection", "s40-unique", "queued_count", alert=False, primary=False),
        Kpi("S40", "processor", "S40 Load Events", "flowfiles_out", alert=False, good="up", primary=False),
    ),
    family="NONE",
    detect_within=7200,
    expected=(action("NO_ACTION"),),
    acceptable=(),
    traps=(
        "The queue climbs steeply for three minutes and a trend warns: it stops short and drains.",
        "The loader is busy only during the bursts; more tasks would sit idle the rest of the hour.",
    ),
)


def build_s41(pg):
    # Store orders arrive at 5/s all day. For minutes 0-9 of each hour the
    # partner export adds 10/s, and its validator fails 3 in 10 into a
    # connected reject DLQ: 3/s are dead-lettered while the export runs,
    # and every FlowFile in comes out.
    generator(pg, "S41 Partner Export", period="* 0-9 * * * ?", batch=10, text=ORDER,
              strategy="CRON_DRIVEN")
    stage(pg, "map", "S41 Map Partner Fields", period="0 sec")
    debug_flow(pg, "validate", "S41 Validate Partner Order", successes=7, failures=3,
               period="0 sec", tasks=1)
    stage(pg, "enrich", "S41 Enrich Partner Order", period="0 sec")
    generator(pg, "S41 Store Orders", period="1 sec", batch=5, text=ORDER, key="store")
    stage(pg, "parse", "S41 Parse Store Order", period="0 sec")
    stage(pg, "route", "S41 Route By Region", period="0 sec")
    stage(pg, "price", "S41 Price Order", period="0 sec")
    sink(pg, "load", "S41 Load Orders", period="0 sec")
    dlq(pg, "S41 Reject DLQ")
    pg.connect("gen", "map", ["success"], "s41-partner")
    pg.connect("map", "validate", ["success"], "s41-mapped")
    pg.connect("validate", "enrich", ["success"], "s41-valid")
    pg.connect("validate", "dlq", ["failure"], "s41-rejected")
    pg.connect("enrich", "route", ["success"], "s41-partner-enriched")
    pg.connect("store", "parse", ["success"], "s41-store")
    pg.connect("parse", "route", ["success"], "s41-store-parsed")
    pg.connect("route", "price", ["success"], "s41-routed")
    pg.connect("price", "load", ["success"], "s41-priced")


S41 = Scenario(
    id="S41",
    title="Expected Rejects In The DLQ",
    symptom="healthy",
    fault=("None. For ten minutes each hour the partner export runs, and the 30% of its records that "
           "fail validation go to a connected DLQ, as designed."),
    build=build_s41,
    kpis=(
        Kpi("S41", "processor", "S41 Validate Partner Order", "flowfiles_in", alert=False, good="up",
            primary=False),
        Kpi("S41", "processor", "S41 Validate Partner Order", "flowfiles_out", alert=False, good="up"),
        Kpi("S41", "processor", "S41 Reject DLQ", "flowfiles_in", alert=False, primary=False),
        Kpi("S41", "connection", "s41-priced", "queued_count", above=5000),
    ),
    family="NONE",
    detect_within=7200,
    expected=(action("NO_ACTION"),),
    acceptable=(),
    traps=(
        "The DLQ fills every hour: those records are rejected on purpose and accounted for.",
        "The sink takes in less than the sources send while the export runs; the difference is in "
        "the DLQ, not lost.",
    ),
)


PACK = Pack(
    name="p18-noisy-bursts",
    title="Controls: bursts and expected rejects",
    scenarios=(S40, S41),
    wave=2,
)
