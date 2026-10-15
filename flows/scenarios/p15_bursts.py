"""
Pack p15-bursts: faults that come and go on a schedule.

S35's campaign service sends a burst every two hours, more than the
loader can take: the backlog fills, drains in the quiet hour, and comes
back with the next burst. S36's partner export runs for ten minutes at
the top of each hour, and a quarter of its records fail validation into
an auto-terminated relationship: they vanish only while the export runs,
and the rest of the hour the flow looks healthy.

Cron expressions are Quartz, in UTC.
"""

from flows.scenarios.base import Kpi, Pack, Scenario, action
from flows.scenarios.common import debug_flow, dlq, generator, sink, stage

EVENT = '{"event": "click", "campaign": "fall-24", "user": "u-4410", "ts": 1760000000}'
ORDER = '{"order": "o-88120", "store": "st-12", "sku": "sku-301", "qty": 2}'


def build_s35(pg):
    # Web events trickle in at 2/s all day. For minutes 0-19 of every
    # second hour the campaign burst adds 20/s, into a loader with one
    # task every 100 ms (10/s): its queue fills at 12/s, crosses 8,000
    # 11 minutes in and is full at 14. Once the burst ends it drains at
    # 8/s: back under 8,000 about 4 minutes later, empty within the hour.
    generator(pg, "S35 Campaign Burst", period="* 0-19 */2 * * ?", batch=20, text=EVENT,
              strategy="CRON_DRIVEN")
    generator(pg, "S35 Web Events", period="1 sec", batch=2, text=EVENT, key="web")
    stage(pg, "parse", "S35 Parse Event", period="0 sec")
    stage(pg, "tag", "S35 Tag Campaign", period="0 sec")
    stage(pg, "dedupe", "S35 Dedupe Events", period="0 sec")
    stage(pg, "enrich", "S35 Enrich User", period="0 sec")
    stage(pg, "load", "S35 Load Events", period="100 ms", tasks=1)
    stage(pg, "index", "S35 Index Events", period="0 sec")
    stage(pg, "aggregate", "S35 Aggregate Campaign", period="0 sec")
    sink(pg, "publish", "S35 Publish Metrics", period="0 sec")
    pg.connect("gen", "parse", ["success"], "s35-burst")
    pg.connect("web", "parse", ["success"], "s35-web")
    pg.connect("parse", "tag", ["success"], "s35-parsed")
    pg.connect("tag", "dedupe", ["success"], "s35-tagged")
    pg.connect("dedupe", "enrich", ["success"], "s35-unique")
    pg.connect("enrich", "load", ["success"], "s35-to-load")
    pg.connect("load", "index", ["success"], "s35-loaded")
    pg.connect("index", "aggregate", ["success"], "s35-indexed")
    pg.connect("aggregate", "publish", ["success"], "s35-aggregated")


S35 = Scenario(
    id="S35",
    title="Bursts Overwhelm The Loader",
    symptom="backlog",
    fault=("For 20 minutes every second hour a campaign burst adds 20/s to 2/s of web events, into a "
           "loader that does 10/s: its queue fills, drains in the quiet hour, and fills again."),
    build=build_s35,
    kpis=(
        Kpi("S35", "connection", "s35-to-load", "queued_count", above=8000),
        Kpi("S35", "connection", "s35-unique", "queued_count", alert=False, primary=False),
        Kpi("S35", "processor", "S35 Load Events", "flowfiles_out", alert=False, good="up", primary=False),
    ),
    family="FLOW_EDIT",
    kind="intermittent",
    detect_within=7200,
    clear_within=1800,
    expected=(action("SET_CONCURRENT_TASKS", "S35 Load Events", value=4),),
    acceptable=(action("SET_RUN_SCHEDULE", "S35 Load Events", value="0 sec"),),
    traps=(
        "Between bursts the queue drains on its own; an empty queue an hour later is not a fix.",
        "A higher threshold holds a bigger backlog for longer; the loader is no faster.",
    ),
    wrong=(
        action("SET_BACKPRESSURE", "s35-to-load", object_threshold=20000),
        action("SET_CONCURRENT_TASKS", "S35 Enrich User", value=4),
    ),
)


def build_s36(pg):
    # Store orders arrive at 5/s all day. For minutes 0-9 of each hour the
    # partner export adds 10/s, and its validator fails 1 in 4 into an
    # auto-terminated relationship: 2.5/s vanish while the export runs,
    # nothing the rest of the hour. Its reject DLQ exists but nothing is
    # routed to it.
    generator(pg, "S36 Partner Export", period="* 0-9 * * * ?", batch=10, text=ORDER,
              strategy="CRON_DRIVEN")
    stage(pg, "map", "S36 Map Partner Fields", period="0 sec")
    debug_flow(pg, "validate", "S36 Validate Partner Order", successes=3, failures=1,
               period="0 sec", tasks=1, auto_terminate=("failure",))
    stage(pg, "enrich", "S36 Enrich Partner Order", period="0 sec")
    generator(pg, "S36 Store Orders", period="1 sec", batch=5, text=ORDER, key="store")
    stage(pg, "parse", "S36 Parse Store Order", period="0 sec")
    stage(pg, "route", "S36 Route By Region", period="0 sec")
    stage(pg, "price", "S36 Price Order", period="0 sec")
    sink(pg, "load", "S36 Load Orders", period="0 sec")
    dlq(pg, "S36 Reject DLQ", connected=False)
    pg.connect("gen", "map", ["success"], "s36-partner")
    pg.connect("map", "validate", ["success"], "s36-mapped")
    pg.connect("validate", "enrich", ["success"], "s36-valid")
    pg.connect("enrich", "route", ["success"], "s36-partner-enriched")
    pg.connect("store", "parse", ["success"], "s36-store")
    pg.connect("parse", "route", ["success"], "s36-store-parsed")
    pg.connect("route", "price", ["success"], "s36-routed")
    pg.connect("price", "load", ["success"], "s36-priced")


S36 = Scenario(
    id="S36",
    title="Hourly Bad Partner Records",
    symptom="data_loss",
    fault=("For ten minutes each hour the partner export runs, and its validator auto-terminates the "
           "quarter of its records that fail: they vanish, and only while the export runs."),
    build=build_s36,
    kpis=(
        # The validator's own in and out: a burst reaches each later stage
        # a little later, which a ratio across stages reads as loss while
        # the burst starts.
        Kpi("S36", "processor", "S36 Validate Partner Order", "flowfiles_in", alert=False, good="up",
            primary=False),
        Kpi("S36", "processor", "S36 Validate Partner Order", "flowfiles_out", alert=False, good="up"),
        # Decides: once rejects reach it, they are accounted for, not lost.
        Kpi("S36", "processor", "S36 Reject DLQ", "flowfiles_in", alert=False, good="up"),
    ),
    family="FLOW_EDIT",
    kind="intermittent",
    detect_within=3600,
    expected=(action("ROUTE_RELATIONSHIP_TO_DLQ", "S36 Validate Partner Order", relationship="failure",
                     dlq="S36 Reject DLQ"),),
    acceptable=(),
    traps=(
        "Fifty minutes of every hour the counts agree: the loss shows only while the export runs.",
        "More tasks on the validator reject just as many; it is not slow.",
    ),
    wrong=(
        action("SET_CONCURRENT_TASKS", "S36 Validate Partner Order", value=4),
        action("SET_CONCURRENT_TASKS", "S36 Map Partner Fields", value=4),
    ),
)


PACK = Pack(
    name="p15-bursts",
    title="Intermittent faults on a schedule",
    scenarios=(S35, S36),
    wave=2,
)
