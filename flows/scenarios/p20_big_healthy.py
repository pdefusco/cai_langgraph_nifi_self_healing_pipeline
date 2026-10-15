"""
Pack p20-big-healthy: controls that are large or busy, and healthy.

S44 is a 30-processor sensor pipeline: a split fans each batch out, a
balancer spreads it over three regional lanes, and a merge batches it
back up, so in and out counts differ wildly from stage to stage and
none of it is loss. S45 keeps the node's CPU busy all day, more so at
lunch, and still has headroom: busy is what the node is paid for.

Cron expressions are Quartz, in UTC.
"""

from flows.scenarios.base import Kpi, Pack, Scenario, action
from flows.scenarios.common import debug_flow, dlq, generator, sink, stage

READINGS = "ts,sensor,value\n" + "\n".join(f"1760000{i:03d},s-{i % 6},{20 + i % 9}.{i % 10}" for i in range(20))
HEARTBEAT = '{"gateway": "gw-14", "status": "ok", "ts": 1760000000}'
EVENT = '{"device": "d-6012", "event": "upload", "bytes": 1048576, "ts": 1760000000}'
REGIONS = ("1", "2", "3")


def build_s44(pg):
    # 2 batches/s of 20 readings, split one per FlowFile: 40/s. 1 in 20
    # fails validation into a connected DLQ. A rate limit of 100/s, far
    # above the load, then three lanes of ~13/s, each with one stage
    # doing 20/s. The merge batches 500-1,000 readings, about one batch
    # every 13 s, and sends its originals to an audit sink. A heartbeat
    # side flow runs at 1/s.
    generator(pg, "S44 Sensor Batches", period="1 sec", batch=2, text=READINGS)
    pg.processor("split", "S44 Split Readings", "SplitText", {
        "Line Split Count": "1", "Header Line Count": "1", "Remove Trailing Newlines": "true",
    }, period="0 sec", tasks=1, auto_terminate=("failure",))
    sink(pg, "raw", "S44 Archive Raw Batch", period="0 sec")
    stage(pg, "parse", "S44 Parse Reading", period="0 sec")
    debug_flow(pg, "validate", "S44 Validate Reading", successes=19, failures=1, period="0 sec", tasks=1)
    dlq(pg, "S44 Invalid DLQ")
    stage(pg, "convert", "S44 Convert Units", period="0 sec")
    stage(pg, "tag", "S44 Tag Site", period="0 sec")
    stage(pg, "dedupe", "S44 Dedupe Readings", period="0 sec")
    pg.processor("smooth", "S44 Smooth Rate", "ControlRate", {
        "Rate Control Criteria": "flowfile count",
        "Maximum Rate": "6000",
        "Time Duration": "1 min",
    }, period="0 sec", tasks=1, auto_terminate=("failure",))
    pg.processor("balance", "S44 Balance Regions", "DistributeLoad", {
        "Number of Relationships": str(len(REGIONS)),
        "Distribution Strategy": "round robin",
    }, period="0 sec", tasks=1)
    stage(pg, "join", "S44 Join Regions", period="0 sec")

    for region in REGIONS:
        stage(pg, f"enrich{region}", f"S44 Enrich Region {region}", period="0 sec")
        stage(pg, f"score{region}", f"S44 Score Region {region}", period="50 ms", tasks=1)
        pg.connect("balance", f"enrich{region}", [region], f"s44-to-region-{region}")
        pg.connect(f"enrich{region}", f"score{region}", ["success"], f"s44-enriched-{region}")
        pg.connect(f"score{region}", "join", ["success"], f"s44-scored-{region}")

    stage(pg, "window", "S44 Window Stats", period="0 sec")
    stage(pg, "detect", "S44 Detect Anomalies", period="0 sec")
    stage(pg, "format", "S44 Format Output", period="0 sec")
    pg.processor("merge", "S44 Batch Readings", "MergeContent", {
        "Merge Strategy": "Bin-Packing Algorithm",
        "Merge Format": "Binary Concatenation",
        "Minimum Number of Entries": "500",
        "Maximum Number of Entries": "1000",
        "Max Bin Age": "1 min",
        "Maximum number of Bins": "5",
    }, period="0 sec", tasks=1, auto_terminate=("failure",))
    sink(pg, "audit", "S44 Audit Originals", period="0 sec")
    pg.processor("compress", "S44 Compress Batch", "CompressContent", {
        "Mode": "compress", "Compression Format": "gzip", "Compression Level": "1",
    }, period="0 sec", tasks=1, auto_terminate=("failure",))
    pg.processor("hash", "S44 Checksum Batch", "CryptographicHashContent", {"Hash Algorithm": "SHA-256"},
                 period="0 sec", tasks=1, auto_terminate=("failure",))
    stage(pg, "name", "S44 Name Batch", period="0 sec")
    sink(pg, "store", "S44 Store Batch", period="0 sec")
    pg.connect("gen", "split", ["success"], "s44-batches")
    pg.connect("split", "raw", ["original"], "s44-raw")
    pg.connect("split", "parse", ["splits"], "s44-splits")
    pg.connect("parse", "validate", ["success"], "s44-parsed")
    pg.connect("validate", "dlq", ["failure"], "s44-invalid")
    pg.connect("validate", "convert", ["success"], "s44-valid")
    pg.connect("convert", "tag", ["success"], "s44-converted")
    pg.connect("tag", "dedupe", ["success"], "s44-tagged")
    pg.connect("dedupe", "smooth", ["success"], "s44-unique")
    pg.connect("smooth", "balance", ["success"], "s44-smoothed")
    pg.connect("join", "window", ["success"], "s44-joined")
    pg.connect("window", "detect", ["success"], "s44-windowed")
    pg.connect("detect", "format", ["success"], "s44-checked")
    pg.connect("format", "merge", ["success"], "s44-to-merge")
    pg.connect("merge", "audit", ["original"], "s44-originals")
    pg.connect("merge", "compress", ["merged"], "s44-merged")
    pg.connect("compress", "hash", ["success"], "s44-compressed")
    pg.connect("hash", "name", ["success"], "s44-hashed")
    pg.connect("name", "store", ["success"], "s44-named")

    generator(pg, "S44 Gateway Heartbeats", period="1 sec", batch=1, text=HEARTBEAT, key="beat")
    stage(pg, "check", "S44 Check Heartbeat", period="0 sec")
    sink(pg, "log", "S44 Log Heartbeat", period="0 sec")
    pg.connect("beat", "check", ["success"], "s44-heartbeats")
    pg.connect("check", "log", ["success"], "s44-beats-checked")


S44 = Scenario(
    id="S44",
    title="Thirty-Stage Healthy Pipeline",
    symptom="healthy",
    fault=("None. A 30-processor pipeline that splits, validates into a DLQ, rate-limits well above "
           "its load, balances over three lanes and merges, all within capacity."),
    build=build_s44,
    kpis=(
        Kpi("S44", "connection", "s44-splits", "queued_count", above=8000),
        Kpi("S44", "connection", "s44-to-region-1", "queued_count", above=2000, primary=False),
        Kpi("S44", "connection", "s44-to-merge", "queued_count", above=5000, primary=False),
        Kpi("S44", "processor", "S44 Validate Reading", "flowfiles_in", alert=False, good="up", primary=False),
        Kpi("S44", "processor", "S44 Validate Reading", "flowfiles_out", alert=False, good="up", primary=False),
    ),
    family="NONE",
    detect_within=7200,
    expected=(action("NO_ACTION"),),
    acceptable=(),
    traps=(
        "The merge sends on one FlowFile for every 500 or more it takes in: that is batching, not loss.",
        "The rate limit is far above the load, and the lanes keep up; nothing here is a bottleneck.",
    ),
)


def build_s45(pg):
    # Device uploads: 14 MB/s of random bytes all day, 20 MB/s through the
    # lunch hour (12:00-12:59 UTC), through gzip level 6 (~25 MB/s per
    # core) with two tasks, then SHA-256. With the rest of the pack, about
    # 1.1 of the EXTRA_SMALL node's 2 cores all day and 1.5 at lunch:
    # 60% CPU, 75% at lunch, with headroom left. An event trickle runs at 2/s.
    generator(pg, "S45 Device Uploads", period="1 sec", batch=14, size="1 MB", binary=True)
    generator(pg, "S45 Lunch Uploads", period="* * 12 * * ?", batch=6, size="1 MB", binary=True,
              key="lunch", strategy="CRON_DRIVEN")
    pg.processor("compress", "S45 Compress Upload", "CompressContent", {
        "Mode": "compress",
        "Compression Format": "gzip",
        "Compression Level": "6",
    }, period="0 sec", tasks=2, auto_terminate=("failure",))
    pg.processor("hash", "S45 Hash Upload", "CryptographicHashContent", {"Hash Algorithm": "SHA-256"},
                 period="0 sec", tasks=1, auto_terminate=("failure",))
    stage(pg, "name", "S45 Name Upload", period="0 sec")
    stage(pg, "catalog", "S45 Catalog Upload", period="0 sec")
    sink(pg, "store", "S45 Store Upload", period="0 sec")
    pg.connect("gen", "compress", ["success"], "s45-uploads")
    pg.connect("lunch", "compress", ["success"], "s45-lunch")
    pg.connect("compress", "hash", ["success"], "s45-compressed")
    pg.connect("hash", "name", ["success"], "s45-hashed")
    pg.connect("name", "catalog", ["success"], "s45-named")
    pg.connect("catalog", "store", ["success"], "s45-cataloged")

    generator(pg, "S45 Device Events", period="1 sec", batch=2, text=EVENT, key="events")
    stage(pg, "parse", "S45 Parse Event", period="0 sec")
    sink(pg, "index", "S45 Index Event", period="0 sec")
    pg.connect("events", "parse", ["success"], "s45-events")
    pg.connect("parse", "index", ["success"], "s45-parsed")


S45 = Scenario(
    id="S45",
    title="Busy But Sized Right",
    symptom="healthy",
    fault=("None. Compression keeps the node's CPU near 60% all day and 75% at lunch, with headroom "
           "left and no backlog."),
    build=build_s45,
    kpis=(
        Kpi("S45", "system", "", "cpu", above=85, tolerance_seconds=300),
        Kpi("S45", "connection", "s45-uploads", "queued_count", above=2000, primary=False),
        Kpi("S45", "processor", "S45 Compress Upload", "flowfiles_out", alert=False, good="up",
            primary=False),
    ),
    family="NONE",
    detect_within=86400,
    expected=(action("NO_ACTION"),),
    acceptable=(),
    traps=(
        "75% CPU at lunch is a node earning its keep: a larger size would cost more and change nothing.",
        "A lower compression level would cut CPU but grow every archive; nothing needs it.",
    ),
)


PACK = Pack(
    name="p20-big-healthy",
    title="Controls: large and busy",
    scenarios=(S44, S45),
    wave=2,
)
