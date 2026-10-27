"""
Pack p9-clickstream: web clicks sessionized, with the fault one or two
stages past where it shows.

S24 fans each click batch out into single clicks, more than the
one-task sessionizer can take: the backlog fills the queues back to the
fan-out, and the alert is on the queue two hops before the cause. S25
loses clicks instead: the sessionizer's buffer expires what it cannot
reach, so no queue grows and only the counts in and out disagree.
"""

from flows.scenarios.base import Kpi, Pack, Scenario, action
from flows.scenarios.common import dlq, generator, sink, stage

CLICKS = "ts,user,page\n" + "\n".join(f"1760000{i:03d},u-{i % 9},/p/{i % 7}" for i in range(50))
VIEW = '{"user": "u-9", "page": "/cart", "ms": 412, "agent": "Mozilla/5.0"}'


def _merge(pg, key: str, name: str) -> dict:
    return pg.processor(key, name, "MergeContent", {
        "Merge Strategy": "Bin-Packing Algorithm",
        "Merge Format": "Binary Concatenation",
        "Minimum Number of Entries": "500",
        "Maximum Number of Entries": "1000",
        "Max Bin Age": "1 min",
        "Maximum number of Bins": "5",
    }, period="0 sec", tasks=1, auto_terminate=("original", "failure"))


def build_s24(pg):
    # 2 batches/s of 50 clicks, split one click per FlowFile: 100/s into a
    # sessionizer with one task every 20 ms, 50/s. The surplus fills its
    # input in 200 s, then each queue before it in 200 s more: the splits
    # queue, two hops back, is full about 15 minutes in.
    generator(pg, "S24 Click Batches", period="1 sec", batch=2, text=CLICKS)
    pg.processor("split", "S24 Split Clicks", "SplitText", {
        "Line Split Count": "1", "Header Line Count": "1", "Remove Trailing Newlines": "true",
    }, period="0 sec", tasks=1, auto_terminate=("original", "failure"))
    stage(pg, "parse", "S24 Parse Click", period="0 sec")
    stage(pg, "bots", "S24 Drop Bots", period="0 sec")
    stage(pg, "geo", "S24 Geo Enrich", period="0 sec")
    stage(pg, "sessionize", "S24 Sessionize", period="20 ms", tasks=1)
    stage(pg, "score", "S24 Score Session", period="0 sec")
    _merge(pg, "merge", "S24 Merge Sessions")
    pg.processor("compress", "S24 Compress Sessions", "CompressContent", {
        "Mode": "compress", "Compression Format": "gzip", "Compression Level": "1",
    }, period="0 sec", tasks=1, auto_terminate=("failure",))
    sink(pg, "publish", "S24 Publish Sessions", period="0 sec")
    pg.connect("gen", "split", ["success"], "s24-batches")
    pg.connect("split", "parse", ["splits"], "s24-splits")
    pg.connect("parse", "bots", ["success"], "s24-parsed")
    pg.connect("bots", "geo", ["success"], "s24-humans")
    pg.connect("geo", "sessionize", ["success"], "s24-to-sessionize")
    pg.connect("sessionize", "score", ["success"], "s24-sessions")
    pg.connect("score", "merge", ["success"], "s24-scored")
    pg.connect("merge", "compress", ["merged"], "s24-merged")
    pg.connect("compress", "publish", ["success"], "s24-compressed")


S24 = Scenario(
    id="S24",
    title="Fan-out Into One Task",
    symptom="backlog",
    fault="Each batch splits into 50 clicks, 100/s, into a sessionizer with one task that does 50/s.",
    build=build_s24,
    kpis=(
        Kpi("S24", "connection", "s24-splits", "queued_count", above=8000),
        Kpi("S24", "connection", "s24-to-sessionize", "queued_count", alert=False, primary=False),
        Kpi("S24", "processor", "S24 Sessionize", "flowfiles_out", alert=False, good="up", primary=False),
    ),
    family="FLOW_EDIT",
    kind="cascade",
    clear_within=1800,
    expected=(action("SET_CONCURRENT_TASKS", "S24 Sessionize", value=3),),
    acceptable=(action("SET_RUN_SCHEDULE", "S24 Sessionize", value="0 sec"),),
    traps=(
        "The alert is on the splits queue, but S24 Parse Click is back-pressured, not slow.",
        "Fewer, larger splits change what a FlowFile is for every stage after the split.",
    ),
    wrong=(
        action("SET_CONCURRENT_TASKS", "S24 Parse Click", value=4),
        action("SET_CONCURRENT_TASKS", "S24 Split Clicks", value=2),
    ),
)


def build_s25(pg):
    # 20/s of page views into a sessionizer that does 10/s (one task every
    # 100 ms), behind a buffer that expires FlowFiles after a minute: the
    # buffer levels off near 1,300 and 10/s silently expire.
    generator(pg, "S25 Page Views", period="1 sec", batch=20, text=VIEW)
    pg.processor("parse", "S25 Parse View", "EvaluateJsonPath", {
        "Destination": "flowfile-attribute",
        "Return Type": "auto-detect",
        "view.page": "$.page",
    }, period="0 sec", tasks=1)
    stage(pg, "bots", "S25 Drop Bots", period="0 sec")
    stage(pg, "mask", "S25 Mask IP", period="0 sec")
    stage(pg, "geo", "S25 Geo Enrich", period="0 sec")
    stage(pg, "sessionize", "S25 Sessionize", period="100 ms", tasks=1)
    stage(pg, "score", "S25 Score Engagement", period="0 sec")
    _merge(pg, "merge", "S25 Merge Sessions")
    sink(pg, "publish", "S25 Publish Sessions", period="0 sec")
    dlq(pg, "S25 Parse DLQ")
    pg.connect("gen", "parse", ["success"], "s25-views")
    pg.connect("parse", "bots", ["matched"], "s25-parsed")
    pg.connect("parse", "dlq", ["unmatched", "failure"], "s25-unparsed")
    pg.connect("bots", "mask", ["success"], "s25-humans")
    pg.connect("mask", "geo", ["success"], "s25-masked")
    pg.connect("geo", "sessionize", ["success"], "s25-session-buffer", expiration="1 min")
    pg.connect("sessionize", "score", ["success"], "s25-sessions")
    pg.connect("score", "merge", ["success"], "s25-scored")
    pg.connect("merge", "publish", ["merged"], "s25-merged")


S25 = Scenario(
    id="S25",
    title="Expiring Session Buffer",
    symptom="data_loss",
    fault=("The sessionizer does 10/s of the 20/s coming in, and its buffer expires FlowFiles after "
           "a minute, so half the page views vanish without a queue growing."),
    build=build_s25,
    kpis=(
        Kpi("S25", "connection", "s25-session-buffer", "queued_count", above=5000, primary=False),
        Kpi("S25", "processor", "S25 Page Views", "flowfiles_out", alert=False, good="up", primary=False),
        Kpi("S25", "processor", "S25 Score Engagement", "flowfiles_in", alert=False, good="up"),
    ),
    family="FLOW_EDIT",
    kind="cascade",
    expected=(action("SET_CONCURRENT_TASKS", "S25 Sessionize", value=4),),
    # 3 tasks do 30/s and 40 ms 25/s against the 20/s coming in: less headroom, and both clear.
    acceptable=(action("SET_CONCURRENT_TASKS", "S25 Sessionize", value=3),
                action("SET_RUN_SCHEDULE", "S25 Sessionize", value="40 ms")),
    traps=(
        "Removing the expiration alone turns the loss into a backlog growing at 10/s.",
        "The buffer looks healthy, flat and well under its threshold; that is the symptom.",
    ),
    wrong=(
        action("SET_FLOWFILE_EXPIRATION", "s25-session-buffer", value="0 sec"),
        action("SET_CONCURRENT_TASKS", "S25 Geo Enrich", value=4),
    ),
)


PACK = Pack(
    name="p9-clickstream",
    title="Clickstream cascades",
    scenarios=(S24, S25),
    wave=2,
)
