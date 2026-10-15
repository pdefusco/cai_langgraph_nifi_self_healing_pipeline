"""
Pack p17-nightly-batch: a sizing fault that shows once a day.

S39's nightly archive job runs for 40 minutes from 02:00 UTC and
compresses at gzip level 9, about two cores of work: the EXTRA_SMALL
node's CPU is pinned while it runs and idle the rest of the day. It is
its own deployment because sizing changes every other scenario's
numbers.
"""

from flows.scenarios.base import Kpi, Pack, Scenario, action
from flows.scenarios.common import generator, sink, stage

AUDIT = '{"audit": "a-77310", "user": "svc-batch", "action": "export", "ts": 1760000000}'


def build_s39(pg):
    # 02:00-02:39 UTC: 15 MB/s of random bytes through gzip level 9
    # (~8 MB/s per core) then SHA-256, about 2 cores of work, which pins
    # an EXTRA_SMALL node (2 cores) and leaves a SMALL one (3) near 70%.
    # An audit trickle runs all day at 2/s, cheaply.
    generator(pg, "S39 Nightly Export", period="*/2 0-39 2 * * ?", batch=3, size="10 MB",
              binary=True, strategy="CRON_DRIVEN")
    pg.processor("compress", "S39 Compress Export", "CompressContent", {
        "Mode": "compress",
        "Compression Format": "gzip",
        "Compression Level": "9",
    }, period="0 sec", tasks=4, auto_terminate=("failure",))
    pg.processor("hash", "S39 Hash Export", "CryptographicHashContent", {"Hash Algorithm": "SHA-256"},
                 period="0 sec", tasks=2, auto_terminate=("failure",))
    stage(pg, "name", "S39 Name Archive", period="0 sec")
    stage(pg, "catalog", "S39 Catalog Archive", period="0 sec")
    sink(pg, "store", "S39 Store Archive", period="0 sec")
    pg.connect("gen", "compress", ["success"], "s39-to-compress")
    pg.connect("compress", "hash", ["success"], "s39-to-hash")
    pg.connect("hash", "name", ["success"], "s39-hashed")
    pg.connect("name", "catalog", ["success"], "s39-named")
    pg.connect("catalog", "store", ["success"], "s39-cataloged")

    generator(pg, "S39 Audit Events", period="1 sec", batch=2, text=AUDIT, key="audit")
    stage(pg, "parse", "S39 Parse Audit", period="0 sec")
    stage(pg, "mask", "S39 Mask Audit", period="0 sec")
    sink(pg, "index", "S39 Index Audit", period="0 sec")
    pg.connect("audit", "parse", ["success"], "s39-audit")
    pg.connect("parse", "mask", ["success"], "s39-parsed")
    pg.connect("mask", "index", ["success"], "s39-masked")


S39 = Scenario(
    id="S39",
    title="Nightly Batch Starves The CPU",
    symptom="sizing_cpu",
    fault=("For 40 minutes from 02:00 UTC the nightly export compresses at level 9, about all the "
           "CPU an EXTRA_SMALL node has; the rest of the day the node is nearly idle."),
    build=build_s39,
    kpis=(
        Kpi("S39", "system", "", "cpu", above=85, tolerance_seconds=300),
        Kpi("S39", "connection", "s39-to-compress", "queued_count", alert=False, primary=False),
        Kpi("S39", "processor", "S39 Index Audit", "flowfiles_in", alert=False, good="up", primary=False),
    ),
    family="SIZING",
    kind="intermittent",
    detect_within=86400,
    clear_within=1800,
    expected=(action("SCALE_DEPLOYMENT", cluster_size="SMALL", nodes=1),),
    acceptable=(
        action("SCALE_DEPLOYMENT", cluster_size="MEDIUM", nodes=1),
        action("SET_PROCESSOR_PROPERTY", "S39 Compress Export", property="Compression Level", value="1"),
    ),
    traps=(
        "The CPU is idle 23 hours a day: a fix judged at noon shows nothing either way.",
        "More S39 Compress Export tasks on a saturated node only add contention.",
        "Two nodes do not help: nothing load-balances the export across them.",
    ),
    wrong=(
        action("SET_CONCURRENT_TASKS", "S39 Compress Export", value=8),
        action("SET_CONCURRENT_TASKS", "S39 Hash Export", value=4),
    ),
)


PACK = Pack(
    name="p17-nightly-batch",
    title="Nightly batch sizing fault",
    scenarios=(S39,),
    cluster_size="EXTRA_SMALL",
    nodes=1,
    wave=2,
)
