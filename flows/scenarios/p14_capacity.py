"""
Pack p14-capacity: a sizing fault and a flow fault in one deployment.

S34 compresses archives at level 9, about two cores of work on an
EXTRA_SMALL node's two, and next to it sends notices at one a second
against four coming in. The CPU needs a bigger node (SCALE_DEPLOYMENT)
and the sender a faster run schedule (a flow edit): two families, so
two fixes. It is its own deployment because sizing changes every other
scenario's numbers.
"""

from flows.scenarios.base import Kpi, Pack, Scenario, action
from flows.scenarios.common import generator, sink, stage

NOTICE = '{"notice": "n-5120", "account": "acc-118", "channel": "email", "template": "archive-ready"}'


def build_s34(pg):
    # 15 MB/s of random bytes through gzip level 9 (~8 MB/s per core)
    # then SHA-256: about 2 cores of work, which saturates an EXTRA_SMALL
    # node (2 cores) and leaves a SMALL one (3) near 80%. Notices come in
    # at 4/s and the sender runs once a second: its queue fills at 3/s,
    # 2,000 in 11 minutes, whatever the CPU.
    generator(pg, "S34 Archive Source", period="2 sec", batch=3, size="10 MB", binary=True)
    pg.processor("compress", "S34 Compress Archive", "CompressContent", {
        "Mode": "compress",
        "Compression Format": "gzip",
        "Compression Level": "9",
    }, period="0 sec", tasks=4, auto_terminate=("failure",))
    pg.processor("hash", "S34 Hash Archive", "CryptographicHashContent", {"Hash Algorithm": "SHA-256"},
                 period="0 sec", tasks=2, auto_terminate=("failure",))
    stage(pg, "name", "S34 Name Archive", period="0 sec")
    stage(pg, "index", "S34 Index Archive", period="0 sec")
    sink(pg, "store", "S34 Store Archive", period="0 sec")
    pg.connect("gen", "compress", ["success"], "s34-to-compress")
    pg.connect("compress", "hash", ["success"], "s34-to-hash")
    pg.connect("hash", "name", ["success"], "s34-hashed")
    pg.connect("name", "index", ["success"], "s34-named")
    pg.connect("index", "store", ["success"], "s34-indexed")

    generator(pg, "S34 Notice Requests", period="1 sec", batch=4, text=NOTICE, key="notices")
    stage(pg, "format", "S34 Format Notice", period="0 sec")
    stage(pg, "render", "S34 Render Notice", period="0 sec")
    stage(pg, "send", "S34 Send Notice", period="1 sec")
    sink(pg, "sent", "S34 Notices Sent", period="0 sec")
    pg.connect("notices", "format", ["success"], "s34-notices")
    pg.connect("format", "render", ["success"], "s34-formatted")
    pg.connect("render", "send", ["success"], "s34-to-send", object_threshold=2000)
    pg.connect("send", "sent", ["success"], "s34-sent")


SCALE = action("SCALE_DEPLOYMENT", cluster_size="SMALL", nodes=1)
SEND = action("SET_RUN_SCHEDULE", "S34 Send Notice", value="0 sec")

S34 = Scenario(
    id="S34",
    title="CPU Starved And Slow Sender",
    symptom="sizing_cpu",
    fault=("Compression needs about all the CPU an EXTRA_SMALL node has, and S34 Send Notice runs "
           "once a second against 4/s: a sizing fault and a flow fault."),
    build=build_s34,
    kpis=(
        Kpi("S34", "system", "", "cpu", above=85, tolerance_seconds=300),
        Kpi("S34", "connection", "s34-to-compress", "queued_count", above=50, primary=False),
        Kpi("S34", "connection", "s34-to-send", "queued_count", above=1500),
    ),
    family="SIZING",
    kind="compound",
    faults=(SCALE, SEND),
    expected=(SCALE,),
    acceptable=(
        SEND,
        action("SCALE_DEPLOYMENT", cluster_size="MEDIUM", nodes=1),
        action("SET_PROCESSOR_PROPERTY", "S34 Compress Archive", property="Compression Level", value="1"),
        action("SET_CONCURRENT_TASKS", "S34 Send Notice", value=8),
    ),
    traps=(
        "The notice backlog is not the CPU: the sender runs once a second whatever the node size.",
        "More S34 Compress Archive tasks on a saturated node only add contention.",
        "Two nodes do not help: nothing load-balances the archives across them.",
    ),
    wrong=(
        action("SET_CONCURRENT_TASKS", "S34 Compress Archive", value=8),
        action("SET_BACKPRESSURE", "s34-to-send", object_threshold=10000),
    ),
)


PACK = Pack(
    name="p14-capacity",
    title="Capacity and flow compound fault",
    scenarios=(S34,),
    cluster_size="EXTRA_SMALL",
    nodes=1,
    wave=2,
)
