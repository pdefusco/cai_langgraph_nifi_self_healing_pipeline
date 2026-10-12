"""
Packs p6-sizing-cpu and p7-sizing-overprov: the deployment, not the flow.

S20 runs CPU-heavy work on an EXTRA_SMALL single node: the flow is fine,
the cluster is too small. S21 is the reverse, a MEDIUM two-node cluster
carrying a trickle: nothing is broken, it is just expensive.

Both are fixed with `dfworkload update-deployment`, which restarts NiFi
nodes; the agent must treat that as the riskiest family.

Each pack is its own deployment because sizing applies to a whole
deployment and would change every other scenario's numbers.
"""

from flows.scenarios.base import Kpi, Pack, Scenario, action
from flows.scenarios.common import generator, sink, stage


def build_s20(pg):
    # 15 MB/s of random bytes through gzip level 9 (~8 MB/s per core) then
    # SHA-256: about 2 cores of work on a 2-core node. The source runs on
    # the primary node only, so adding nodes adds capacity and not load;
    # ROUND_ROBIN on the first connection is what spreads the work.
    generator(pg, "S20 Payload Source", period="2 sec", batch=3, size="10 MB", binary=True,
              execution_node="PRIMARY")
    pg.processor("compress", "S20 Compress", "CompressContent", {
        "Mode": "compress",
        "Compression Format": "gzip",
        "Compression Level": "9",
    }, period="0 sec", tasks=4, auto_terminate=("failure",))
    pg.processor("hash", "S20 Hash", "CryptographicHashContent", {"Hash Algorithm": "SHA-256"},
                 period="0 sec", tasks=2, auto_terminate=("failure",))
    sink(pg, "archive", "S20 Archive", period="0 sec")
    pg.connect("gen", "compress", ["success"], "s20-to-compress", load_balance="ROUND_ROBIN")
    pg.connect("compress", "hash", ["success"], "s20-to-hash")
    pg.connect("hash", "archive", ["success"], "s20-hashed")


S20 = Scenario(
    id="S20",
    title="CPU Saturated",
    symptom="sizing_cpu",
    fault="Compression and hashing need about twice the CPU an EXTRA_SMALL node has.",
    build=build_s20,
    kpis=(
        Kpi("S20", "system", "", "cpu", above=85, tolerance_seconds=300),
        Kpi("S20", "connection", "s20-to-compress", "queued_count", above=50, primary=False),
    ),
    family="SIZING",
    expected=(action("SCALE_DEPLOYMENT", cluster_size="SMALL", nodes=2),),
    acceptable=(
        action("SCALE_DEPLOYMENT", cluster_size="EXTRA_SMALL", nodes=2),
        action("SET_PROCESSOR_PROPERTY", "S20 Compress", property="Compression Level", value="1"),
    ),
    traps=(
        "More S20 Compress tasks on a saturated node only add contention.",
        "Scaling to more nodes only helps because s20-to-compress load-balances.",
    ),
)


def build_s21(pg):
    # One small FlowFile every 10 s.
    generator(pg, "S21 Source", period="10 sec", batch=1)
    stage(pg, "transform", "S21 Transform", period="1 sec", tasks=1)
    sink(pg, "publish", "S21 Publish", period="1 sec", tasks=1)
    pg.connect("gen", "transform", ["success"], "s21-input")
    pg.connect("transform", "publish", ["success"], "s21-output")


S21 = Scenario(
    id="S21",
    title="Over-provisioned",
    symptom="sizing_cost",
    fault="A MEDIUM two-node cluster runs a flow that needs a fraction of one small node.",
    build=build_s21,
    kpis=(
        Kpi("S21", "system", "", "cpu", below=5, tolerance_seconds=1800, good="up"),
        Kpi("S21", "connection", "s21-input", "queued_count", above=100, primary=False),
    ),
    family="SIZING",
    expected=(action("SCALE_DEPLOYMENT", cluster_size="EXTRA_SMALL", nodes=1),),
    acceptable=(action("SCALE_DEPLOYMENT", cluster_size="SMALL", nodes=1),),
    traps=("A downsize is only a success if the queues stay flat afterwards.",),
)


P6 = Pack(
    name="p6-sizing-cpu",
    title="CPU sizing",
    scenarios=(S20,),
    cluster_size="EXTRA_SMALL",
    nodes=1,
)

P7 = Pack(
    name="p7-sizing-overprov",
    title="Over-provisioning",
    scenarios=(S21,),
    cluster_size="MEDIUM",
    nodes=2,
)
