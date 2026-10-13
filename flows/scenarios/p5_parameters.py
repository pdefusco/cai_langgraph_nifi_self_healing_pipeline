"""
Pack p5-parameters: right flow, wrong values.

S17 and S18 read their faulty values from the parameter context
`nfh-p5-parameters`, which CDF exposes as a parameter group. The fix is
`update-flow-in-deployment --parameter-groups`, which needs no new flow
version; editing the flow instead would be the wrong lever. The correct
values are in playbooks/parameter-misconfiguration.md, the runbook an
operator would consult.

S19 is the healthy control: the agent should find nothing to do.

S17's partner endpoint is a ListenHTTP inside the same flow, so the
"fixed" URL works inside any CDF deployment without network access. A
listen component makes CDF check for an inbound connection endpoint,
which this pack deliberately does not have (`has_listen_components`).
"""

from flows.scenarios.base import Kpi, Pack, Scenario, action
from flows.scenarios.common import generator, invoke_http, sink

CONTEXT = "nfh-p5-parameters"

STUB_PORT = "9876"
CORRECT_URL = f"http://localhost:{STUB_PORT}/ingest"
WRONG_URL = "http://10.255.255.1:8080/ingest"


def build_s17(pg):
    # One request per 5 s; each waits out a 30 s connect timeout against
    # the wrong address, so the queue grows by ~5 every 30 s.
    generator(pg, "S17 Source", period="5 sec", batch=1)
    invoke_http(pg, "call", "S17 Partner Call", "#{s17.target.url}", connect_timeout="30 secs",
                period="0 sec", tasks=1, auto_terminate=("Original", "Response"))
    pg.processor("failed", "S17 Failed Requests", "LogAttribute", {"Log Level": "warn"},
                 period="0 sec", tasks=1, auto_terminate=("success",))
    pg.processor("stub", "S17 Partner Stub", "ListenHTTP", {
        "Listening Port": STUB_PORT,
        "Base Path": "ingest",
    }, period="0 sec", tasks=1)
    sink(pg, "received", "S17 Received", period="0 sec")
    pg.connect("gen", "call", ["success"], "s17-requests")
    pg.connect("call", "failed", ["Failure", "No Retry", "Retry"], "s17-failed")
    pg.connect("stub", "received", ["success"], "s17-received")


S17 = Scenario(
    id="S17",
    title="Wrong Endpoint Parameter",
    symptom="misconfiguration",
    fault="The s17.target.url parameter points at an address nothing listens on.",
    build=build_s17,
    kpis=(
        Kpi("S17", "connection", "s17-requests", "queued_count", above=50),
        Kpi("S17", "processor", "S17 Partner Stub", "flowfiles_out", below=0.001,
            tolerance_seconds=600, good="up", primary=False),
    ),
    family="PARAMETER",
    expected=(action("UPDATE_PARAMETER", CONTEXT, parameter="s17.target.url", value=CORRECT_URL),),
    acceptable=(),
    traps=(
        "Shortening the timeout or adding tasks makes failures faster; nothing reaches the partner.",
        "Editing HTTP URL in the flow replaces the parameter reference and forks the configuration.",
    ),
    parameters={"s17.target.url": WRONG_URL},
    # Delivery crosses HTTP, not a connection: the partner's side is what arrives.
    edges=(("S17 Source",), ("S17 Received",)),
)


def build_s18(pg):
    # One record every 10 s copied 50 times, against a sink doing 1/s.
    generator(pg, "S18 Source", period="10 sec", batch=1)
    pg.processor("fanout", "S18 Fan Out", "DuplicateFlowFile", {"Number of Copies": "#{s18.copies}"},
                 period="0 sec", tasks=1)
    sink(pg, "sink", "S18 Sink", period="1 sec", tasks=1)
    pg.connect("gen", "fanout", ["success"], "s18-to-fanout")
    pg.connect("fanout", "sink", ["success"], "s18-copies")


S18 = Scenario(
    id="S18",
    title="Load-test Parameter Left On",
    symptom="misconfiguration",
    fault="s18.copies is still 50, the load-test value; production expects 1.",
    build=build_s18,
    kpis=(Kpi("S18", "connection", "s18-copies", "queued_count", above=1000),),
    family="PARAMETER",
    expected=(action("UPDATE_PARAMETER", CONTEXT, parameter="s18.copies", value="1"),),
    acceptable=(),
    traps=("Speeding up S18 Sink processes 50x the intended records downstream.",),
    parameters={"s18.copies": "50"},
)


def build_s19(pg):
    # 1/s through two stages with 4/s of capacity each.
    generator(pg, "S19 Source", period="1 sec", batch=1)
    pg.processor("transform", "S19 Transform", "UpdateAttribute", {"Store State": "Do not store state"},
                 period="500 ms", tasks=2)
    sink(pg, "publish", "S19 Publish", period="500 ms", tasks=2)
    pg.connect("gen", "transform", ["success"], "s19-input")
    pg.connect("transform", "publish", ["success"], "s19-output")


S19 = Scenario(
    id="S19",
    title="Healthy Control",
    symptom="healthy",
    fault="None. A correctly sized flow, to check the agent leaves it alone.",
    build=build_s19,
    kpis=(
        Kpi("S19", "connection", "s19-input", "queued_count", above=500),
        Kpi("S19", "connection", "s19-output", "queued_count", above=500, primary=False),
    ),
    family="NONE",
    expected=(action("NO_ACTION"),),
    acceptable=(),
    traps=("Any change here is a false positive.",),
)


PACK = Pack(
    name="p5-parameters",
    title="Parameters and control",
    scenarios=(S17, S18, S19),
    parameter_context=CONTEXT,
    has_listen_components=True,
)
