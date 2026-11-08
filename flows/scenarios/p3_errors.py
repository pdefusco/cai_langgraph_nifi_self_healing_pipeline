"""
Pack p3-errors: failures handled badly.

S09 and S10 loop failures back into the processor that produced them:
S09's malformed records pile up there for good, S10's retries crowd out
new work. S11 is a configuration bug that looks
like a slow quarantine. S12 uses NiFi's own retry, but with a backoff
that stalls the whole processor.
"""

from flows.scenarios.base import Kpi, Pack, Scenario, action
from flows.scenarios.common import debug_flow, dlq, generator, invoke_http, sink

BLACKHOLE_URL = "http://10.255.255.1:8080/orders"
# One in ten has no value: {"id": } is not JSON.
S09_RECORD = """{"id": ${random():mod(10):equals(0):ifElse('','1')}}"""


def build_s09(pg):
    # One record in ten is malformed: it fails, goes straight back in and
    # fails again, for good. The validator keeps up with the input four
    # times over, so the loop only grows, by one record every 10 s, and no
    # number of tasks or runs gets a malformed record through.
    generator(pg, "S09 Source", period="1 sec", batch=1, text=S09_RECORD)
    pg.processor("validator", "S09 Validator", "EvaluateJsonPath", {
        "Destination": "flowfile-attribute",
        "Return Type": "auto-detect",
        "order.id": "$.id",
    }, period="250 ms", tasks=1, auto_terminate=("unmatched",))
    sink(pg, "publish", "S09 Publish", period="0 sec")
    dlq(pg, "S09 DLQ", connected=False)
    pg.connect("gen", "validator", ["success"], "s09-input")
    pg.connect("validator", "validator", ["failure"], "s09-failure-loop")
    pg.connect("validator", "publish", ["matched"], "s09-valid")


S09 = Scenario(
    id="S09",
    title="Failure Self-Loop",
    symptom="failure_loop",
    fault="Malformed records fail and are routed back into the validator with no limit, so they pile up there "
          "for good, retried on every run.",
    build=build_s09,
    kpis=(
        Kpi("S09", "connection", "s09-failure-loop", "queued_count", above=100),
        # Only once the loop is full and back pressure stops the validator.
        Kpi("S09", "connection", "s09-input", "queued_count", above=500, primary=False),
    ),
    family="FLOW_EDIT",
    expected=(action("ROUTE_RELATIONSHIP_TO_DLQ", "S09 Validator", relationship="failure", dlq="S09 DLQ"),),
    acceptable=(),
    traps=(
        "More validator tasks or a shorter run schedule only retry the same malformed records faster: "
        "none of them will ever parse.",
        "SET_RETRY_POLICY alone still lands exhausted FlowFiles back on the self-loop.",
    ),
    wrong=(
        action("SET_CONCURRENT_TASKS", "S09 Validator", value=4),
        action("SET_RUN_SCHEDULE", "S09 Validator", value="0 sec"),
    ),
)


def build_s10(pg):
    # A partner endpoint that does not answer: every call waits out the
    # 60 s connect timeout, then Failure loops straight back.
    generator(pg, "S10 Source", period="2 sec", batch=1)
    invoke_http(pg, "call", "S10 Partner Call", BLACKHOLE_URL, connect_timeout="60 secs",
                period="0 sec", tasks=1, auto_terminate=("Original", "Response"))
    dlq(pg, "S10 DLQ", connected=False)
    pg.connect("gen", "call", ["success"], "s10-requests")
    pg.connect("call", "call", ["Failure", "No Retry", "Retry"], "s10-retry-loop")


S10 = Scenario(
    id="S10",
    title="Unreachable Endpoint",
    symptom="failure_loop",
    fault="Calls to an unreachable host wait 60 s each and every failure is retried immediately.",
    build=build_s10,
    kpis=(
        Kpi("S10", "connection", "s10-requests", "queued_count", above=100),
        Kpi("S10", "connection", "s10-retry-loop", "queued_count", alert=False, primary=False),
    ),
    family="FLOW_EDIT",
    expected=(
        action("ROUTE_RELATIONSHIP_TO_DLQ", "S10 Partner Call",
               relationship=["Failure", "No Retry", "Retry"], dlq="S10 DLQ"),
        action("SET_PROCESSOR_PROPERTY", "S10 Partner Call",
               property="Connection Timeout", value="5 secs"),
        action("SET_CONCURRENT_TASKS", "S10 Partner Call", value=4),
    ),
    acceptable=(),
    traps=(
        "Concurrency alone: 8 tasks x 1 call per 60 s is still 0.13/s against 0.5/s in.",
        "The endpoint is down, not slow; nothing in the flow makes it answer.",
    ),
)


def build_s11(pg):
    # The JSON has order_id; the path asks for orderId. Every FlowFile is
    # unmatched and the quarantine (one run per 10 s) is blamed.
    generator(pg, "S11 Order Source", period="1 sec", batch=1,
              text='{"order_id": 42, "amount": 10.5}')
    pg.processor("extract", "S11 Extract Order", "EvaluateJsonPath", {
        "Destination": "flowfile-content",
        "Return Type": "auto-detect",
        "orderId": "$.orderId",
    }, period="0 sec", tasks=1)
    sink(pg, "publish", "S11 Publish", period="0 sec")
    pg.processor("quarantine", "S11 Quarantine", "LogAttribute", {"Log Level": "warn"},
                 period="10 sec", tasks=1, auto_terminate=("success",))
    pg.connect("gen", "extract", ["success"], "s11-input")
    pg.connect("extract", "publish", ["matched"], "s11-matched")
    pg.connect("extract", "quarantine", ["failure", "unmatched"], "s11-unmatched")


S11 = Scenario(
    id="S11",
    title="Wrong JSON Path",
    symptom="misconfiguration",
    fault="EvaluateJsonPath asks for $.orderId but the records carry order_id, so nothing matches.",
    build=build_s11,
    kpis=(
        Kpi("S11", "connection", "s11-unmatched", "queued_count", above=100),
        # Decides too: the fix stops the unmatched queue growing, but the
        # quarantine drains it at 6/min, so only Publish shows it at once.
        Kpi("S11", "processor", "S11 Publish", "flowfiles_in", alert=False, good="up"),
    ),
    family="FLOW_EDIT",
    expected=(action("SET_PROCESSOR_PROPERTY", "S11 Extract Order", property="orderId", value="$.order_id"),),
    acceptable=(),
    traps=("Speeding up S11 Quarantine clears the queue and loses every order to quarantine.",),
)


def build_s12(pg):
    # 30% transient failures, retried in place, but YIELD_PROCESSOR backs
    # off the *processor*: 10 s, doubling per retry of the same FlowFile up
    # to 10 minutes, and every task waits it out. That is ~7.5 s of stall
    # per FlowFile on average, against one arriving per second.
    generator(pg, "S12 Source", period="1 sec", batch=1)
    debug_flow(pg, "service", "S12 Transient Service", successes=7, failures=3,
               period="0 sec", tasks=2, retried=("failure",), retry_count=10,
               backoff="YIELD_PROCESSOR", max_backoff="10 mins", yield_duration="10 sec",
               auto_terminate=("success",))
    pg.processor("errors", "S12 Errors", "LogAttribute", {"Log Level": "warn"},
                 period="0 sec", tasks=1, auto_terminate=("success",))
    pg.connect("gen", "service", ["success"], "s12-requests")
    pg.connect("service", "errors", ["failure"], "s12-failed")


S12 = Scenario(
    id="S12",
    title="Processor-wide Backoff",
    symptom="failure_loop",
    fault="Retries use YIELD_PROCESSOR with a 10 minute ceiling, so one failure stalls all work.",
    build=build_s12,
    kpis=(
        Kpi("S12", "connection", "s12-requests", "queued_count", above=200),
        Kpi("S12", "processor", "S12 Transient Service", "flowfiles_in", alert=False, good="up",
            primary=False),
    ),
    family="FLOW_EDIT",
    expected=(action("SET_RETRY_POLICY", "S12 Transient Service", backoff="PENALIZE_FLOWFILE"),),
    acceptable=(
        action("SET_RETRY_POLICY", "S12 Transient Service", backoff="PENALIZE_FLOWFILE",
               max_backoff="1 min"),
    ),
    traps=("Adding tasks does nothing while the processor is yielded; all tasks wait together.",),
    # The service auto-terminates its success; what reaches it is what gets through.
    edges=(("S12 Source",), ("S12 Transient Service",)),
)


PACK = Pack(
    name="p3-errors",
    title="Error handling",
    scenarios=(S09, S10, S11, S12),
)
