"""
Pack p16-slow-burn: faults that take hours to show.

S37's trade feed runs 2.5% faster than the ledger writer can take: the
queue grows by 30 a minute and crosses its threshold nearly three hours
after deploy. S38's validator sends 1 in 50 records to a retry queue
whose processor was disabled and never enabled again: it fills over two
hours, and once full it stops the validator and everything behind it.
The queue trend warns of both well before the breach.
"""

from flows.scenarios.base import Kpi, Pack, Scenario, action
from flows.scenarios.common import debug_flow, generator, sink, stage

TRADE = '{"trade": "t-55102", "account": "acc-310", "ccy": "EUR", "amount": 1250.00}'
CLAIM = '{"claim": "c-20817", "policy": "pol-7731", "amount": 640.00, "code": "R-14"}'


def build_s37(pg):
    # 41 trades every 2 s (20.5/s) into a ledger writer with one task
    # every 50 ms (20/s): 0.5/s, 30 a minute, stays behind. The queue
    # crosses 5,000 after about 2.8 hours, with no other sign.
    generator(pg, "S37 Trade Feed", period="2 sec", batch=41, text=TRADE)
    stage(pg, "parse", "S37 Parse Trade", period="0 sec")
    stage(pg, "validate", "S37 Validate Trade", period="0 sec")
    stage(pg, "account", "S37 Enrich Account", period="0 sec")
    stage(pg, "fx", "S37 Convert Currency", period="0 sec")
    stage(pg, "ledger", "S37 Write Ledger", period="50 ms", tasks=1)
    stage(pg, "reconcile", "S37 Reconcile Position", period="0 sec")
    stage(pg, "post", "S37 Post Settlement", period="0 sec")
    stage(pg, "index", "S37 Index Trade", period="0 sec")
    sink(pg, "publish", "S37 Publish Confirmation", period="0 sec")
    pg.connect("gen", "parse", ["success"], "s37-trades")
    pg.connect("parse", "validate", ["success"], "s37-parsed")
    pg.connect("validate", "account", ["success"], "s37-valid")
    pg.connect("account", "fx", ["success"], "s37-enriched")
    pg.connect("fx", "ledger", ["success"], "s37-to-ledger")
    pg.connect("ledger", "reconcile", ["success"], "s37-written")
    pg.connect("reconcile", "post", ["success"], "s37-reconciled")
    pg.connect("post", "index", ["success"], "s37-posted")
    pg.connect("index", "publish", ["success"], "s37-indexed")


S37 = Scenario(
    id="S37",
    title="Producer Just Over Capacity",
    symptom="backlog",
    fault=("The trade feed sends 20.5/s and the ledger writer does 20/s: the queue in front of it "
           "grows by 30 a minute and breaches its threshold hours after deploy."),
    build=build_s37,
    kpis=(
        Kpi("S37", "connection", "s37-to-ledger", "queued_count", above=5000),
        Kpi("S37", "processor", "S37 Write Ledger", "flowfiles_out", alert=False, good="up", primary=False),
    ),
    family="FLOW_EDIT",
    kind="slow_burn",
    detect_within=14400,
    expected=(action("SET_CONCURRENT_TASKS", "S37 Write Ledger", value=2),),
    acceptable=(action("SET_RUN_SCHEDULE", "S37 Write Ledger", value="0 sec"),),
    traps=(
        "The writer is busy, not stalled, and almost keeps up: its FlowFiles Out looks normal.",
        "A higher threshold moves the breach a few hours out; the queue never stops growing.",
    ),
    wrong=(
        action("SET_BACKPRESSURE", "s37-to-ledger", object_threshold=20000),
        action("SET_CONCURRENT_TASKS", "S37 Convert Currency", value=4),
    ),
)


def build_s38(pg):
    # 10/s of claims; the validator fails 1 in 50 (0.2/s) to a retry
    # queue whose processor is disabled. The queue crosses 1,500 after
    # about 2 hours and is full (2,000) 40 minutes later: the validator
    # is back-pressured, and claims back up behind it.
    generator(pg, "S38 Claim Intake", period="1 sec", batch=10, text=CLAIM)
    stage(pg, "parse", "S38 Parse Claim", period="0 sec")
    stage(pg, "policy", "S38 Lookup Policy", period="0 sec")
    debug_flow(pg, "validate", "S38 Validate Claim", successes=49, failures=1, period="0 sec", tasks=1)
    stage(pg, "retry", "S38 Retry Claim", period="1 sec", scheduled_state="DISABLED")
    stage(pg, "score", "S38 Score Fraud", period="0 sec")
    stage(pg, "route", "S38 Route Claim", period="0 sec")
    stage(pg, "reserve", "S38 Set Reserve", period="0 sec")
    stage(pg, "notify", "S38 Notify Adjuster", period="0 sec")
    sink(pg, "store", "S38 Store Claim", period="0 sec")
    pg.connect("gen", "parse", ["success"], "s38-intake")
    pg.connect("parse", "policy", ["success"], "s38-parsed")
    pg.connect("policy", "validate", ["success"], "s38-to-validate")
    pg.connect("validate", "retry", ["failure"], "s38-retry", object_threshold=2000)
    pg.connect("retry", "score", ["success"], "s38-retried")
    pg.connect("validate", "score", ["success"], "s38-valid")
    pg.connect("score", "route", ["success"], "s38-scored")
    pg.connect("route", "reserve", ["success"], "s38-routed")
    pg.connect("reserve", "notify", ["success"], "s38-reserved")
    pg.connect("notify", "store", ["success"], "s38-notified")


S38 = Scenario(
    id="S38",
    title="Forgotten Retry Queue",
    symptom="backlog",
    fault=("S38 Retry Claim is disabled, so the 1 in 50 claims that fail validation pile up in its "
           "queue: it fills over two hours, then back-pressures the validator and the whole flow."),
    build=build_s38,
    kpis=(
        Kpi("S38", "connection", "s38-retry", "queued_count", above=1500),
        Kpi("S38", "connection", "s38-to-validate", "queued_count", above=8000, primary=False),
        Kpi("S38", "processor", "S38 Store Claim", "flowfiles_in", alert=False, good="up", primary=False),
    ),
    family="FLOW_EDIT",
    kind="slow_burn",
    detect_within=10800,
    expected=(action("SET_SCHEDULED_STATE", "S38 Retry Claim", value="ENABLED"),),
    acceptable=(),
    traps=(
        "The validator is healthy: 49 in 50 pass, and it stops only when the retry queue is full.",
        "A higher threshold on the retry queue only postpones the stall; nothing ever drains it.",
    ),
    wrong=(
        action("SET_BACKPRESSURE", "s38-retry", object_threshold=20000),
        action("SET_CONCURRENT_TASKS", "S38 Validate Claim", value=4),
    ),
)


PACK = Pack(
    name="p16-slow-burn",
    title="Slow-burn faults",
    scenarios=(S37, S38),
    wave=2,
)
