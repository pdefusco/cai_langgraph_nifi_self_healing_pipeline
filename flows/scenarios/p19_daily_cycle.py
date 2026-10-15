"""
Pack p19-daily-cycle: controls whose load follows the clock.

S42's meter readings rise through the business day and peak at lunch,
twelve times the night's rate, and fall again in the evening. S43's
bank file arrives once a night: an hour of work, then 23 hours with
nothing flowing at all. A falling or zero rate in either is the
schedule, not a fault.

Cron expressions are Quartz, in UTC.
"""

from flows.scenarios.base import Kpi, Pack, Scenario, action
from flows.scenarios.common import generator, sink, stage

READING = '{"meter": "m-30418", "site": "plant-3", "kwh": 4.71, "ts": 1760000000}'
STATEMENT = '{"account": "acct-5520", "entries": 1, "balance": 18234.10, "date": "2026-10-14"}'


def build_s42(pg):
    # Meters report at 1/s around the clock, 7/s from 08:00 to 19:59 UTC,
    # and 12/s through the lunch hour (12:00-12:59), into a loader with
    # one task every 50 ms (20/s): its queue stays near empty all day.
    generator(pg, "S42 Meter Readings", period="1 sec", batch=1, text=READING)
    generator(pg, "S42 Business Hours", period="* * 8-19 * * ?", batch=6, text=READING, key="day",
              strategy="CRON_DRIVEN")
    generator(pg, "S42 Lunch Peak", period="* * 12 * * ?", batch=5, text=READING, key="lunch",
              strategy="CRON_DRIVEN")
    stage(pg, "parse", "S42 Parse Reading", period="0 sec")
    stage(pg, "convert", "S42 Convert Units", period="0 sec")
    stage(pg, "enrich", "S42 Enrich Site", period="0 sec")
    stage(pg, "aggregate", "S42 Aggregate Interval", period="0 sec")
    stage(pg, "load", "S42 Load Readings", period="50 ms", tasks=1)
    stage(pg, "index", "S42 Index Readings", period="0 sec")
    sink(pg, "publish", "S42 Publish Usage", period="0 sec")
    pg.connect("gen", "parse", ["success"], "s42-meters")
    pg.connect("day", "parse", ["success"], "s42-day")
    pg.connect("lunch", "parse", ["success"], "s42-lunch")
    pg.connect("parse", "convert", ["success"], "s42-parsed")
    pg.connect("convert", "enrich", ["success"], "s42-converted")
    pg.connect("enrich", "aggregate", ["success"], "s42-enriched")
    pg.connect("aggregate", "load", ["success"], "s42-to-load")
    pg.connect("load", "index", ["success"], "s42-loaded")
    pg.connect("index", "publish", ["success"], "s42-indexed")


S42 = Scenario(
    id="S42",
    title="Daily Cycle Load",
    symptom="healthy",
    fault=("None. The load rises from 1/s at night to 12/s at lunch and falls again in the evening, "
           "well within the loader's 20/s."),
    build=build_s42,
    kpis=(
        Kpi("S42", "connection", "s42-to-load", "queued_count", above=2000),
        Kpi("S42", "processor", "S42 Load Readings", "flowfiles_in", alert=False, good="up", primary=False),
        Kpi("S42", "processor", "S42 Load Readings", "flowfiles_out", alert=False, good="up", primary=False),
    ),
    family="NONE",
    detect_within=86400,
    expected=(action("NO_ACTION"),),
    acceptable=(),
    traps=(
        "The rate falls sevenfold at 20:00 UTC: the business day ends, nothing upstream broke.",
        "The lunch peak is the busiest hour of the day, and the loader keeps up with it.",
    ),
)


def build_s43(pg):
    # The bank file arrives from 01:00 to 01:59 UTC: 20 statements every
    # 5 s (4/s), into a ledger poster with one task every 100 ms (10/s).
    # The other 23 hours nothing flows at all.
    generator(pg, "S43 Bank File Drop", period="*/5 * 1 * * ?", batch=20, text=STATEMENT,
              strategy="CRON_DRIVEN")
    stage(pg, "decrypt", "S43 Decrypt File", period="0 sec")
    stage(pg, "parse", "S43 Parse Statement", period="0 sec")
    stage(pg, "check", "S43 Check Totals", period="0 sec")
    stage(pg, "match", "S43 Match Payments", period="0 sec")
    stage(pg, "reconcile", "S43 Reconcile Account", period="0 sec")
    stage(pg, "post", "S43 Post Ledger", period="100 ms", tasks=1)
    stage(pg, "notify", "S43 Notify Treasury", period="0 sec")
    stage(pg, "index", "S43 Index Entries", period="0 sec")
    sink(pg, "archive", "S43 Archive Statement", period="0 sec")
    pg.connect("gen", "decrypt", ["success"], "s43-file")
    pg.connect("decrypt", "parse", ["success"], "s43-decrypted")
    pg.connect("parse", "check", ["success"], "s43-parsed")
    pg.connect("check", "match", ["success"], "s43-checked")
    pg.connect("match", "reconcile", ["success"], "s43-matched")
    pg.connect("reconcile", "post", ["success"], "s43-to-post")
    pg.connect("post", "notify", ["success"], "s43-posted")
    pg.connect("notify", "index", ["success"], "s43-notified")
    pg.connect("index", "archive", ["success"], "s43-indexed")


S43 = Scenario(
    id="S43",
    title="Long Idle Periods",
    symptom="healthy",
    fault=("None. The nightly bank file takes an hour from 01:00 UTC; the other 23 hours nothing "
           "flows, as scheduled."),
    build=build_s43,
    kpis=(
        Kpi("S43", "connection", "s43-to-post", "queued_count", above=2000),
        Kpi("S43", "processor", "S43 Post Ledger", "flowfiles_in", alert=False, good="up", primary=False),
        Kpi("S43", "processor", "S43 Post Ledger", "flowfiles_out", alert=False, good="up", primary=False),
    ),
    family="NONE",
    detect_within=86400,
    expected=(action("NO_ACTION"),),
    acceptable=(),
    traps=(
        "A rate of zero for 23 hours is the schedule: the source is a nightly file, not a stalled feed.",
        "Starting or speeding up the source would only post statements that do not exist yet.",
    ),
)


PACK = Pack(
    name="p19-daily-cycle",
    title="Controls: load that follows the clock",
    scenarios=(S42, S43),
    wave=2,
)
