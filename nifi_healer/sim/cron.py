"""
CRON-driven scheduling for the simulator: which seconds an expression
fires on, and how many firings fall within a step.

    firings("* 0-14 */2 * * ?", t - DT, t)    # bursts: every second, minutes 0-14 of every 2nd hour

Six fields: second, minute, hour, day of month, month, day of week. Only
the subset that Quartz and Spring's CronExpression read alike is
accepted, so an expression means the same whichever NiFi evaluates it:
`*`, `?`, lists, ranges and steps; month and weekday names; at most one
of the two day fields restricted. A weekday number is refused, because
the two number the week differently (Quartz 1 = SUN, Spring 1 = MON).
Times are UTC, as on a CDF NiFi node.
"""

import functools
import math
import time
from dataclasses import dataclass

MONTHS = {name: n for n, name in enumerate("JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split(), 1)}
WEEKDAYS = {name: n for n, name in enumerate("MON TUE WED THU FRI SAT SUN".split())}    # as tm_wday

ANY = ("*", "?")


@dataclass(frozen=True)
class Cron:
    seconds: frozenset
    minutes: frozenset
    hours: frozenset
    days: frozenset | None          # None: any day of the month
    months: frozenset
    weekdays: frozenset | None      # None: any day of the week

    def matches(self, t: int) -> bool:
        if t % 60 not in self.seconds:
            return False

        tm = time.gmtime(t)

        return (tm.tm_min in self.minutes and tm.tm_hour in self.hours and tm.tm_mon in self.months
                and (self.days is None or tm.tm_mday in self.days)
                and (self.weekdays is None or tm.tm_wday in self.weekdays))


def _value(token: str, names: dict | None, label: str) -> int:
    if names and token.upper() in names:
        return names[token.upper()]

    if names is WEEKDAYS:
        raise ValueError(f"day of week {token!r}: use a name (MON-SUN); Quartz and Spring number the week "
                         f"differently")

    if not token.isdigit():
        raise ValueError(f"{label} {token!r} is not a number")

    return int(token)


def _field(text: str, low: int, high: int, label: str, names: dict | None = None) -> frozenset:
    values = set()

    for part in text.split(","):
        base, slash, step = part.partition("/")

        if base in ANY:
            start, end = low, high

        elif "-" in base:
            first, last = base.split("-", 1)
            start, end = _value(first, names, label), _value(last, names, label)

        else:
            start = _value(base, names, label)
            end = high if slash else start

        if slash and (not step.isdigit() or int(step) == 0):
            raise ValueError(f"{label} step {step!r} is not a positive number")

        if not low <= start <= end <= high:
            raise ValueError(f"{label} {part!r} is outside {low}-{high}")

        values.update(range(start, end + 1, int(step) if slash else 1))

    return frozenset(values)


@functools.lru_cache(maxsize=256)
def parse(expression: str) -> Cron:
    """A Cron, or ValueError saying what is wrong with the expression."""

    fields = str(expression or "").split()

    if len(fields) != 6:
        raise ValueError(f"{expression!r}: a CRON expression has six fields (second minute hour "
                         f"day-of-month month day-of-week), this has {len(fields)}")

    second, minute, hour, day, month, weekday = fields

    for label, text in (("second", second), ("minute", minute), ("hour", hour), ("month", month)):
        if "?" in text:
            raise ValueError(f"{label}: ? is only for day of month or day of week")

    if day not in ANY and weekday not in ANY:
        raise ValueError("restrict day of month or day of week, not both: make one of them ?")

    return Cron(
        seconds=_field(second, 0, 59, "second"),
        minutes=_field(minute, 0, 59, "minute"),
        hours=_field(hour, 0, 23, "hour"),
        days=None if day in ANY else _field(day, 1, 31, "day of month"),
        months=_field(month, 1, 12, "month", MONTHS),
        weekdays=None if weekday in ANY else _field(weekday, 0, 6, "day of week", WEEKDAYS),
    )


def firings(expression: str, start: float, end: float) -> int:
    """How many times the expression fires in (start, end], epoch seconds."""

    cron = parse(expression)

    return sum(1 for t in range(math.floor(start) + 1, math.floor(end) + 1) if cron.matches(t))
