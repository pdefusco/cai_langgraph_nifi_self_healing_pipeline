"""
The app's own vital signs: how long its CDP and LLM calls take, how
often they fail, and how far the poller runs behind. The poller drains
these once a minute into the store's health_samples (the System tab's
App health block).

    telemetry.record("cdp", seconds, ok)
    telemetry.drain()   # {"cdp": {"calls", "errors", "p50_ms", "p95_ms"}, ...} since the last drain

Only a duration and whether the call failed are kept, never its
arguments or result, so nothing sensitive reaches the store.
"""

import math
import os
import resource
import sys
import threading
import time

_LOCK = threading.Lock()
_CALLS = {}       # kind -> [(seconds, ok)] since the last drain

# A burst between drains keeps only its newest calls.
_MAX_PER_KIND = 20000


def record(kind: str, seconds: float, ok: bool = True) -> None:
    with _LOCK:
        calls = _CALLS.setdefault(kind, [])
        calls.append((max(0.0, float(seconds)), bool(ok)))

        if len(calls) > _MAX_PER_KIND:
            del calls[:len(calls) - _MAX_PER_KIND]


def percentile(values: list, q: float) -> float | None:
    """The q-th percentile (0..100) of `values` by nearest rank, or None for none."""

    if not values:
        return None

    ordered = sorted(values)

    return ordered[min(len(ordered) - 1, max(0, math.ceil(q / 100 * len(ordered)) - 1))]


def summary(calls: list) -> dict:
    seconds = [s for s, _ in calls]
    p50, p95 = percentile(seconds, 50), percentile(seconds, 95)

    return {"calls": len(calls), "errors": sum(1 for _, ok in calls if not ok),
            "p50_ms": None if p50 is None else round(p50 * 1000), "p95_ms": None if p95 is None else round(p95 * 1000)}


def drain() -> dict:
    """Each kind's summary since the last drain, and start over."""

    with _LOCK:
        taken = {kind: calls for kind, calls in _CALLS.items()}
        _CALLS.clear()

    return {kind: summary(calls) for kind, calls in taken.items()}


# -- the process and its container -------------------------------------------

def _read(path: str) -> str:
    try:
        with open(path) as handle:
            return handle.read().strip()

    except OSError:
        return ""


def _container_cpu_seconds() -> float | None:
    """CPU used by every process in the container (the CLI workers too), from its cgroup."""

    for line in _read("/sys/fs/cgroup/cpu.stat").splitlines():          # cgroup v2
        if line.startswith("usage_usec "):
            return int(line.split()[1]) / 1e6

    usage = _read("/sys/fs/cgroup/cpuacct/cpuacct.usage")                # v1

    return int(usage) / 1e9 if usage.isdigit() else None


def _container_cpus() -> float:
    """The CPUs the container may use: its quota, else the machine's."""

    quota = _read("/sys/fs/cgroup/cpu.max").split()

    if len(quota) == 2 and quota[0].isdigit() and int(quota[1]) > 0:
        return int(quota[0]) / int(quota[1])

    cfs, period = _read("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"), _read("/sys/fs/cgroup/cpu/cpu.cfs_period_us")

    if cfs.isdigit() and period.isdigit() and int(period) > 0:
        return int(cfs) / int(period)

    return float(os.cpu_count() or 1)


def _cpu_seconds() -> float:
    container = _container_cpu_seconds()

    if container is not None:
        return container

    times = os.times()

    return times.user + times.system + times.children_user + times.children_system


_CPU = {"at": None, "used": None}


def process_stats() -> dict:
    """
    rss.mb (this process), mem.mb (the container, where its cgroup says)
    and cpu.pct: the share of the container's CPUs used since the last
    call, 100 meaning all of them; None on the first call.
    """

    out = {}
    pages = _read("/proc/self/statm").split()

    if len(pages) > 1 and pages[1].isdigit():
        out["rss.mb"] = round(int(pages[1]) * os.sysconf("SC_PAGE_SIZE") / 2**20, 1)

    else:
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss     # bytes on macOS, KiB on Linux
        out["rss.mb"] = round(peak / (2**20 if sys.platform == "darwin" else 2**10), 1)

    memory = _read("/sys/fs/cgroup/memory.current") or _read("/sys/fs/cgroup/memory/memory.usage_in_bytes")

    if memory.isdigit():
        out["mem.mb"] = round(int(memory) / 2**20, 1)

    now, used = time.monotonic(), _cpu_seconds()

    with _LOCK:
        if _CPU["at"] is not None and now > _CPU["at"]:
            out["cpu.pct"] = round(100 * (used - _CPU["used"]) / (now - _CPU["at"]) / _container_cpus(), 1)

        _CPU.update(at=now, used=used)

    return out
