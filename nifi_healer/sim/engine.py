"""
A mean-field NiFi simulator: queues, schedules, back pressure and CPU.

Not NiFi. It moves *amounts* of FlowFiles (fractional counts are fine)
through the compiled flow in fixed steps, and models exactly the
mechanisms the scenarios inject faults through:

  * a timer-driven processor runs each task once per run schedule, one
    FlowFile per run, or as fast as its per-FlowFile latency allows on
    a 0 sec schedule
  * a processor is not scheduled while any outgoing connection is at
    its back-pressure threshold; a run already started may overshoot
  * FlowFile expiration drops what has waited too long
  * DebugFlow fails a fixed fraction; retried relationships cost
    attempts (PENALIZE_FLOWFILE) or stall the processor (YIELD_PROCESSOR)
  * InvokeHTTP to an address nothing answers waits out its connect
    timeout; to a ListenHTTP in the same flow it succeeds
  * MergeContent bins until Minimum Number of Entries or Max Bin Age
  * CompressContent and hashing are CPU-bound, and share the node's
    cores; sources on executionNode ALL run on every node, and only a
    load-balanced connection spreads work after a PRIMARY one

Every step records one sample per (kind, component name, metric), and
`chart` answers a window of them as DataFlow does: ~25 buckets anchored
to the moment of the request.
"""

import math
import urllib.parse
from collections import deque

from nifi_healer.sim.model import (compile_flow, dynamic_properties, json_path_lookup,
                                   parse_duration, parse_json, parse_size)

DT = 15.0                      # simulated seconds per step

RAW_KEEP = 2 * 3600            # raw samples kept in memory
RAW_PERSIST = 3600             # raw samples written to disk
COARSE = 300                   # coarse bucket width
COARSE_KEEP = 24 * 3600

PERIODS = {
    "LAST_THIRTY_MINUTES": 1800,
    "LAST_ONE_HOUR": 3600,
    "LAST_TWELVE_HOURS": 12 * 3600,
    "LAST_ONE_DAY": 24 * 3600,
}
BUCKETS = 25
ALERT_RESOLUTION = 60.0        # alerts are judged on means over this many seconds

CORES = {"EXTRA_SMALL": 2, "SMALL": 3, "MEDIUM": 6, "LARGE": 12}
BASELINE_CORES = 0.15          # NiFi itself, per node
CHEAP_LATENCY = 0.002          # seconds per FlowFile for attribute-only work
CHEAP_CPU = 0.0005             # core-seconds per FlowFile for the same
LOCAL_HTTP_LATENCY = 0.05
GENERATE_BYTES_PER_CORE = 300e6
HASH_BYTES_PER_CORE = 200e6
LOCAL_HOSTS = {"localhost", "127.0.0.1", "0.0.0.0"}


def compress_rate(level: int) -> float:
    """gzip bytes per second per core, by compression level."""

    if level >= 7:
        return 8e6

    if level >= 4:
        return 25e6

    return 60e6


class Simulation:
    def __init__(self, definition: dict, parameters: dict, cluster_size: str, nodes: int,
                 start: float):
        self.t = float(start)
        self.queues = {}            # connection id -> deque([t, count, bytes])
        self.state = {}             # processor id -> dict
        self.cpu_scale = 1.0
        self.pause_until = 0.0
        self.expired = {}           # connection name -> FlowFiles expired, total
        self.times = deque()
        self.series = {}            # key -> deque of values aligned with times
        self.coarse_times = deque()
        self.coarse = {}            # key -> deque of 5-min means
        self.configure(definition, parameters, cluster_size, nodes)

    # -----------------------------------------------------------------
    # Configuration
    # -----------------------------------------------------------------

    def configure(self, definition: dict, parameters: dict, cluster_size: str, nodes: int) -> None:
        """Load a (new) flow version, parameters or sizing; queues are kept by id."""

        self.definition = definition
        self.parameters = dict(parameters or {})
        self.cluster_size = cluster_size
        self.nodes = max(1, int(nodes))
        self.model = compile_flow(definition, self.parameters)
        self.queues = {cid: self.queues.get(cid, deque()) for cid in self.model.connections}
        self.state = {pid: self.state.get(pid, {"acc": 1.0}) for pid in self.model.processors}
        self._spread = {}

    @property
    def cores_per_node(self) -> int:
        return CORES.get(self.cluster_size, 2)

    # -----------------------------------------------------------------
    # Queues
    # -----------------------------------------------------------------

    def queued(self, cid: str) -> tuple:
        queue = self.queues.get(cid) or ()

        return sum(c[1] for c in queue), sum(c[2] for c in queue)

    def _full(self, cid: str) -> bool:
        conn = self.model.connections[cid]
        count, size = self.queued(cid)

        return count >= conn.object_threshold or size >= conn.size_threshold

    def _room(self, cid: str, per_flowfile_bytes: float) -> float:
        conn = self.model.connections[cid]
        count, size = self.queued(cid)
        room = conn.object_threshold - count

        if per_flowfile_bytes > 0 and conn.size_threshold != float("inf"):
            room = min(room, (conn.size_threshold - size) / per_flowfile_bytes)

        return max(0.0, room)

    def _put(self, cid: str, count: float, size: float) -> None:
        if count > 1e-9:
            self.queues[cid].append([self.t, count, size])

    def _ready(self, cid: str) -> tuple:
        """
        What a processor may take this step: FlowFiles queued before it
        began. Without this a consumer would empty, in the same step, what
        its producer just queued, and no queue would ever show a burst.
        """

        cohorts = [c for c in self.queues.get(cid) or () if c[0] < self.t]

        return sum(c[1] for c in cohorts), sum(c[2] for c in cohorts)

    def _take(self, cid: str, wanted: float) -> tuple:
        queue = self.queues[cid]
        count = size = 0.0

        while queue and wanted - count > 1e-9 and queue[0][0] < self.t:
            cohort = queue[0]
            part = min(cohort[1], wanted - count)
            share = cohort[2] * part / cohort[1] if cohort[1] else 0.0
            count += part
            size += share
            cohort[1] -= part
            cohort[2] -= share

            if cohort[1] <= 1e-9:
                queue.popleft()

        return count, size

    def _available(self, proc) -> tuple:
        totals = [self._ready(cid) for cid in proc.inputs]

        return sum(c for c, _ in totals), sum(b for _, b in totals)

    def _take_inputs(self, proc, wanted: float) -> tuple:
        """Round-robin across input connections: water-fill, then FIFO within each."""

        available = {cid: self._ready(cid)[0] for cid in proc.inputs}
        grant = {cid: 0.0 for cid in available}
        left = wanted
        open_ = [cid for cid, n in available.items() if n > 1e-9]

        while left > 1e-9 and open_:
            share = left / len(open_)

            for cid in list(open_):
                give = min(share, available[cid] - grant[cid])
                grant[cid] += give
                left -= give

                if available[cid] - grant[cid] <= 1e-9:
                    open_.remove(cid)

        count = size = 0.0

        for cid, n in grant.items():
            c, b = self._take(cid, n)
            count += c
            size += b

        return count, size

    def _expire(self) -> None:
        for cid, conn in self.model.connections.items():
            if conn.expiration <= 0:
                continue

            queue = self.queues[cid]
            dropped = 0.0

            while queue and self.t - queue[0][0] > conn.expiration:
                dropped += queue.popleft()[1]

            if dropped:
                self.expired[conn.name] = self.expired.get(conn.name, 0.0) + dropped

    # -----------------------------------------------------------------
    # Topology
    # -----------------------------------------------------------------

    def spread(self, pid: str, seen=None) -> int:
        """How many nodes a processor's work is spread across."""

        if pid in self._spread:
            return self._spread[pid]

        proc = self.model.processors[pid]
        seen = (seen or set()) | {pid}

        if proc.is_source:
            value = self.nodes if proc.execution_node == "ALL" else 1

        else:
            value = 1

            for cid in proc.inputs:
                conn = self.model.connections[cid]

                if conn.load_balanced:
                    value = max(value, self.nodes)

                elif conn.source in self.model.processors and conn.source not in seen:
                    value = max(value, self.spread(conn.source, seen))

        self._spread[pid] = value

        return value

    def _outgoing(self, proc) -> list:
        return sorted({cid for cids in proc.outputs.values() for cid in cids})

    def _route(self, proc, routed: dict, out: dict) -> None:
        """Deliver {relationship: (count, bytes)}; clones into every connection."""

        for rel, (count, size) in routed.items():
            if count <= 1e-9:
                continue

            targets = proc.outputs.get(rel) or []

            if not targets:
                continue           # auto-terminated: leaves the flow, not counted as out

            for cid in targets:
                self._put(cid, count, size)
                out["count"] += count
                out["bytes"] += size

    # -----------------------------------------------------------------
    # Stepping
    # -----------------------------------------------------------------

    def advance(self, until: float) -> None:
        while self.t + DT <= until + 1e-6:
            self.step()

    def step(self) -> None:
        self.t += DT
        self._spread = {}
        self._expire()
        stats = {pid: {"in": 0.0, "count": 0.0, "bytes": 0.0} for pid in self.model.processors}
        cpu = {"fixed": BASELINE_CORES * self.nodes, "bound_used": 0.0, "bound_demand": 0.0}
        paused = self.t < self.pause_until

        if not paused:
            for pid, proc in self.model.processors.items():
                if proc.runs:
                    self._run(proc, stats[pid], cpu)

        cores = self.cores_per_node * self.nodes
        spare = cores - cpu["fixed"]
        self.cpu_scale = (max(0.05, min(1.0, spare / cpu["bound_demand"]))
                          if cpu["bound_demand"] > 1e-9 else 1.0)
        used = cpu["fixed"] + cpu["bound_used"]
        self._record(stats, min(100.0, used / cores * 100.0))

    def _schedule_limit(self, proc, spread: int, latency: float) -> float:
        """FlowFiles the schedule allows this step, ignoring input and CPU."""

        return proc.tasks * spread * DT / max(proc.period, latency, 1e-6)

    def _blocked(self, proc) -> bool:
        return any(self._full(cid) for cid in self._outgoing(proc))

    def _out_room(self, proc, per_flowfile_bytes: float, multiplier: float = 1.0) -> float:
        rooms = [self._room(cid, per_flowfile_bytes) for cid in self._outgoing(proc)]

        if not rooms:
            return float("inf")

        # One run in flight may overshoot the threshold, as in NiFi.
        return min(rooms) / max(multiplier, 1e-9) + 1.0

    def _run(self, proc, stat: dict, cpu: dict) -> None:
        spread = self.spread(proc.id)
        state = self.state[proc.id]
        kind = proc.kind

        if kind == "GenerateFlowFile":
            self._run_generator(proc, spread, state, stat, cpu)
            return

        if kind == "ListenHTTP":
            inbox = state.pop("inbox", [0.0, 0.0])

            if inbox[0] > 0 and not self._blocked(proc):
                self._route(proc, {"success": (inbox[0], inbox[1])}, stat)

            elif inbox[0] > 0:
                state["inbox"] = inbox      # held until there is room

            return

        if self._blocked(proc):
            return

        count_avail, bytes_avail = self._available(proc)

        if count_avail <= 1e-9 and kind != "MergeContent":
            return

        avg = bytes_avail / count_avail if count_avail > 1e-9 else 0.0
        handler = getattr(self, f"_run_{kind}", self._run_passthrough)
        handler(proc, spread, state, stat, cpu, count_avail, avg)

    # -- per kind ------------------------------------------------------

    def _run_generator(self, proc, spread, state, stat, cpu) -> None:
        batch = float(proc.properties.get("Batch Size") or 1)
        size = parse_size(proc.properties.get("File Size"), 0.0)
        binary = proc.properties.get("Data Format") == "Binary"
        text = proc.properties.get("generate-ff-custom-text")

        if text is not None:
            size = float(len(text.encode()))

        if self._blocked(proc):
            # Not scheduled while back-pressured; fires as soon as it is not.
            state["acc"] = min(1.0, state.get("acc", 1.0) + spread * DT / max(proc.period, 1e-6))
            return

        if proc.period <= 0:
            runs = proc.tasks * spread * DT / CHEAP_LATENCY
            state["acc"] = 0.0

        else:
            acc = state.get("acc", 1.0) + 0.0
            runs = math.floor(acc + 1e-9)
            state["acc"] = acc - runs + spread * DT / proc.period

        if runs <= 0:
            return

        produced = min(runs * batch, self._out_room(proc, size) + batch - 1.0)
        produced = max(0.0, produced)

        if binary:
            cpu["fixed"] += produced * size / GENERATE_BYTES_PER_CORE / DT

        cpu["fixed"] += produced * CHEAP_CPU / DT
        self._route(proc, {"success": (produced, produced * size)}, stat)

    def _run_passthrough(self, proc, spread, state, stat, cpu, avail, avg) -> None:
        """UpdateAttribute, LogAttribute and anything else that only touches attributes."""

        n = min(avail, self._schedule_limit(proc, spread, CHEAP_LATENCY), self._out_room(proc, avg))
        count, size = self._take_inputs(proc, n)
        stat["in"] += count
        cpu["fixed"] += count * CHEAP_CPU / DT
        self._route(proc, {"success": (count, size)}, stat)

    def _run_DebugFlow(self, proc, spread, state, stat, cpu, avail, avg) -> None:
        successes = float(proc.properties.get("FlowFile Success Iterations") or 1)
        failures = float(proc.properties.get("FlowFile Failure Iterations") or 0)
        p = failures / (successes + failures) if successes + failures else 0.0
        capacity = self._schedule_limit(proc, spread, CHEAP_LATENCY)
        r = proc.retry_count
        retried = "failure" in proc.retried and r > 0

        if retried and proc.backoff == "YIELD_PROCESSOR":
            # Each retry yields the whole processor: every task waits.
            per_node = capacity / spread / DT
            stall = sum(p ** (k + 1) * min(proc.max_backoff, proc.yield_seconds * 2 ** k)
                        for k in range(r))
            capacity = spread * DT / (1.0 / per_node + stall)

        elif retried:
            attempts = (1 - p ** (r + 1)) / (1 - p) if p < 1 else r + 1
            capacity /= attempts

        n = min(avail, capacity, self._out_room(proc, avg))
        count, size = self._take_inputs(proc, n)
        stat["in"] += count
        cpu["fixed"] += count * CHEAP_CPU / DT
        fail = p ** (r + 1) if retried else p
        self._route(proc, {
            "success": (count * (1 - fail), size * (1 - fail)),
            "failure": (count * fail, size * fail),
        }, stat)

    def _listener_for(self, url: str):
        parsed = urllib.parse.urlparse(url)

        if (parsed.hostname or "") not in LOCAL_HOSTS:
            return None, False

        for other in self.model.processors.values():
            if other.kind != "ListenHTTP" or not other.runs:
                continue

            port = str(other.properties.get("Listening Port") or "")
            base = (other.properties.get("Base Path") or "contentListener").strip("/")

            if str(parsed.port or "") == port and parsed.path.strip("/") == base:
                return other, True

        return None, True

    def _run_InvokeHTTP(self, proc, spread, state, stat, cpu, avail, avg) -> None:
        url = (proc.properties.get("HTTP URL") or "").strip()
        listener, local = self._listener_for(url)

        if listener is not None:
            latency = LOCAL_HTTP_LATENCY

        elif local:
            latency = CHEAP_LATENCY         # connection refused, at once

        else:
            latency = parse_duration(proc.properties.get("Connection Timeout"), 5.0)

        n = min(avail, self._schedule_limit(proc, spread, latency), self._out_room(proc, avg))
        count, size = self._take_inputs(proc, n)
        stat["in"] += count
        cpu["fixed"] += count * CHEAP_CPU / DT

        if listener is not None:
            inbox = self.state[listener.id].setdefault("inbox", [0.0, 0.0])
            inbox[0] += count
            inbox[1] += size
            self._route(proc, {"Original": (count, size), "Response": (count, count * 64.0)}, stat)

        else:
            self._route(proc, {"Failure": (count, size)}, stat)

    def _source_text(self, proc):
        for other in self.model.processors.values():
            if other.group == proc.group and other.kind == "GenerateFlowFile":
                text = other.properties.get("generate-ff-custom-text")

                if text is not None:
                    return text

        return None

    def _run_EvaluateJsonPath(self, proc, spread, state, stat, cpu, avail, avg) -> None:
        paths = list(dynamic_properties(proc).values())
        ok, document = parse_json(self._source_text(proc))

        if not ok:
            rel = "failure"

        else:
            found = paths and all(json_path_lookup(document, path)[0] for path in paths)
            rel = "matched" if found else "unmatched"

        n = min(avail, self._schedule_limit(proc, spread, CHEAP_LATENCY), self._out_room(proc, avg))
        count, size = self._take_inputs(proc, n)
        stat["in"] += count
        cpu["fixed"] += count * CHEAP_CPU / DT
        self._route(proc, {rel: (count, size)}, stat)

    def _run_DuplicateFlowFile(self, proc, spread, state, stat, cpu, avail, avg) -> None:
        try:
            copies = max(0, int(float(proc.properties.get("Number of Copies") or 100)))

        except ValueError:
            copies = 100

        multiplier = copies + 1
        n = min(avail, self._schedule_limit(proc, spread, CHEAP_LATENCY),
                self._out_room(proc, avg, multiplier))
        count, size = self._take_inputs(proc, n)
        stat["in"] += count
        cpu["fixed"] += count * multiplier * CHEAP_CPU / DT
        self._route(proc, {"success": (count * multiplier, size * multiplier)}, stat)

    def _run_MergeContent(self, proc, spread, state, stat, cpu, avail, avg) -> None:
        minimum = max(1.0, float(proc.properties.get("Minimum Number of Entries") or 1))
        maximum = max(minimum, float(proc.properties.get("Maximum Number of Entries") or 1000))
        bins = max(1.0, float(proc.properties.get("Maximum number of Bins") or 5))
        max_age = parse_duration(proc.properties.get("Max Bin Age"), 0.0)
        held = state.setdefault("bin", [0.0, 0.0, None])

        if avail > 1e-9:
            count, size = self._take_inputs(proc, avail)
            stat["in"] += count
            cpu["fixed"] += count * CHEAP_CPU / DT

            if held[0] <= 1e-9:
                held[2] = self.t

            held[0] += count
            held[1] += size

        merged = consumed = 0.0

        batches = math.floor(held[0] / minimum + 1e-9)

        if batches:
            merged += batches
            consumed += batches * minimum

        rest = held[0] - consumed
        aged = max_age > 0 and held[2] is not None and self.t - held[2] >= max_age
        evicted = rest > bins * maximum

        if rest > 1e-9 and (aged or evicted):
            merged += 1
            consumed = held[0]

        if consumed <= 1e-9:
            return

        size = held[1] * consumed / held[0]
        held[0] -= consumed
        held[1] -= size
        held[2] = self.t if held[0] > 1e-9 else None
        self._route(proc, {"merged": (merged, size), "original": (consumed, size)}, stat)

    def _cpu_bound(self, proc, spread, stat, cpu, avail, avg, rate) -> tuple:
        """Take what the shared cores allow at `rate` bytes/s per core."""

        cores = min(proc.tasks, self.cores_per_node) * spread
        demand = min(cores, avail * avg / (rate * DT)) if avg > 0 else 0.0
        cpu["bound_demand"] += demand

        if avg <= 0:
            n = min(avail, self._schedule_limit(proc, spread, CHEAP_LATENCY))

        else:
            n = min(avail, cores * self.cpu_scale * rate * DT / avg, self._out_room(proc, avg))

        count, size = self._take_inputs(proc, n)
        stat["in"] += count
        cpu["bound_used"] += size / rate / DT

        return count, size

    def _run_CompressContent(self, proc, spread, state, stat, cpu, avail, avg) -> None:
        try:
            level = int(proc.properties.get("Compression Level") or 1)

        except ValueError:
            level = 1

        count, size = self._cpu_bound(proc, spread, stat, cpu, avail, avg, compress_rate(level))
        # Random bytes do not compress.
        self._route(proc, {"success": (count, size)}, stat)

    def _run_CryptographicHashContent(self, proc, spread, state, stat, cpu, avail, avg) -> None:
        count, size = self._cpu_bound(proc, spread, stat, cpu, avail, avg, HASH_BYTES_PER_CORE)
        self._route(proc, {"success": (count, size)}, stat)

    # -----------------------------------------------------------------
    # Samples
    # -----------------------------------------------------------------

    def _record(self, stats: dict, cpu_percent: float) -> None:
        values = {("system", "", "cpu"): cpu_percent}

        for cid, conn in self.model.connections.items():
            if not conn.name:
                continue

            count, size = self.queued(cid)
            full = max(count / conn.object_threshold if conn.object_threshold != float("inf") else 0.0,
                       size / conn.size_threshold if conn.size_threshold != float("inf") else 0.0)
            values[("connection", conn.name, "queued_count")] = count
            values[("connection", conn.name, "queued_bytes")] = size
            values[("connection", conn.name, "percent_full")] = full * 100.0

        for pid, proc in self.model.processors.items():
            stat = stats[pid]
            values[("processor", proc.name, "flowfiles_in")] = stat["in"] / DT
            values[("processor", proc.name, "flowfiles_out")] = stat["count"] / DT
            values[("processor", proc.name, "bytes_out")] = stat["bytes"]

        self.times.append(self.t)

        for key, value in values.items():
            key = "|".join(key)

            if key not in self.series:
                # A component that appeared in a new version: pad its past.
                self.series[key] = deque([None] * (len(self.times) - 1))

            self.series[key].append(value)

        for key, column in self.series.items():
            if len(column) < len(self.times):
                column.append(None)          # a component that no longer exists

        self._roll()

    def _roll(self) -> None:
        """Fold samples older than RAW_KEEP into 5-minute means."""

        while self.times and self.t - self.times[0] > RAW_KEEP:
            start = self.times[0] - self.times[0] % COARSE
            count = 0

            while count < len(self.times) and self.times[count] < start + COARSE:
                count += 1

            if count == len(self.times):
                break

            self.coarse_times.append(start)

            for key, column in self.series.items():
                chunk = [column.popleft() for _ in range(count)]
                chunk = [v for v in chunk if v is not None]
                self.coarse.setdefault(key, deque([None] * (len(self.coarse_times) - 1)))
                self.coarse[key].append(sum(chunk) / len(chunk) if chunk else None)

            for key, column in self.coarse.items():
                if len(column) < len(self.coarse_times):
                    column.append(None)

            for _ in range(count):
                self.times.popleft()

        while self.coarse_times and self.t - self.coarse_times[0] > COARSE_KEEP:
            self.coarse_times.popleft()

            for column in self.coarse.values():
                column.popleft()

    def samples(self, key: str, since: float) -> list:
        """[(t, value)] from `since`, coarse means first where raw is gone."""

        out = []
        raw_start = self.times[0] if self.times else self.t

        for t, value in zip(self.coarse_times, self.coarse.get(key, ())):
            if t >= since and t < raw_start and value is not None:
                out.append((t, value))

        for t, value in zip(self.times, self.series.get(key, ())):
            if t >= since and value is not None:
                out.append((t, value))

        return out

    def chart(self, key: str, period: str, now: float) -> list:
        """[{timestamp ms, value}] in BUCKETS buckets anchored to `now`."""

        window = PERIODS.get(period, 1800)
        width = window / BUCKETS
        start = now - window
        sums = [[0.0, 0] for _ in range(BUCKETS)]

        for t, value in self.samples(key, start):
            index = int((t - start) // width)

            if 0 <= index < BUCKETS:
                sums[index][0] += value
                sums[index][1] += 1

        return [
            {"timestamp": int((start + i * width) * 1000), "value": total / n}
            for i, (total, n) in enumerate(sums)
            if n
        ]

    def latest(self, key: str):
        column = self.series.get(key)

        return column[-1] if column else None

    def breach_since(self, key: str, above=None, below=None):
        """
        When the current unbroken breach began, or None if not breaching.
        Judged on one-minute means, as the service aggregates before it
        alerts: a queue that sawtooths through the threshold inside each
        minute is still breaching.
        """

        since = None
        pairs = list(zip(self.times, self.series.get(key, ())))
        per_minute = max(1, int(ALERT_RESOLUTION // DT))

        for end in range(len(pairs), 0, -per_minute):
            window = pairs[max(0, end - per_minute):end]

            if any(value is None for _, value in window):
                break

            value = sum(v for _, v in window) / len(window)

            if (above is not None and value > above) or (below is not None and value < below):
                since = window[0][0]

            else:
                break

        return since

    # -----------------------------------------------------------------
    # Persistence
    # -----------------------------------------------------------------

    def to_dict(self) -> dict:
        keep = [i for i, t in enumerate(self.times) if self.t - t <= RAW_PERSIST]
        first = keep[0] if keep else len(self.times)

        def rounded(column):
            return [None if v is None else round(v, 4) for v in column]

        return {
            "t": self.t,
            "cpu_scale": self.cpu_scale,
            "pause_until": self.pause_until,
            "expired": self.expired,
            "queues": {cid: [list(c) for c in q] for cid, q in self.queues.items() if q},
            "state": self.state,
            "times": list(self.times)[first:],
            "series": {k: rounded(list(v)[first:]) for k, v in self.series.items()},
            "coarse_times": list(self.coarse_times),
            "coarse": {k: rounded(v) for k, v in self.coarse.items()},
        }

    @classmethod
    def from_dict(cls, data: dict, definition: dict, parameters: dict, cluster_size: str,
                  nodes: int) -> "Simulation":
        sim = cls.__new__(cls)
        sim.t = data["t"]
        sim.cpu_scale = data.get("cpu_scale", 1.0)
        sim.pause_until = data.get("pause_until", 0.0)
        sim.expired = data.get("expired", {})
        sim.queues = {cid: deque(cohorts) for cid, cohorts in data.get("queues", {}).items()}
        sim.state = data.get("state", {})
        sim.times = deque(data.get("times", []))
        sim.series = {k: deque(v) for k, v in data.get("series", {}).items()}
        sim.coarse_times = deque(data.get("coarse_times", []))
        sim.coarse = {k: deque(v) for k, v in data.get("coarse", {}).items()}
        sim.configure(definition, parameters, cluster_size, nodes)

        return sim
