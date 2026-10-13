"""
The poller: one background thread that watches every registered pack,
opens incidents, and measures the outcome of every applied fix.

    from nifi_healer.runtime import poller
    p = poller.start(on_incident=handler)   # idempotent: one thread per process
    p.snapshot()                            # what the UI shows, from memory

Two tiers, each on its own interval and per pack:

  A  every POLL_HEALTH_SECONDS   describe-deployment, describe-flow-in-
                                 deployment, list-deployment-active-alerts
  B  every POLL_KPI_SECONDS      the three chart surfaces (metric_charts),
                                 then the detectors, then incidents
  C  every POLL_EVENTS_SECONDS   list-deployment-events: what changed and
                                 when, for the Monitor tab

Once per process and pack, history is backfilled from the longer chart
periods (an hour, 12 hours, a day), filling only the gaps between the
buckets already stored: the app may have been down, or the pack new.

Reads run on a small pool (POLL_WORKERS), never two of the same pack and
tier at once, so one slow CLI call delays one pack instead of the fleet.
Everything read lands in an in-memory cache (the UI's source, refreshed
every second without touching the tenant) and the newest bucket of each
KPI in SQLite.

Incidents. Critical signals are grouped by process group. A group with
an open incident just refreshes that incident's signals; one without
opens a new incident, unless its last one closed less than
INCIDENT_COOLDOWN_SECONDS ago. Nothing is opened while the deployment is
mid-change (UPDATING, DEPLOYING, a half-applied version swap) or
unreadable. `on_incident(incident)` is called for each new incident when
AUTO_DIAGNOSE is on.

Outcomes. An applied action is judged once per verify window
(config.VERIFY_WINDOWS: 15 min, 1 h, 12 h) as each closes, every time
from a single chart response split at the change (kpi/outcomes.py). A
later change that could move the same KPIs cuts a window short
(INCONCLUSIVE). A failed chart read is retried on the next pass rather
than recorded as INCONCLUSIVE.

The thread is started behind a module flag, not from a page-load hook:
Gradio's demo.load runs per browser tab, which in the cai_langgraph
articles started one poller per tab.
"""

import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor

import config
from flows.builder.lib import iter_processors
from flows.builder.processors import kind_of
from nifi_healer import fleet
from nifi_healer.cdp.client import Client, get_client, is_half_applied
from nifi_healer.kpi import changes, detectors, metrics, outcomes, rollup
from nifi_healer.store.db import Store, get_store

BUSY_STATES = ("UPDATING", "DEPLOYING", "UNKNOWN", "TERMINATING")

_TICK = 0.25          # wall seconds between scheduling passes
_FLEET_EVERY = 2.0    # wall seconds between fleet file reads
_BACKFILL_RETRY = 600  # wall seconds before a failed backfill is tried again

# Fine to coarse, so the finer buckets claim their times first.
BACKFILL_PERIODS = ("LAST_ONE_HOUR", "LAST_TWELVE_HOURS", "LAST_ONE_DAY")

# Change kinds that cut a verify window short; KPI edits and the
# deployment's creation change no data.
_CUTTING_KINDS = ("flow_version", "parameters", "sizing", "other")


def log(message: str) -> None:
    print(f"[poller] {message}", flush=True)


class Poller:
    def __init__(self, client: Client | None = None, store: Store | None = None,
                 on_incident=None):
        self.client = client or get_client()
        self.store = store or get_store()
        self.on_incident = on_incident
        self.lock = threading.RLock()
        self.cache = {}
        self.heartbeat = {"started_at": None, "loops": 0, "last_loop": None,
                          "last_error": None, "last_error_at": None}
        self._stop = threading.Event()
        self._thread = None
        self._pool = ThreadPoolExecutor(max_workers=config.POLL_WORKERS,
                                        thread_name_prefix="poll")
        self._in_flight = set()
        self._last_run = {}
        self._fleet = {}
        self._fleet_at = 0.0
        self._kinds = {}  # flow version crn -> {processor name: kind}; a version never changes
        self._backfilled = set()

    # -- lifecycle --------------------------------------------------------

    def start(self) -> "Poller":
        with self.lock:
            if self._thread and self._thread.is_alive():
                return self

            self._stop.clear()
            self.heartbeat["started_at"] = time.time()
            self._thread = threading.Thread(target=self._loop, name="poller", daemon=True)
            self._thread.start()
            log(f"started: health every {config.POLL_HEALTH_SECONDS}s, KPIs every "
                f"{config.POLL_KPI_SECONDS}s, {config.POLL_WORKERS} worker(s), "
                f"{self.client.kind} client")

        return self

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()

        if self._thread:
            self._thread.join(timeout)

    @property
    def running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    # -- scheduling ---------------------------------------------------------

    def _packs(self) -> dict:
        if time.monotonic() - self._fleet_at > _FLEET_EVERY:
            self._fleet = fleet.deployed_packs()
            self._fleet_at = time.monotonic()

        return self._fleet

    def _due(self, job: tuple, interval: float) -> bool:
        with self.lock:
            if job in self._in_flight:
                return False

            if time.monotonic() - self._last_run.get(job, -1e9) < interval:
                return False

            self._in_flight.add(job)
            self._last_run[job] = time.monotonic()

            return True

    def _submit(self, job: tuple, fn, *args) -> None:
        def run():
            try:
                fn(*args)

            except Exception as exc:     # one pack's failure must not stop the loop
                self._error(f"{job[0]} {job[1]}: {exc}")
                traceback.print_exc()

            finally:
                with self.lock:
                    self._in_flight.discard(job)

        self._pool.submit(run)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                for name, entry in self._packs().items():
                    if self._due(("health", name), config.POLL_HEALTH_SECONDS):
                        self._submit(("health", name), self.poll_health, name, entry)

                    if self._due(("kpis", name), config.POLL_KPI_SECONDS):
                        self._submit(("kpis", name), self.poll_kpis, name, entry)

                    if self._due(("events", name), config.POLL_EVENTS_SECONDS):
                        self._submit(("events", name), self.poll_events, name, entry)

                    # After the pack's first chart read, so a restart shows the fleet first.
                    if (name not in self._backfilled and "kpi_at" in self.cache.get(name, {})
                            and self._due(("backfill", name), _BACKFILL_RETRY)):
                        self._submit(("backfill", name), self.backfill, name, entry)

                if self._due(("outcomes", ""), config.POLL_KPI_SECONDS):
                    self._submit(("outcomes", ""), self.evaluate_outcomes)

                with self.lock:
                    self.heartbeat["loops"] += 1
                    self.heartbeat["last_loop"] = time.time()

            except Exception as exc:
                self._error(f"loop: {exc}")

            self._stop.wait(_TICK)

    def _error(self, message: str) -> None:
        with self.lock:
            self.heartbeat["last_error"] = message
            self.heartbeat["last_error_at"] = time.time()

        log(f"error: {message}")

    def _view(self, name: str) -> dict:
        return self.cache.setdefault(name, {"pack": name, "signals": [], "kpis": {},
                                            "series": {}, "alerts": []})

    # -- tier A -------------------------------------------------------------

    def poll_health(self, name: str, entry: dict) -> dict:
        crn = entry["deployment_crn"]
        facts = self.client.deployment_facts(crn)
        flow = self.client.flow_facts(crn, entry.get("deployed_flow_crn", ""))
        alerts = self.client.active_alerts(crn) if not facts.get("error") else []
        now = self.client.now()

        if not facts.get("error"):
            self.store.upsert_deployment(crn, name, facts, now)

        if flow.get("flow_version_crn"):
            baseline = flow["flow_version_crn"] == entry.get("baseline_flow_version_crn")
            self.store.note_flow_version(flow["flow_version_crn"], name, entry.get("flow_crn"),
                                         flow.get("flow_version"), now,
                                         source="baseline" if baseline else "external")

        with self.lock:
            view = self._view(name)
            view.update(deployment_crn=crn, facts=facts, flow=flow, alerts=alerts,
                        health_at=now, health_wall=time.time())

        return view

    # -- tier B -------------------------------------------------------------

    def poll_kpis(self, name: str, entry: dict) -> dict:
        crn = entry["deployment_crn"]
        read = self.client.metric_charts(crn, entry.get("deployed_flow_crn", ""))
        now = read.get("read_at") or self.client.now()

        with self.lock:
            view = self._view(name)
            alerts = list(view.get("alerts") or [])
            facts = view.get("facts") or {}
            flow = view.get("flow") or {}

        found = detectors.detect(name, entry, read["charts"], alerts, now, self.processor_kinds(flow))
        samples = []

        for kpi in entry.get("kpis") or []:
            chart = metrics.find_chart(read["charts"], kpi["kind"], kpi["component"],
                                       kpi["metric"], kpi.get("chart"))
            latest = metrics.latest_sample(chart)

            if latest:
                samples.append((kpi["key"], latest[0], latest[1]))

        system = metrics.system_series(read["charts"])
        samples += [(f"system:{short}",) + points[-1] for short, points in system.items() if points]
        self.store.add_samples(crn, samples)
        series = {}

        for kpi in entry.get("kpis") or []:
            chart = metrics.find_chart(read["charts"], kpi["kind"], kpi["component"],
                                       kpi["metric"], kpi.get("chart"))
            series[kpi["key"]] = metrics.points(chart)

        groups = {}

        for kpi in entry.get("kpis") or []:
            groups.setdefault(kpi["scenario"], []).append(kpi)

        groups = {scenario: rollup.group_flow(kpis, series) for scenario, kpis in groups.items()}

        with self.lock:
            view.update(kpis=found["kpis"], signals=found["signals"], series=series, groups=groups,
                        system={short: metrics.summarize(points) for short, points in system.items()},
                        unmatched_alerts=found["unmatched_alerts"], sources=read["sources"],
                        kpi_at=now, kpi_wall=time.time())

        busy = self._busy(facts, flow)

        with self.lock:
            view["detection_paused"] = busy

        if not busy:
            self.open_incidents(name, entry, found["signals"], flow, now)

        return view

    # -- tier C and history -------------------------------------------------

    def poll_events(self, name: str, entry: dict) -> int:
        """Record the deployment's changes; returns how many were new."""

        found = changes.from_events(self.client.deployment_events(entry["deployment_crn"]))
        added = self.store.add_changes(entry["deployment_crn"], name, found)

        if added:
            log(f"[{name}] {added} new change(s) in the event log")

        return added

    def backfill(self, name: str, entry: dict) -> int:
        """
        Fill the stored history from the longer chart periods. A period
        that fails to read leaves the pack to be tried again later.
        Returns the number of samples added.
        """

        crn, added = entry["deployment_crn"], 0

        for period in BACKFILL_PERIODS:
            read = self.client.metric_charts(crn, entry.get("deployed_flow_crn", ""), time_period=period)

            if any(str(v).startswith("error") for v in read["sources"].values()):
                log(f"[{name}] backfill: {period} unreadable; trying again later")

                return added

            series = {kpi["key"]: metrics.points(metrics.find_chart(
                read["charts"], kpi["kind"], kpi["component"], kpi["metric"], kpi.get("chart")))
                for kpi in entry.get("kpis") or []}
            series.update({f"system:{short}": points
                           for short, points in metrics.system_series(read["charts"]).items()})

            for key, points in series.items():
                if len(points) > 1:
                    gaps = sorted(b[0] - a[0] for a, b in zip(points, points[1:]))
                    added += self.store.fill_samples(crn, [(key, t, v) for t, v in points],
                                                     gaps[len(gaps) // 2])

        self._backfilled.add(name)
        log(f"[{name}] backfill: {added} sample(s) added")

        return added

    def processor_kinds(self, flow: dict) -> dict:
        """
        {processor name: kind} of the deployed flow version, read once per
        version; {} until a health poll has seen the version, or if the
        read fails (retried next pass).
        """

        crn = flow.get("flow_version_crn")

        if not crn:
            return {}

        if crn not in self._kinds:
            definition = self.client.flow_definition(crn)

            if "error" in definition:
                self._error(f"processor kinds of {crn}: {definition['error']}")
                return {}

            self._kinds[crn] = {p.get("name"): kind_of(p) for _, _, p in iter_processors(definition)}

        return self._kinds[crn]

    @staticmethod
    def _busy(facts: dict, flow: dict) -> str:
        """Why detection should wait, or "" when the deployment is steady."""

        if not facts:
            return "no health read yet"

        if facts.get("state") in BUSY_STATES:
            return f"deployment is {facts.get('state')}"

        if flow.get("state") in BUSY_STATES:
            return f"deployed flow is {flow.get('state')}"

        if is_half_applied(flow):
            return "a flow version change is half-applied"

        return ""

    # -- incidents ------------------------------------------------------------

    def open_incidents(self, name: str, entry: dict, signals: list, flow: dict, now: float) -> list:
        crn = entry["deployment_crn"]
        opened = []

        for group, found in detectors.by_group(signals).items():
            current = self.store.open_incident(crn, group)

            if current:
                self.store.update_incident(current["id"], now, signals=found, last_signal_at=now)
                continue

            last = self.store.last_closed_incident(crn, group)

            if last and now - last["closed_at"] < config.INCIDENT_COOLDOWN_SECONDS:
                continue

            incident = self.store.create_incident(
                pack=name, deployment_crn=crn,
                deployed_flow_crn=entry.get("deployed_flow_crn", ""),
                scenario=found[0]["scenario"], process_group=group, signals=found,
                summary=detectors.headline(found),
                expected_flow_version_crn=flow.get("flow_version_crn") or "", now=now)
            opened.append(incident)
            log(f"[{name}] opened {incident['id']} for {group}: {incident['summary']}")

            if self.on_incident and config.AUTO_DIAGNOSE:
                try:
                    self.on_incident(incident)

                except Exception as exc:
                    self._error(f"on_incident {incident['id']}: {exc}")

        return opened

    # -- outcomes -------------------------------------------------------------

    def evaluate_outcomes(self) -> list:
        now = self.client.now()
        judged = []

        for action, window in self.store.windows_due(now):
            result = self.evaluate_action(action, now, window)

            if result:
                judged.append(result)

        return judged

    def evaluate_action(self, action: dict, now: float, window: int | None = None) -> dict | None:
        """
        One verify window's verdict on one applied action. The first window
        closes the incident when it is clear (IMPROVED or WORSENED); the
        second closes it either way. Later ones never change it: they flag
        a regression when a resolved incident's KPIs got worse after all.
        """

        window = window or config.VERIFY_WINDOW_SECONDS
        incident = self.store.incident(action["incident_id"]) or {}
        entry = fleet.pack(action["pack"])
        kpis = [k for k in entry.get("kpis") or [] if k["scenario"] == incident.get("scenario")]
        end = outcomes.due_at(action["finished_at"], window, action["started_at"])
        cut = self._cut_by(action, incident, end) if kpis else None
        span = outcomes.plan(window, action["started_at"], now)
        period = ""

        if not kpis:
            outcome = {"verdict": outcomes.INCONCLUSIVE, "kpis": [], "side_effects": [],
                       "reason": f"no KPIs recorded for scenario {incident.get('scenario')!r}"}

        elif cut:
            outcome = {"verdict": outcomes.INCONCLUSIVE, "kpis": [], "side_effects": [], "cut": cut,
                       "reason": f"cut short by {cut['text']} "
                                 f"{max(cut['t'] - action['finished_at'], 0) / 60:.0f} min after this change"}

        elif span is None:
            outcome = {"verdict": outcomes.INCONCLUSIVE, "kpis": [], "side_effects": [], "missed": True,
                       "reason": "missed: no chart reaches back to the change any more (the app "
                                 "was not judging this window when it closed)"}

        else:
            period = span["period"]
            read = self.client.metric_charts(action["deployment_crn"],
                                             entry.get("deployed_flow_crn", ""), time_period=period)

            if not any(s.startswith("ok") for s in read["sources"].values()):
                self._error(f"outcome for action {action['id']}: no chart surface could be read "
                            f"({read['sources']}); retrying next pass")
                return None

            series = {}

            for kpi in kpis:
                chart = metrics.find_chart(read["charts"], kpi["kind"], kpi["component"],
                                           kpi["metric"], kpi.get("chart"))
                series[kpi["key"]] = metrics.points(chart)

            signalled = {s.get("kpi") for s in incident.get("signals") or []}
            outcome = outcomes.evaluate(kpis, series, action["started_at"], action["finished_at"],
                                        signalled, span)

        outcome["window_seconds"] = window
        deciding = window in config.VERIFY_WINDOWS[:2]

        # Resolved by an earlier window, then worse after all.
        if window != config.VERIFY_WINDOWS[0] and incident.get("status") == "RESOLVED" \
                and outcome["verdict"] == outcomes.WORSENED:
            outcome["regression"] = True

        self.store.add_outcome(action["incident_id"], action["id"], outcome["verdict"], period,
                               outcome, now, window)
        log(f"[{action['pack']}] action {action['id']} ({action['action_type']}) on "
            f"{incident.get('id')}, {window // 60} min window: {outcome['verdict']}"
            + (" (a regression of a resolved incident)" if outcome.get("regression") else ""))

        if deciding and incident.get("status") == "VERIFYING":
            self._close_or_wait(incident, action, window, outcome["verdict"], now)

        return outcome

    def _close_or_wait(self, incident: dict, action: dict, window: int, verdict: str, now: float) -> None:
        """Close the incident on this window's verdict, unless it waits for the next window or another action."""

        judged = {(o["action_id"], o["window_seconds"]) for o in self.store.outcomes(incident["id"])}
        waiting = [a for a in self.store.actions(incident_id=incident["id"])
                   if a["status"] == "APPLIED" and not a["dry_run"] and a["id"] != action["id"]
                   and (a["id"], window) not in judged]

        if waiting:
            return

        last = len(config.VERIFY_WINDOWS) == 1 or window == config.VERIFY_WINDOWS[1]

        if last or verdict in (outcomes.IMPROVED, outcomes.WORSENED):
            self.store.update_incident(incident["id"], now, verdict=verdict,
                                       status="RESOLVED" if verdict == outcomes.IMPROVED else "UNRESOLVED")

        else:
            # Still VERIFYING, so the group opens no new incident while a
            # backlog drains: the next window decides.
            self.store.update_incident(incident["id"], now, verdict=verdict)

    def _cut_by(self, action: dict, incident: dict, end: float) -> dict | None:
        """
        The first later change that could move this fix's KPIs before its
        window ends at `end`: an operator's (its reach is unknown), or the
        agent's on the same process group or on the deployment's size. The
        agent's edits elsewhere in the flow restart only their own
        components, so they do not cut it.
        """

        crn = action["deployment_crn"]
        found = changes.attribute(self.store.changes(crn), self.store.actions(deployment_crn=crn))

        for change in found:
            other = change.get("action")

            if not action["started_at"] < change["t"] < end or (other or {}).get("id") == action["id"]:
                continue

            kind = change["kind"].replace("_", " ")

            if change["source"] == "operator" and change["kind"] in _CUTTING_KINDS:
                return {"t": change["t"], "source": "operator", "kind": change["kind"],
                        "text": f"an operator's {kind} change"}

            if change["source"] == "agent" and other:
                group = (self.store.incident(other.get("incident_id") or "") or {}).get("process_group")

                if other.get("family") == "SIZING" or group == incident.get("process_group"):
                    return {"t": change["t"], "source": "agent", "kind": change["kind"],
                            "action_id": other["id"], "text": f"the agent's {other.get('action_type')}"}

        return None

    # -- reading ----------------------------------------------------------------

    def poll_now(self, name: str = "") -> None:
        """Synchronous tier A then B, for one pack or all: tests and the UI's refresh button."""

        for pack_name, entry in fleet.deployed_packs().items():
            if name and pack_name != name:
                continue

            self.poll_health(pack_name, entry)
            self.poll_kpis(pack_name, entry)

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "packs": {name: dict(view) for name, view in self.cache.items()},
                "heartbeat": dict(self.heartbeat, running=self.running),
                "now": self.client.now(),
            }


_POLLER = {"poller": None}
_POLLER_LOCK = threading.Lock()


def start(client: Client | None = None, store: Store | None = None, on_incident=None) -> Poller:
    """The process's poller, started on first call. Later calls only set `on_incident`."""

    with _POLLER_LOCK:
        if _POLLER["poller"] is None:
            _POLLER["poller"] = Poller(client, store, on_incident)

        poller = _POLLER["poller"]

        if on_incident is not None:
            poller.on_incident = on_incident

    return poller.start()


def get() -> Poller | None:
    return _POLLER["poller"]
