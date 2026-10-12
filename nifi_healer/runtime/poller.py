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

Outcomes. An applied action is judged once VERIFY_SETTLE_SECONDS plus
VERIFY_WINDOW_SECONDS have passed since it finished, from a single chart
response split at the change (kpi/outcomes.py). A failed chart read is
retried on the next pass rather than recorded as INCONCLUSIVE.

The thread is started behind a module flag, not from a page-load hook:
Gradio's demo.load runs per browser tab, which in the cai_langgraph
articles started one poller per tab.
"""

import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor

import config
from nifi_healer import fleet
from nifi_healer.cdp.client import Client, get_client, is_half_applied
from nifi_healer.kpi import detectors, metrics, outcomes
from nifi_healer.store.db import Store, get_store

BUSY_STATES = ("UPDATING", "DEPLOYING", "UNKNOWN", "TERMINATING")

_TICK = 0.25          # wall seconds between scheduling passes
_FLEET_EVERY = 2.0    # wall seconds between fleet file reads


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

        found = detectors.detect(name, entry, read["charts"], alerts, now)
        samples = []

        for kpi in entry.get("kpis") or []:
            chart = metrics.find_chart(read["charts"], kpi["kind"], kpi["component"],
                                       kpi["metric"], kpi.get("chart"))
            latest = metrics.latest_sample(chart)

            if latest:
                samples.append((kpi["key"], latest[0], latest[1]))

        self.store.add_samples(crn, samples)
        series = {}

        for kpi in entry.get("kpis") or []:
            chart = metrics.find_chart(read["charts"], kpi["kind"], kpi["component"],
                                       kpi["metric"], kpi.get("chart"))
            series[kpi["key"]] = metrics.points(chart)

        with self.lock:
            view.update(kpis=found["kpis"], signals=found["signals"], series=series,
                        unmatched_alerts=found["unmatched_alerts"], sources=read["sources"],
                        kpi_at=now, kpi_wall=time.time())

        busy = self._busy(facts, flow)

        with self.lock:
            view["detection_paused"] = busy

        if not busy:
            self.open_incidents(name, entry, found["signals"], flow, now)

        return view

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
        due = self.store.actions_awaiting_outcome(
            now - config.VERIFY_SETTLE_SECONDS - config.VERIFY_WINDOW_SECONDS)
        judged = []

        for action in due:
            result = self.evaluate_action(action, now)

            if result:
                judged.append(result)

        return judged

    def evaluate_action(self, action: dict, now: float) -> dict | None:
        incident = self.store.incident(action["incident_id"]) or {}
        entry = fleet.pack(action["pack"])
        kpis = [k for k in entry.get("kpis") or [] if k["scenario"] == incident.get("scenario")]

        if not kpis:
            outcome = {"verdict": outcomes.INCONCLUSIVE, "kpis": [], "side_effects": [],
                       "reason": f"no KPIs recorded for scenario {incident.get('scenario')!r}"}
            period = ""

        else:
            period = outcomes.period_for(action["started_at"], now)
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

            outcome = outcomes.evaluate(kpis, series, action["started_at"], action["finished_at"])

        self.store.add_outcome(action["incident_id"], action["id"], outcome["verdict"], period,
                               outcome, now)
        log(f"[{action['pack']}] action {action['id']} ({action['action_type']}) on "
            f"{incident.get('id')}: {outcome['verdict']}")

        waiting = [a for a in self.store.actions(incident_id=action["incident_id"])
                   if a["status"] == "APPLIED" and not a["dry_run"] and a["id"] != action["id"]
                   and not any(o["action_id"] == a["id"]
                               for o in self.store.outcomes(action["incident_id"]))]

        if incident and not waiting:
            status = "RESOLVED" if outcome["verdict"] == outcomes.IMPROVED else "UNRESOLVED"
            self.store.update_incident(incident["id"], now, status=status,
                                       verdict=outcome["verdict"])

        return outcome

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
