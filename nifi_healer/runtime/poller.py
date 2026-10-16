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
AUTO_DIAGNOSE is on. `on_tick(client, store)` is called every
POLL_KPI_SECONDS (agent/autonomy.py's veto windows).

Outcomes. An applied action is judged once per verify window
(config.VERIFY_WINDOWS: 15 min, 1 h, 12 h) as each closes, every time
from a single chart response split at the change (kpi/outcomes.py). A
later change that could move the same KPIs cuts a window short
(INCONCLUSIVE). A failed chart read is retried on the next pass rather
than recorded as INCONCLUSIVE. A reviewer can re-check a window while a
chart still reaches it (recheck), from a later start to skip a stretch
the fix could not show in; the verdict it replaces is kept.

Rollbacks. A fix whose deciding window measured WORSENED, or whose later
window flags a regression, gets a rollback incident (actions/rollback.py)
unless a reviewer labelled it the right fix; it goes to review like any
other. While its process group is busy, or the deployment has moved on,
the poller says so once and tries again until ROLLBACK_WITHIN_SECONDS
after the verdict.

The thread is started behind a module flag, not from a page-load hook:
Gradio's demo.load runs per browser tab, which in the cai_langgraph
articles started one poller per tab.
"""

import os
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor

import config
from flows import packs
from flows.builder.lib import iter_processors
from flows.builder.processors import kind_of
from nifi_healer import chaos, fleet
from nifi_healer.actions import rollback
from nifi_healer.cdp.client import Client, get_client, is_half_applied
from nifi_healer.kpi import changes, detectors, metrics, outcomes, rollup
from nifi_healer.runtime import telemetry
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

HEALTH_EVERY = 60          # wall seconds between App health samples
RETENTION_EVERY = 3600     # wall seconds between pruning passes

# Each tier's newest read, from the view, and how often it should come.
_TIERS = (("health", "health_wall", "POLL_HEALTH_SECONDS"), ("kpis", "kpi_wall", "POLL_KPI_SECONDS"),
          ("events", "events_wall", "POLL_EVENTS_SECONDS"))


def log(message: str) -> None:
    print(f"[poller] {message}", flush=True)


def window_label(seconds: int) -> str:
    return f"{seconds // 3600} h" if seconds >= 3600 and not seconds % 3600 else f"{seconds // 60} min"


def settled(verdicts: dict) -> tuple:
    """
    (status, verdict) from one action's deciding windows, {window seconds:
    verdict}: the first window when it is clear (IMPROVED or WORSENED),
    else the second. (None, the first's verdict) while the second is yet
    to come, and (None, None) before any.
    """

    first, *later = config.VERIFY_WINDOWS
    verdict = verdicts.get(first)

    if verdict is None:
        return None, None

    if verdict not in (outcomes.IMPROVED, outcomes.WORSENED) and later:
        if later[0] not in verdicts:
            return None, verdict

        verdict = verdicts[later[0]]

    return ("RESOLVED" if verdict == outcomes.IMPROVED else "UNRESOLVED"), verdict


class Poller:
    def __init__(self, client: Client | None = None, store: Store | None = None,
                 on_incident=None, on_tick=None):
        self.client = client or get_client()
        self.store = store or get_store()
        self.on_incident = on_incident
        self.on_tick = on_tick
        self.lock = threading.RLock()
        self.cache = {}
        self.heartbeat = {"started_at": None, "loops": 0, "last_loop": None,
                          "last_error": None, "last_error_at": None}
        self._stop = threading.Event()
        self._thread = None
        self.workers = config.poll_workers(0)
        self._pool = ThreadPoolExecutor(max_workers=self.workers, thread_name_prefix="poll")
        self._in_flight = set()
        self._last_run = {}
        self._waits = {}     # tier -> [seconds a job waited for a worker], since the last health sample
        self._pending = 0
        self._errors = []    # wall times of recent errors, for the errors-per-hour count
        self._fleet = {}
        self._fleet_at = 0.0
        self._kinds = {}  # flow version crn -> {processor name: kind}; a version never changes
        self._backfilled = set()
        self.judging = threading.Lock()   # the poller's verdicts and a reviewer's re-check, one at a time
        self._not_rolled_back = {}        # action id -> why no rollback was proposed, as last logged

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
                f"{config.POLL_KPI_SECONDS}s, {self.workers} worker(s) to begin with, "
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
            self._grow(config.poll_workers(len(self._fleet)))

        return self._fleet

    def _grow(self, workers: int) -> None:
        """A bigger pool for a bigger fleet; the old one finishes what it holds."""

        if workers <= self.workers:
            return

        old, self._pool = self._pool, ThreadPoolExecutor(max_workers=workers, thread_name_prefix="poll")
        self.workers = workers
        old.shutdown(wait=False)
        log(f"{workers} worker(s) for {len(self._fleet)} deployment(s)")

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
        submitted = time.monotonic()

        with self.lock:
            self._pending += 1

        def run():
            with self.lock:
                self._pending -= 1
                waits = self._waits.setdefault(job[0], [])

                if len(waits) < 10000:
                    waits.append(time.monotonic() - submitted)

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

                if self.on_tick and self._due(("autonomy", ""), config.POLL_KPI_SECONDS):
                    self._submit(("autonomy", ""), self.on_tick, self.client, self.store)

                if config.CHAOS and self._due(("chaos", ""), config.CHAOS_TICK):
                    self._submit(("chaos", ""), chaos.tick, self.client, self.store)

                if self._due(("app_health", ""), HEALTH_EVERY):
                    self._submit(("app_health", ""), self.sample_health)

                if self._due(("retention", ""), RETENTION_EVERY):
                    self._submit(("retention", ""), self.retain)

                with self.lock:
                    self.heartbeat["loops"] += 1
                    self.heartbeat["last_loop"] = time.time()

            except Exception as exc:
                self._error(f"loop: {exc}")

            self._stop.wait(_TICK)

    def _error(self, message: str) -> None:
        with self.lock:
            now = time.time()
            self.heartbeat["last_error"] = message
            self.heartbeat["last_error_at"] = now
            self._errors = [t for t in self._errors if now - t < 3600][-999:] + [now]

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

        with self.lock:
            self._view(name)["events_wall"] = time.time()

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

        with self.judging:
            for action, window in self.store.windows_due(now):
                result = self.evaluate_action(action, now, window)

                if result:
                    judged.append(result)

        if config.PROPOSE_ROLLBACKS:
            self.propose_rollbacks(now)

        return judged

    def propose_rollbacks(self, now: float) -> list:
        """Rollback incidents for the fixes judged worse within ROLLBACK_WITHIN_SECONDS; see the module notes."""

        since = now - config.ROLLBACK_WITHIN_SECONDS
        recent = {}

        for found in self.store.outcomes():          # newest first
            if found["evaluated_at"] < since:
                break

            recent.setdefault(found["action_id"], found["incident_id"])

        opened = []

        for action_id, incident_id in recent.items():
            if rollback.is_rollback(self.store.incident(incident_id)) or self.store.rollbacks_of(action_id):
                continue

            reason = self._worse(incident_id, action_id)
            label = self.store.labels(incident_id).get(action_id)

            if not reason or (label and label["label"] == "RIGHT_FIX"):
                continue

            action = self.store.action(action_id)
            why = rollback.blocked(self.store, action)

            if why:
                if self._not_rolled_back.get(action_id) != why:
                    self._not_rolled_back[action_id] = why
                    log(f"[{action['pack']}] action {action_id}: {reason}; no rollback proposed yet: {why}")

                continue

            incident = rollback.open_for(self.store, action, reason, now)
            opened.append(incident)
            log(f"[{action['pack']}] opened {incident['id']}: {incident['summary']}")

            if self.on_incident and config.AUTO_DIAGNOSE:
                try:
                    self.on_incident(incident)

                except Exception as exc:
                    self._error(f"on_incident {incident['id']}: {exc}")

        return opened

    def _worse(self, incident_id: str, action_id: int) -> str:
        """Why the action measured worse, "" unless it did: its deciding windows, or a later regression."""

        rows = [o for o in self.store.outcomes(incident_id) if o["action_id"] == action_id]
        verdicts = {o["window_seconds"]: o["verdict"] for o in rows}
        status, verdict = settled(verdicts)

        if status and verdict == outcomes.WORSENED:
            window = next(w for w in config.VERIFY_WINDOWS if verdicts.get(w) == outcomes.WORSENED)

            return f"its {window_label(window)} verify window measured WORSENED"

        late = next((o for o in rows if (o.get("details") or {}).get("regression")), None)

        if late:
            return (f"its {window_label(late['window_seconds'])} verify window measured WORSENED "
                    "after it had resolved the incident")

        return ""

    def evaluate_action(self, action: dict, now: float, window: int | None = None) -> dict | None:
        """
        One verify window's verdict on one applied action. The first window
        closes the incident when it is clear (IMPROVED or WORSENED); the
        second closes it either way. Later ones never change it: they flag
        a regression when a resolved incident's KPIs got worse after all.
        """

        window = window or config.VERIFY_WINDOW_SECONDS
        incident = self.store.incident(action["incident_id"]) or {}
        kpis = self._kpis(action, incident)
        end = outcomes.due_at(action["finished_at"], window, action["started_at"])
        cut = next(iter(self._cuts(action, incident, end)), None) if kpis else None
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
            outcome = self._measure(action, incident, kpis, span, action["finished_at"])

            if outcome is None:
                return None

            period = span["period"]

        outcome["window_seconds"] = window

        # Resolved by an earlier window, then worse after all.
        if window != config.VERIFY_WINDOWS[0] and incident.get("status") == "RESOLVED" \
                and outcome["verdict"] == outcomes.WORSENED:
            outcome["regression"] = True

        self.store.add_outcome(action["incident_id"], action["id"], outcome["verdict"], period,
                               outcome, now, window)
        log(f"[{action['pack']}] action {action['id']} ({action['action_type']}) on "
            f"{incident.get('id')}, {window // 60} min window: {outcome['verdict']}"
            + (" (a regression of a resolved incident)" if outcome.get("regression") else ""))

        if window in config.VERIFY_WINDOWS[:2] and incident.get("status") == "VERIFYING":
            self._close_or_wait(incident, action, window, now)

        return outcome

    def recheck(self, action_id: int, window: int, start: float | None = None,
                reviewer: str = "") -> tuple:
        """
        A reviewer's re-measure of one verify window, from `start` (when the
        change finished, or later, to skip a stretch the fix could not show
        in: another fault, a broken flow). (outcome, "") once recorded: it
        supersedes the window's verdict, which is kept, and the incident's
        status follows its deciding windows again. (None, why not) when the
        window cannot be measured now; nothing changes then.
        """

        now = self.client.now()
        action = self.store.action(action_id)

        if not action or action["status"] != "APPLIED" or action["dry_run"] or not action.get("finished_at"):
            return None, f"Action {action_id} was not applied to the tenant, so there is nothing to measure."

        if window not in config.VERIFY_WINDOWS:
            return None, f"{window} s is not a verify window ({', '.join(str(w) for w in config.VERIFY_WINDOWS)})."

        start = action["finished_at"] if start is None else start

        if start < action["finished_at"]:
            return None, "A measurement can start no earlier than the change finished."

        incident = self.store.incident(action["incident_id"]) or {}
        kpis = self._kpis(action, incident)

        if not kpis:
            return None, f"No KPIs are recorded for scenario {incident.get('scenario')!r}."

        span = outcomes.plan(window, action["started_at"], now)

        if span is None:
            # The finer charts are shorter: a 15 min window can be read for
            # about 40 min after the change, the 1 h for about ten hours.
            return None, (f"No chart fine enough for the {window // 60} min window reaches back to this change "
                          "any more, so it cannot be measured; a longer window may still be.")

        end = start + span["settle"] + window

        if end > now:
            return None, (f"That window closes {(end - now) / 60:.0f} min from now "
                          f"({span['settle'] / 60:.0f} min settle, then {window // 60} min); re-check after that.")

        cuts = self._cuts(action, incident, end)
        cut = next((c for c in cuts if c["t"] > start), None)

        if cut:
            return None, (f"{cut['text'][0].upper()}{cut['text'][1:]} {(cut['t'] - start) / 60:.0f} min after "
                          "that start would cut the window short. Start after it to measure both changes together.")

        with self.judging:
            outcome = self._measure(action, incident, kpis, span, start)

            if outcome is None:
                return None, "No chart surface could be read; try again."

            was = next((o for o in self.store.outcomes(incident["id"])
                        if o["action_id"] == action_id and o["window_seconds"] == window), None)
            outcome.update(window_seconds=window, recheck={
                "from": start, "by": reviewer, "at": now, "was": was["verdict"] if was else None,
                "skipped": [c["text"] for c in cuts if c["t"] <= start]})
            self.store.supersede_outcome(incident["id"], action_id, window, outcome["verdict"], span["period"],
                                         outcome, now)
            log(f"[{action['pack']}] action {action_id} on {incident['id']}, {window // 60} min window "
                f"re-checked from {(start - action['finished_at']) / 60:.0f} min after the change: "
                f"{outcome['verdict']}" + (f" (was {was['verdict']})" if was else ""))
            self._restate(self.store.incident(incident["id"]), action, window, now)

        return outcome, ""

    def _kpis(self, action: dict, incident: dict) -> list:
        entry = fleet.pack(action["pack"])

        return [k for k in entry.get("kpis") or [] if k["scenario"] == incident.get("scenario")]

    def _measure(self, action: dict, incident: dict, kpis: list, span: dict, start: float) -> dict | None:
        """
        The verdict from one read of span's chart, with the after window
        from `start`. None when no chart surface could be read (retried).
        """

        entry = fleet.pack(action["pack"])
        read = self.client.metric_charts(action["deployment_crn"], entry.get("deployed_flow_crn", ""),
                                         time_period=span["period"])

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
        outcome = outcomes.evaluate(kpis, series, action["started_at"], start, signalled, span)

        if start != action["finished_at"]:
            outcome.update(finished_at=action["finished_at"], measured_from=start)

        return outcome

    def _close_or_wait(self, incident: dict, action: dict, window: int, now: float) -> None:
        """Close the incident on its deciding windows, unless it waits for the next window or another action."""

        found = self.store.outcomes(incident["id"])
        judged = {(o["action_id"], o["window_seconds"]) for o in found}
        waiting = [a for a in self.store.actions(incident_id=incident["id"])
                   if a["status"] == "APPLIED" and not a["dry_run"] and a["id"] != action["id"]
                   and (a["id"], window) not in judged]

        if waiting:
            return

        status, verdict = settled({o["window_seconds"]: o["verdict"] for o in found
                                   if o["action_id"] == action["id"]})

        # Still VERIFYING without a status, so the group opens no new
        # incident while a backlog drains: the next window decides.
        self.store.update_incident(incident["id"], now, verdict=verdict, **({"status": status} if status else {}))

    def _restate(self, incident: dict, action: dict, window: int, now: float) -> None:
        """
        After a re-check: an incident still verifying closes or waits as
        usual; a closed one takes the status its last fix's deciding windows
        now give it (keeping when it closed), and every later window's
        regression flag follows that status.
        """

        if incident["status"] == "VERIFYING":
            if window in config.VERIFY_WINDOWS[:2]:
                self._close_or_wait(incident, action, window, now)

            return

        if incident["status"] not in ("RESOLVED", "UNRESOLVED"):
            return

        applied = [a for a in self.store.actions(incident_id=incident["id"])
                   if a["status"] == "APPLIED" and not a["dry_run"]]
        found = self.store.outcomes(incident["id"])
        status, verdict = settled({o["window_seconds"]: o["verdict"] for o in found
                                   if applied and o["action_id"] == applied[-1]["id"]})

        if status and (status, verdict) != (incident["status"], incident.get("verdict")):
            incident = self.store.update_incident(incident["id"], now, status=status, verdict=verdict,
                                                  closed_at=incident["closed_at"])
            log(f"[{incident['pack']}] {incident['id']} is now {status} ({verdict}) after a re-check")

        for o in found:
            details = o.get("details") or {}
            regression = o["window_seconds"] != config.VERIFY_WINDOWS[0] and incident["status"] == "RESOLVED" \
                and o["verdict"] == outcomes.WORSENED

            if bool(details.get("regression")) != regression:
                self.store.update_outcome(o["id"], details={**details, "regression": regression})

    def _cuts(self, action: dict, incident: dict, end: float) -> list:
        """
        The later changes, oldest first, that could move this fix's KPIs
        before its window ends at `end`: an operator's (its reach is
        unknown), or the agent's or the chaos injector's on the same
        process group or on the deployment's size. Their edits elsewhere in
        the flow restart only their own components, so they do not cut it.
        """

        crn = action["deployment_crn"]
        found = changes.attribute(self.store.changes(crn), self.store.actions(deployment_crn=crn),
                                  self.store.injections(deployment_crn=crn))
        out = []

        for change in found:
            other = change.get("action")

            if not action["started_at"] < change["t"] < end or (other or {}).get("id") == action["id"]:
                continue

            kind = change["kind"].replace("_", " ")

            if change["source"] == "operator" and change["kind"] in _CUTTING_KINDS:
                out.append({"t": change["t"], "source": "operator", "kind": change["kind"],
                            "text": f"an operator's {kind} change"})

            elif change["source"] == "agent" and other:
                group = (self.store.incident(other.get("incident_id") or "") or {}).get("process_group")

                if other.get("family") == "SIZING" or group == incident.get("process_group"):
                    out.append({"t": change["t"], "source": "agent", "kind": change["kind"],
                                "action_id": other["id"], "text": f"the agent's {other.get('action_type')}"})

            elif change["source"] == "chaos" and change.get("injection"):
                scenario = packs.SCENARIOS.get(change["injection"]["scenario"])

                # Worded like any change of this app's: the agent reads outcomes.
                if change["kind"] == "sizing" or (scenario and scenario.group_name == incident.get("process_group")):
                    out.append({"t": change["t"], "source": "chaos", "kind": change["kind"],
                                "text": "a later change by this app"})

        return sorted(out, key=lambda c: c["t"])

    # -- the app's own health -------------------------------------------------

    def lag(self, now: float | None = None) -> dict:
        """
        Per tier, how far each pack's newest read is behind its schedule
        (its age less the tier's interval, never below 0): the p95 and the
        worst over the packs read at least once.
        """

        now, out = now or time.time(), {}

        with self.lock:
            views = [dict(v) for v in self.cache.values()]

        for tier, field, interval in _TIERS:
            behind = [max(0.0, now - v[field] - getattr(config, interval)) for v in views if v.get(field)]

            if behind:
                out[f"poll.lag.{tier}"] = round(telemetry.percentile(behind, 95), 1)
                out[f"poll.lag.{tier}.max"] = round(max(behind), 1)

        return out

    def sample_health(self) -> dict:
        """One App health sample (store.health_samples): numbers only, never a call's arguments."""

        from nifi_healer.agent import graph

        values = self.lag()

        with self.lock:
            waits, self._waits = self._waits, {}
            values["poll.pending"] = self._pending
            values["poll.workers"] = self.workers
            values["errors.hour"] = sum(1 for t in self._errors if time.time() - t < 3600)

        for tier, seconds in waits.items():
            if tier in ("health", "kpis", "events"):
                values[f"poll.wait.{tier}"] = round(telemetry.percentile(seconds, 95), 2)

        values.update({"cdp.calls": 0, "cdp.errors": 0, "llm.calls": 0, "llm.errors": 0})

        for kind, summary in telemetry.drain().items():
            values.update({f"{kind}.{name}": value for name, value in summary.items()})

        values.update(graph.backlog())
        values.update(telemetry.process_stats())

        try:
            values["db.mb"] = round(os.path.getsize(self.store.path) / 2**20, 2)

        except OSError:
            pass

        self.store.add_health(time.time(), values)
        lagging = (values.get("poll.lag.kpis") or 0) > config.POLL_KPI_SECONDS

        with self.lock:
            self.heartbeat["health"] = values
            # Since when the KPI reads have run more than an interval late (the header's warning).
            self.heartbeat["kpis_late_since"] = (self.heartbeat.get("kpis_late_since") or time.time()
                                                 if lagging else None)

        return values

    def retain(self) -> dict:
        """
        Prune what grows without end: KPI samples older than KEEP_DAYS,
        App health older than HEALTH_KEEP_DAYS, and the checkpoints of
        incidents closed more than CHECKPOINT_KEEP_DAYS ago.
        """

        from nifi_healer.agent import graph

        now, day = self.client.now(), 86400
        pruned = {"kpi_samples": self.store.prune_samples(now - config.KEEP_DAYS * day),
                  "health_samples": self.store.prune_health(time.time() - config.HEALTH_KEEP_DAYS * day)}

        # From where the last pass stopped, so each closed incident is forgotten once.
        mark = float((self.store.setting("checkpoints_pruned_to") or {}).get("value") or 0)
        upto = now - config.CHECKPOINT_KEEP_DAYS * day

        if upto > mark:
            pruned["checkpoints"] = graph.forget(self.store.closed_between(mark, upto))
            self.store.set_setting("checkpoints_pruned_to", repr(upto), "retention", now)

        if any(pruned.values()):
            log("retention: " + ", ".join(f"{count} {what}" for what, count in pruned.items() if count))

        return pruned

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
                "heartbeat": dict(self.heartbeat, running=self.running,
                                  errors_hour=sum(1 for t in self._errors if time.time() - t < 3600)),
                "now": self.client.now(),
            }


_POLLER = {"poller": None}
_POLLER_LOCK = threading.Lock()


def start(client: Client | None = None, store: Store | None = None, on_incident=None,
          on_tick=None) -> Poller:
    """The process's poller, started on first call. Later calls only set `on_incident` and `on_tick`."""

    with _POLLER_LOCK:
        if _POLLER["poller"] is None:
            _POLLER["poller"] = Poller(client, store, on_incident, on_tick)

        poller = _POLLER["poller"]

        if on_incident is not None:
            poller.on_incident = on_incident

        if on_tick is not None:
            poller.on_tick = on_tick

    return poller.start()


def get() -> Poller | None:
    return _POLLER["poller"]
