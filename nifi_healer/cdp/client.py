"""
Cloudera DataFlow API access.

Every CDP call goes through a `Client`. There are two:

    LiveClient  runs the CDP CLI (cdpcli) in a subprocess
    MockClient  answers the same argv from a simulated tenant
                (nifi_healer/cdp/mock_client.py)

Both receive the same argument lists, so the guardrails, the provisioning
scripts and the executor run identical code against either, and a dry
run's argv is the argv an armed live run would execute.

Credentials: `CDP_ACCESS_KEY_ID` / `CDP_PRIVATE_KEY` are passed to the
subprocess through `env` and never placed on a command line (which `ps`
would expose), returned, or logged.

Two DataFlow CLI services, not interchangeable:

    df          control plane. Reads, flow-catalog writes, and
                change-flow-version-in-deployment (a CLI extension that
                hops to the workload plane internally).
    dfworkload  workload plane. Deployment configuration writes. Every
                operation needs --environment-crn, which `dfw()` injects.
                Served by a private load balancer: only reachable from
                inside the environment's VPC.

Ported from agentstudio_nifi_monitoring_and_self_healing_pipelines
(self_healing/df_api.py), where these behaviours were verified live.
"""

import base64
import json
import os
import select
import shlex
import socket
import subprocess
import sys
import threading
import time
import urllib.parse

import config
from nifi_healer.runtime import telemetry


# Deployment states settled enough to act on. Anything else means an
# action is in flight, the deployment is provisioning, or it is gone.
STEADY_STATES = frozenset({"GOOD_HEALTH", "CONCERNING_HEALTH", "BAD_HEALTH"})

# Offered in `validActions` when a version change is stuck between the
# control plane and the workload plane.
HALF_APPLIED_ACTIONS = frozenset(
    {"RETRY_CHANGE_FLOW_VERSION", "CANCEL_CHANGE_FLOW_VERSION"}
)

MAX_RAW_OUTPUT_CHARS = 4000

CDP_CREDENTIALS_FILE = os.path.expanduser("~/.cdp/credentials")

WORKER_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "worker.py")

# Operations the worker pool runs; anything else gets its own process.
READ_PREFIXES = ("describe-", "list-", "get-")

# Calls before a worker is replaced, so nothing cdpcli accumulates grows for ever.
WORKER_CALLS = 500


def command_line(args: list, prefix: str = "cdp") -> str:
    """
    Render an argv as a command line someone can paste.

    Display only: nothing runs through a shell. `--cluster-size
    {"name": "EXTRA_SMALL"}` is one argv element but three broken words
    at a prompt unless quoted.
    """

    return " ".join([prefix] + [shlex.quote(str(a)) for a in args])


def is_half_applied(facts: dict) -> bool:
    return bool(HALF_APPLIED_ACTIONS.intersection(facts.get("valid_actions") or []))


def require(result: dict, context: str = "") -> dict:
    """Return `result`, or exit with a legible message if it is an error."""

    if isinstance(result, dict) and "error" in result:
        lines = ["error: " + (context or "a CDP call failed")]

        for key in ("error", "command", "stderr", "hint", "detail"):
            value = result.get(key)

            if value:
                lines.append(f"  {key}: {value}")

        raise SystemExit("\n".join(lines))

    return result


class Client:
    """
    The DataFlow operations this app uses, over an abstract `run(argv)`.

    `run` returns the parsed JSON response, or a dict carrying an `error`
    key. It never raises for an API failure, so one failing read degrades
    one panel instead of the poller.
    """

    kind = "abstract"

    # -- transport ----------------------------------------------------

    def run(self, args: list) -> dict:
        raise NotImplementedError

    def now(self) -> float:
        """Seconds since the epoch, in the tenant's clock."""

        return time.time()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)

    def df(self, *args) -> dict:
        return self.run(["df"] + [str(a) for a in args])

    def dfw(self, *args) -> dict:
        return self.run(
            ["dfworkload"]
            + [str(a) for a in args]
            + ["--environment-crn", config.ENVIRONMENT_CRN]
        )

    def argv(self, plane: str, args: list) -> list:
        """The exact argv `df()`/`dfw()` would run, for audit and dry runs."""

        if plane == "dfworkload":
            return ["dfworkload"] + [str(a) for a in args] + [
                "--environment-crn", config.ENVIRONMENT_CRN
            ]

        if plane == "df":
            return ["df"] + [str(a) for a in args]

        raise ValueError(f"plane must be 'df' or 'dfworkload', got {plane!r}")

    # -- deployments --------------------------------------------------

    def list_deployments(self) -> list:
        result = self.df("list-deployments", "--max-items", 500)

        if "error" in result:
            return []

        return result.get("deployments") or []

    def describe_deployment(self, deployment_crn: str) -> dict:
        result = self.df("describe-deployment", "--deployment-crn", deployment_crn)

        if "error" in result:
            return result

        return result.get("deployment", result)

    def deployment_facts(self, deployment_crn: str) -> dict:
        """
        Everything one describe-deployment tells us. Never raises; on a
        failed read the state is UNKNOWN, which callers treat as "do not act".
        """

        deployment = self.describe_deployment(deployment_crn)

        if "error" in deployment:
            return {
                "state": "UNKNOWN",
                "name": None,
                "flow_version": None,
                "flow_version_crn": None,
                "flow_crn": None,
                "updated": None,
                "cluster_size": None,
                "node_count": None,
                "alerts": {},
                "error": deployment.get("error"),
            }

        return {
            "state": (deployment.get("status") or {}).get("state") or "UNKNOWN",
            "name": deployment.get("name"),
            "flow_version": deployment.get("flowVersion"),
            "flow_version_crn": deployment.get("flowVersionCrn"),
            "flow_crn": deployment.get("flowCrn"),
            "updated": deployment.get("updated"),
            "cluster_size": deployment.get("clusterSize"),
            "node_count": deployment.get("currentNodeCount")
            or deployment.get("staticNodeCount"),
            "alerts": {
                "info": deployment.get("activeInfoAlertCount") or 0,
                "warning": deployment.get("activeWarningAlertCount") or 0,
                "error": deployment.get("activeErrorAlertCount") or 0,
            },
            "error": None,
        }

    def deployed_flow_crn(self, deployment_crn: str) -> str:
        """The deployed-flow CRN, or "" before the flow has deployed."""

        result = self.df("list-flows-in-deployment", "--deployment-crn", deployment_crn)

        if "error" in result:
            return ""

        flows = result.get("deployedFlows") or []

        return (flows[0].get("crn") or "") if flows else ""

    def flow_facts(self, deployment_crn: str, deployed_flow_crn: str = "") -> dict:
        """
        Read the deployed flow, which is where a version swap shows: the
        deployment's own state does not move during one.
        """

        def empty(reason):
            return {
                "crn": deployed_flow_crn or None,
                "flow_version": None,
                "flow_version_crn": None,
                "state": "UNKNOWN",
                "detailed_state": None,
                "message": None,
                "valid_actions": [],
                "error": reason,
            }

        if not deployed_flow_crn:
            deployed_flow_crn = self.deployed_flow_crn(deployment_crn)

            if not deployed_flow_crn:
                return empty("the deployment has no deployed flow yet")

        result = self.df(
            "describe-flow-in-deployment",
            "--deployment-crn", deployment_crn,
            "--deployed-flow-crn", deployed_flow_crn,
        )

        if "error" in result:
            return empty(result.get("error"))

        # The response key is `deployedFlow`, not `flow`.
        flow = result.get("deployedFlow") or {}
        status = flow.get("status") or {}

        return {
            "crn": flow.get("crn"),
            "flow_version": flow.get("flowVersion"),
            "flow_version_crn": flow.get("flowVersionCrn"),
            "state": status.get("state") or "UNKNOWN",
            "detailed_state": status.get("detailedState"),
            "message": status.get("message"),
            "valid_actions": list(flow.get("validActions") or []),
            "error": None,
        }

    def wait_for_steady_state(self, deployment_crn: str, deadline_seconds: int,
                              verbose: bool = True, poll_seconds: int = 20) -> str:
        """
        Poll until the deployment is steady or the deadline passes.

        `df create-deployment` and the configuration writes return before
        the work is done. Returns the last state seen; the caller decides.
        """

        started = self.now()
        last = None

        while True:
            state = self.deployment_facts(deployment_crn)["state"]

            if state != last:
                if verbose:
                    print(f"  [{int(self.now() - started):>5}s] {state}", flush=True)

                last = state

            if state in STEADY_STATES:
                return state

            if self.now() - started > deadline_seconds:
                return state

            self.sleep(poll_seconds)

    # -- metrics ------------------------------------------------------

    def metric_charts(self, deployment_crn: str, deployed_flow_crn: str = "",
                      time_period: str = "", system: bool = True) -> dict:
        """
        Read and merge the chart surfaces.

        A KPI written through update-flow-in-deployment shows up on the
        flow surface; deployment KPIs on the other. Which surface a given
        KPI lands on is undocumented, so both are read and merged.
        MetricChart has no ids: (componentType, componentName, name) is
        the only identity a chart has, and the dedup key.

        Returns {"charts": [...], "sources": {...}}; `sources` records
        each surface's outcome so a failed read is not mistaken for an
        absent chart.
        """

        period = time_period or config.METRICS_TIME_PERIOD
        charts, seen, sources = [], set(), {}

        reads = [(
            "list-deployment-kpis",
            ["list-deployment-kpis", "--deployment-crn", deployment_crn,
             "--metrics-time-period", period],
        )]

        if deployed_flow_crn:
            reads.append((
                "list-flow-kpis-in-deployment",
                ["list-flow-kpis-in-deployment", "--deployment-crn", deployment_crn,
                 "--deployed-flow-crn", deployed_flow_crn,
                 "--metrics-time-period", period],
            ))

        else:
            sources["list-flow-kpis-in-deployment"] = "skipped: no deployed-flow CRN"

        if system:
            reads.append((
                "list-deployment-system-metrics",
                ["list-deployment-system-metrics", "--deployment-crn", deployment_crn,
                 "--metrics-time-period", period],
            ))

        for name, arguments in reads:
            result = self.df(*arguments)

            if "error" in result:
                sources[name] = f"error: {result.get('error')}"

                continue

            found = result.get("metricCharts") or []
            sources[name] = f"ok: {len(found)} chart(s)"

            for chart in found:
                key = (chart.get("componentType"), chart.get("componentName"), chart.get("name"))

                if key in seen:
                    continue

                seen.add(key)
                chart = dict(chart)
                chart["_source"] = name
                charts.append(chart)

        return {"charts": charts, "sources": sources, "period": period,
                "read_at": self.now()}

    def active_alerts(self, deployment_crn: str) -> list:
        result = self.df("list-deployment-active-alerts", "--deployment-crn", deployment_crn)

        if "error" in result:
            return []

        return result.get("eventSummaries") or []

    def deployment_events(self, deployment_crn: str, max_items: int = 200) -> list:
        """
        The deployment's event log, the flow's events included, newest
        first: {crn, name, eventType, firstOccurrence (ms), ...}. An event
        names what changed ("Parameters Updated") but not the values.
        """

        result = self.df("list-deployment-events", "--deployment-crn", deployment_crn,
                         "--max-items", max_items)

        if "error" in result:
            return []

        return result.get("eventSummaries") or []

    # -- flow catalog -------------------------------------------------

    def get_flow_version(self, flow_version_crn: str) -> dict:
        return self.df("get-flow-version", "--flow-version-crn", flow_version_crn)

    def flow_definition(self, flow_version_crn: str) -> dict:
        """
        The flow definition of a catalog version, parsed. Returns the error
        dict on failure.

        `get-flow-version` is a CLI-side command: cdpcli base64-decodes the
        API's `flowDefinition` and prints the flow JSON itself, so
        `flowContents` is at the top level. The raw API shape (a base64 or
        JSON string under `flowDefinition`) is accepted too.
        """

        result = self.get_flow_version(flow_version_crn)

        if "error" in result:
            return result

        definition = result if "flowContents" in result else result.get("flowDefinition")

        if isinstance(definition, str):
            try:
                definition = json.loads(definition)

            except ValueError:
                try:
                    definition = json.loads(base64.b64decode(definition, validate=True))

                except ValueError:
                    return {"error": "flowDefinition is neither JSON nor base64 JSON.",
                            "command": flow_version_crn}

        if not isinstance(definition, dict) or "flowContents" not in definition:
            return {"error": "the response carries no flow definition with flowContents.",
                    "command": flow_version_crn}

        return definition

    def list_flow_definitions(self, search_term: str) -> list:
        # Bounded: the CLI's default page is 20, and auto-pagination emits
        # a plain-text warning ahead of the JSON.
        result = self.df("list-flow-definitions", "--search-term", search_term, "--max-items", 500)

        if "error" in result:
            return []

        return [f for f in (result.get("flows") or []) if isinstance(f, dict)]

    def list_flow_versions(self, flow_crn: str) -> dict:
        return self.df("list-flow-definition-versions", "--flow-crn", flow_crn)

    def newest_flow_version(self, flow_crn: str) -> dict:
        result = self.list_flow_versions(flow_crn)

        return max(
            result.get("flowVersions") or [],
            key=lambda v: v.get("version") or 0,
            default={},
        )

    # -- workload configuration ----------------------------------------

    def flow_configuration(self, deployment_crn: str, deployed_flow_crn: str) -> dict:
        result = self.dfw(
            "get-flow-configuration-in-deployment",
            "--deployment-crn", deployment_crn,
            "--deployed-flow-crn", deployed_flow_crn,
        )

        if "error" in result:
            return result

        return result.get("deployedFlowConfiguration") or {}

    def flow_configuration_metadata(self, deployment_crn: str, deployed_flow_crn: str) -> dict:
        result = self.dfw(
            "get-flow-configuration-metadata-in-deployment",
            "--deployment-crn", deployment_crn,
            "--deployed-flow-crn", deployed_flow_crn,
        )

        if "error" in result:
            return result

        return result.get("deployedFlowConfigurationMetadata") or {}

    def deployment_configuration_metadata(self, deployment_crn: str) -> dict:
        """The deployment-level KPI scopes (SYSTEM, NIFI_FLOW) and sizing options."""

        result = self.dfw("get-deployment-configuration-metadata", "--deployment-crn", deployment_crn)

        if "error" in result:
            return result

        return result.get("deploymentConfigurationMetadata") or {}

    def deployment_configuration(self, deployment_crn: str) -> dict:
        result = self.dfw("get-deployment-configuration", "--deployment-crn", deployment_crn)

        if "error" in result:
            return result

        return result.get("deploymentConfiguration") or {}

    # -- reachability -------------------------------------------------

    def workload_endpoint(self) -> str:
        """
        The host `dfworkload` commands talk to, plus the token expiry.

        The response also carries a bearer token. Only the host and the
        expiry are returned; the token is never printed or logged.
        """

        result = self.run([
            "iam", "generate-workload-auth-token",
            "--workload-name", "DF",
            "--environment-crn", config.ENVIRONMENT_CRN,
        ])

        if "error" in result:
            return f"UNRESOLVED ({result.get('error')})"

        url = result.get("endpointUrl") or ""
        host = urllib.parse.urlparse(url).hostname or url or "unknown"

        return f"{host} (token expires {result.get('expireAt')})"

    def workload_unreachable(self, timeout: float = 5.0) -> str:
        """
        "" if the workload endpoint accepts a TCP connection, else why not.

        A network probe, not an auth probe. It runs before any write
        because an import is control-plane and would succeed from outside
        the VPC, leaving a permanent catalog version that the following
        workload-plane swap then fails to deploy.
        """

        display = self.workload_endpoint()

        if not display or display.startswith("UNRESOLVED"):
            return (
                "the workload endpoint could not be determined, so "
                f"reachability cannot be established ({display or 'no answer'})"
            )

        # The first field only: the display string carries the expiry
        # after the host, and passing all of it to getaddrinfo would be
        # misreported as a DNS failure.
        host = display.split()[0]

        try:
            addresses = sorted({
                info[4][0] for info in socket.getaddrinfo(host, 443, socket.AF_INET)
            })

        except socket.gaierror as error:
            return f"{host} does not resolve ({error})"

        for address in addresses:
            probe = socket.socket()
            probe.settimeout(timeout)

            try:
                probe.connect((address, 443))

                return ""

            except OSError:
                continue

            finally:
                probe.close()

        return (
            f"{host} resolves to {addresses} and none accept a connection on "
            f"443 within {timeout:g}s. Run from inside the environment's VPC."
        )

    def selftest(self) -> dict:
        """Prove both API planes answer before anything is armed."""

        report = {"client": self.kind, "service_crn": config.SERVICE_CRN,
                  "environment_crn": config.ENVIRONMENT_CRN}

        control = self.df("list-deployments", "--max-items", 500)

        if "error" in control:
            report["control_plane"] = {"ok": False, "error": control.get("error"),
                                       "hint": control.get("hint")}

        else:
            report["control_plane"] = {"ok": True,
                                       "deployments": len(control.get("deployments") or [])}

        workload = self.dfw("list-nifi-versions")

        if "error" in workload:
            report["workload_plane"] = {"ok": False, "error": workload.get("error"),
                                        "hint": workload.get("hint")}

        else:
            versions = workload.get("nifiVersions") or []
            names = [v if isinstance(v, str) else (v.get("version") or v.get("name") or "")
                     for v in versions]
            report["workload_plane"] = {
                "ok": True,
                "nifi_versions": len(versions),
                "configured_nifi_version_available": config.NIFI_VERSION in names,
            }

        report["workload_endpoint"] = self.workload_endpoint()
        report["ok"] = report["control_plane"]["ok"] and report["workload_plane"]["ok"]

        return report


class _WorkerDied(Exception):
    pass


class _Worker:
    """One cdp/worker.py process: one call at a time, from the pool."""

    def __init__(self, environment: dict, timeout: float):
        self.calls = 0
        self.environment = environment

        try:
            self.proc = subprocess.Popen([sys.executable, WORKER_SCRIPT], stdin=subprocess.PIPE,
                                         stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                         text=True, env=environment)

        except OSError as exc:
            raise _WorkerDied(str(exc)) from None

        try:
            ready = self._read(timeout).get("ready")

        except (_WorkerDied, subprocess.TimeoutExpired, ValueError, AttributeError):
            ready = False

        if not ready:
            self.kill()
            raise _WorkerDied("the worker did not start")

    def _read(self, timeout: float) -> dict:
        # poll, not select: select refuses a descriptor numbered 1024 or more, and a busy app has that many open.
        waiting = select.poll()
        waiting.register(self.proc.stdout, select.POLLIN)

        if not waiting.poll(timeout * 1000):
            raise subprocess.TimeoutExpired("cdp worker", timeout)

        line = self.proc.stdout.readline()

        if not line:
            raise _WorkerDied("the worker exited")

        return json.loads(line)

    def call(self, args: list, timeout: float) -> subprocess.CompletedProcess:
        try:
            self.proc.stdin.write(json.dumps({"args": [str(a) for a in args]}) + "\n")
            self.proc.stdin.flush()

        except (BrokenPipeError, OSError, ValueError) as exc:
            raise _WorkerDied(str(exc)) from None

        reply = self._read(timeout)
        self.calls += 1

        return subprocess.CompletedProcess(args, reply["rc"], reply["stdout"], reply["stderr"])

    def kill(self) -> None:
        try:
            self.proc.kill()
            self.proc.wait(5)

        except (OSError, subprocess.TimeoutExpired):
            pass

        for stream in (self.proc.stdin, self.proc.stdout):
            try:
                stream.close()

            except OSError:
                pass


class WorkerPool:
    """
    Up to `size` long-lived cdp workers for reads, started on demand. A
    worker that times out is killed (as the one-shot process would be);
    one that dies is replaced and the call tried once more on a fresh
    one; each is recycled after WORKER_CALLS calls, or when the
    credentials it was started with are no longer the current ones.
    """

    def __init__(self, size: int):
        self.size = size
        self._idle = []
        self._alive = 0
        self._cond = threading.Condition()

    def _take(self, environment: dict, timeout: float) -> _Worker:
        stale = []

        with self._cond:
            # Every worker busy for a whole timeout: the call gets its own process instead.
            if not self._cond.wait_for(lambda: self._idle or self._alive < self.size, timeout):
                raise _WorkerDied("every worker is busy")

            while self._idle and self._idle[-1].environment != environment:
                stale.append(self._idle.pop())

            self._alive -= len(stale)
            worker = self._idle.pop() if self._idle else None

            if worker is None:
                self._alive += 1

        for old in stale:
            old.kill()

        if worker:
            return worker


        try:
            return _Worker(environment, timeout)

        except BaseException:
            self._drop()
            raise

    def _drop(self) -> None:
        with self._cond:
            self._alive -= 1
            self._cond.notify()

    def _give_back(self, worker: _Worker) -> None:
        if worker.calls >= WORKER_CALLS:
            worker.kill()
            self._drop()
            return

        with self._cond:
            self._idle.append(worker)
            self._cond.notify()

    def run(self, args: list, environment: dict, timeout: float) -> subprocess.CompletedProcess:
        """The call's exit as a CompletedProcess; raises TimeoutExpired, or _WorkerDied twice over."""

        for attempt in (1, 2):
            worker = self._take(environment, timeout)

            try:
                completed = worker.call(args, timeout)

            except subprocess.TimeoutExpired:
                worker.kill()
                self._drop()
                raise

            except (_WorkerDied, ValueError, KeyError) as exc:
                worker.kill()
                self._drop()

                if attempt == 2:
                    raise _WorkerDied(str(exc)) from None

                continue

            self._give_back(worker)

            return completed

    def close(self) -> None:
        with self._cond:
            idle, self._idle = self._idle, []
            self._alive -= len(idle)

        for worker in idle:
            worker.kill()


def is_read(args: list) -> bool:
    """A describe/list/get operation, which the worker pool may run."""

    return len(args) > 1 and str(args[1]).startswith(READ_PREFIXES)


class LiveClient(Client):
    """
    Runs the CDP CLI: reads on a pool of long-lived workers
    (SELFHEAL_CDP_WORKERS, 0 for none), every write in its own
    subprocess, as each read did before the pool.
    """

    kind = "live"

    def __init__(self):
        self.pool = WorkerPool(config.CDP_WORKERS) if config.CDP_WORKERS > 0 else None

    def _environment(self) -> dict:
        """
        Environment variables win; else let cdpcli read ~/.cdp/credentials.
        A half-set pair is removed rather than passed, because it would
        override the file with something incomplete.
        """

        environment = dict(os.environ)

        if environment.get("CDP_ACCESS_KEY_ID") and environment.get("CDP_PRIVATE_KEY"):
            return environment

        if os.path.exists(CDP_CREDENTIALS_FILE):
            environment.pop("CDP_ACCESS_KEY_ID", None)
            environment.pop("CDP_PRIVATE_KEY", None)

            return environment

        return {}

    def run(self, args: list) -> dict:
        started = time.monotonic()
        result = self._run(args)
        telemetry.record("cdp", time.monotonic() - started, not (isinstance(result, dict) and "error" in result))

        return result

    def _invoke(self, args: list, environment: dict) -> subprocess.CompletedProcess:
        if self.pool and is_read(args):
            try:
                return self.pool.run(list(args) + ["--output", "json"], environment, config.CDP_TIMEOUT_SECONDS)

            except _WorkerDied:
                pass         # the one-shot process says why, with its hint

        # Through `-c` rather than `-m cdpcli.clidriver`: the module has no
        # __main__ guard, so `-m` is a silent no-op that exits 0.
        command = [
            sys.executable, "-c",
            "import sys; from cdpcli.clidriver import main; sys.exit(main())",
        ] + [str(a) for a in args] + ["--output", "json"]

        return subprocess.run(command, capture_output=True, text=True,
                              timeout=config.CDP_TIMEOUT_SECONDS, env=environment)

    def _run(self, args: list) -> dict:
        environment = self._environment()

        if not environment:
            return {
                "error": "No CDP credentials found.",
                "command": command_line(args),
                "hint": (
                    "Set CDP_ACCESS_KEY_ID and CDP_PRIVATE_KEY as project "
                    f"environment variables, or configure {CDP_CREDENTIALS_FILE}."
                ),
            }

        try:
            completed = self._invoke(args, environment)

        except subprocess.TimeoutExpired:
            return {
                "error": f"The CDP CLI did not complete within {config.CDP_TIMEOUT_SECONDS} seconds.",
                "command": command_line(args),
            }

        except OSError as exc:
            return {
                "error": "The CDP CLI could not be executed.",
                "command": command_line(args),
                "detail": f"{type(exc).__name__}: {exc}",
            }

        stderr = (completed.stderr or "").strip()[:MAX_RAW_OUTPUT_CHARS]

        if completed.returncode != 0:
            return {
                "error": "The CDP CLI returned an error.",
                "command": command_line(args),
                "return_code": completed.returncode,
                "stderr": stderr,
                "hint": hint_for(completed.stderr or "", completed.returncode),
            }

        stdout = (completed.stdout or "").strip()

        if not stdout:
            # Some mutations legitimately return nothing; the CLI exiting 0
            # is the success signal there.
            return {"_empty": True, "stderr": stderr}

        # Auto-paginating operations prefix their JSON with a plain-text
        # warning, which json.loads rejects as "Extra data".
        brace = stdout.find("{")

        if brace > 0:
            stdout = stdout[brace:]

        try:
            return json.loads(stdout)

        except ValueError:
            return {
                "error": "The CDP CLI returned output that was not JSON.",
                "command": command_line(args),
                "raw_output": stdout[:MAX_RAW_OUTPUT_CHARS],
                "stderr": stderr,
            }


def hint_for(stderr: str, return_code: int = None) -> str:
    """Translate a known CDP CLI failure into an actionable sentence."""

    if "No module named 'cdpcli'" in stderr:
        return "cdpcli is not installed for this interpreter: pip install cdpcli==0.9.164"

    # cdpcli reads CDP_PRIVATE_KEY as a path first, so a malformed key is
    # reported as a missing file.
    if "Private key file" in stderr and "does not exist" in stderr:
        return (
            "CDP_PRIVATE_KEY is read as a file path first and as a literal key "
            "only if no such file exists, so this also appears when the key is "
            "malformed or truncated. Supply the complete key, newlines included."
        )

    if "Access Denied" in stderr or "403" in stderr:
        return (
            "This identity lacks the DataFlow permission this operation needs on "
            "this environment. Workload configuration operations and deployment "
            "mutations are gated separately from metrics reads."
        )

    usage_markers = (
        "Unknown options", "unrecognized arguments", "Invalid choice",
        "invalid choice", "arguments are required", "argument operation",
        "Unknown command",
    )

    # A usage error never reached the API; don't send anyone to check keys.
    if return_code == 2 or any(marker in stderr for marker in usage_markers):
        return (
            "A usage error from the CLI's own argument parsing, not an auth "
            "failure. Check the operation and flags with `cdp df <op> --help`."
        )

    if "timed out" in stderr.lower() or "connecttimeout" in stderr.lower():
        return (
            "A timeout talking to the workload plane usually means this is not "
            "running inside the environment's VPC."
        )

    return (
        "Confirm CDP_ACCESS_KEY_ID and CDP_PRIVATE_KEY belong to an active CDP "
        "API key with a DataFlow role on this environment."
    )


_CLIENT = None


def get_client() -> Client:
    """The process-wide client, chosen by SELFHEAL_DF_CLIENT."""

    global _CLIENT

    if _CLIENT is None:
        if config.DF_CLIENT == "live":
            _CLIENT = LiveClient()

        else:
            from nifi_healer.cdp.mock_client import MockClient

            _CLIENT = MockClient()

    return _CLIENT


def set_client(client: Client) -> None:
    """For tests and scripts that construct their own client."""

    global _CLIENT
    _CLIENT = client
