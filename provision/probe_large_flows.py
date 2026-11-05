#!/usr/bin/env python3
"""
NFH-56: how well flow definitions over 64 KiB read, tested at scale.

    python provision/probe_large_flows.py            # import, test, delete
    python provision/probe_large_flows.py --keep     # leave the flows, for reruns
    python provision/probe_large_flows.py --reuse    # test the flows a --keep run left
    python provision/probe_large_flows.py --cleanup  # only delete them
    python provision/probe_large_flows.py --sizes 65536,100000 --requests 200 --concurrency 6,12,24

It imports catalog flows named <prefix>probe-*:

    <n>b        a real pack's flow (p1), padded with process-group
                comments to exactly n bytes, as flows/build_all.py lays
                files out: around the cut and well above it
    utf8-split  one whose byte 65,536 falls inside a two-byte character
    p11, p20    the real large packs

and then tests:

    correctness  get-flow-version, whole or cut; flow_definition, equal
                 to what was sent, and whether through the CLI or the
                 console download
    versions     a second version of one flow; each must read back as
                 itself, not as the other
    load         sequential console downloads, then the same number at
                 each concurrency: latency, throughput, errors
    failures     a wrong version id and a missing version, which must
                 fail legibly

The report goes to stdout and to provision/out/probe_large_flows_<time>.json.
Credentials are whatever the app would use: CDP_ACCESS_KEY_ID and
CDP_PRIVATE_KEY, else ~/.cdp/credentials (CDP_PROFILE picks the profile).
Rerun with another key or on CAI to test those: from a session terminal,
or as a job (script cai_job.py, arguments provision/probe_large_flows.py,
SELFHEAL_DF_CLIENT=live), where the project's CDP_* settings are the key.
It first checks the console takes a signed request, and stops before
importing anything if not. Writes only its own probe flows, and deletes
them unless --keep.
"""

import argparse
import collections
import concurrent.futures
import copy
import json
import os
import platform
import statistics
import sys
import tempfile
import time

import _common
from _common import config, packs

from nifi_healer.cdp.client import CONSOLE_FLOW_VERSION_PATH, is_cut_short, parse_flow_definition

CUT_BYTES = 65536
DEFAULT_SIZES = "60000,65535,65536,65537,100000,500000,2000000"
BASE_PACK = "p1-throughput"
REAL_PACKS = {"p11": "p11-payments", "p20": "p20-big-healthy"}
VERSIONED_CASE = "100000b"
FILLER = "NFH-56 probe padding. "
FILLER_V2 = "NFH-56 probe padding, version 2. "
MISSING_ID = "00000000-0000-0000-0000-000000000000"

# A load phase stops after this many errors, so a throttled or failing
# console is not hammered for the rest of the run.
MAX_PHASE_ERRORS = 10


def render(definition: dict, ascii_only: bool = True) -> bytes:
    """A definition as flows/build_all.py writes it."""

    return (json.dumps(definition, indent=2, sort_keys=True, ensure_ascii=ascii_only) + "\n").encode()


def sized(base: dict, target: int, filler: str = FILLER) -> dict:
    """`base`, padded with process-group comments to exactly `target` bytes."""

    definition = copy.deepcopy(base)
    definition["flowContents"]["comments"] = ""
    short = target - len(render(definition))

    if short < 0:
        raise SystemExit(f"--sizes: {target} is smaller than {BASE_PACK}'s own {target - short} bytes")

    definition["flowContents"]["comments"] = (filler * (short // len(filler) + 1))[:short]
    assert len(render(definition)) == target

    return definition


def split_at_cut(base: dict) -> dict:
    """`base`, padded so that byte CUT_BYTES is the second byte of an "é"."""

    definition = copy.deepcopy(base)
    definition["flowContents"]["comments"] = "NFHPROBEMARK"
    start = render(definition, ascii_only=False).index(b"NFHPROBEMARK")
    ascii_run = CUT_BYTES - 1 - start
    definition["flowContents"]["comments"] = (FILLER * (ascii_run // len(FILLER) + 1))[:ascii_run] + "é" * 1000
    assert render(definition, ascii_only=False)[CUT_BYTES - 1:CUT_BYTES + 1] == "é".encode()

    return definition


def cases(sizes: list) -> list:
    """[(label, definition, ascii_only)], built the same way on every run, so --reuse can compare."""

    base = packs.build(BASE_PACK)
    found = [(f"{size}b", sized(base, size), True) for size in sizes]
    found.append(("utf8-split", split_at_cut(base), False))
    found += [(label, packs.build(pack), True) for label, pack in REAL_PACKS.items()]

    return found


def flow_name(label: str) -> str:
    return f"{config.PREFIX}probe-{label}"


def probe_flows(client) -> dict:
    """{name: flow crn} of the probe flows in the catalog."""

    prefix = flow_name("")

    return {f["name"]: f["crn"] for f in client.list_flow_definitions(prefix)
            if (f.get("name") or "").startswith(prefix)}


def write(directory: str, label: str, definition: dict, ascii_only: bool) -> str:
    path = os.path.join(directory, f"{label}.json")

    with open(path, "wb") as handle:
        handle.write(render(definition, ascii_only))

    return path


def versions_of(client, flow_crn: str) -> dict:
    """{version number: version crn}."""

    result = client.list_flow_versions(flow_crn)

    return {v["version"]: v["crn"] for v in result.get("flowVersions") or [] if isinstance(v, dict)}


def read(client, version_crn: str, sent: dict) -> dict:
    """One version through get-flow-version, then through flow_definition."""

    started = time.monotonic()
    raw = client.get_flow_version(version_crn)
    cli_ms = (time.monotonic() - started) * 1000

    if "error" in raw:
        cli = "cut" if is_cut_short(raw) else "error"

    else:
        cli = "whole" if parse_flow_definition(raw, version_crn) == sent else "differs"

    started = time.monotonic()
    got = client.flow_definition(version_crn)
    total_ms = (time.monotonic() - started) * 1000

    if "error" in got:
        how = "FAILED"

    elif got != sent:
        how = "DIFFERS"

    else:
        how = "cli" if cli == "whole" else "console"

    return {
        "get_flow_version": cli,
        # cdpcli's own message: "(char N)" is where the decoder gave up,
        # which for an unterminated string is where that string began.
        "cli_error": (raw.get("stderr") or raw.get("error"))[:200] if "error" in raw else None,
        "flow_definition": how,
        "error": {k: got[k] for k in ("error", "stderr", "detail") if k in got} if how == "FAILED" else None,
        "cli_ms": round(cli_ms),
        "flow_definition_ms": round(total_ms),
    }


def load_phase(client, path: str, sent: dict, requests: int, concurrency: int) -> dict:
    """`requests` console downloads, `concurrency` at a time."""

    def one(_):
        started = time.monotonic()
        result = client.console_get(path)
        seconds = time.monotonic() - started

        if "error" in result:
            return seconds, result["error"]

        return seconds, None if result == sent else "the download differs from what was sent"

    timings, errors, started = [], collections.Counter(), time.monotonic()

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(one, i) for i in range(requests)]

        for future in concurrent.futures.as_completed(futures):
            seconds, error = future.result()
            timings.append(seconds)

            if error:
                errors[error] += 1

                if sum(errors.values()) >= MAX_PHASE_ERRORS:
                    for pending in futures:
                        pending.cancel()

                    break

    elapsed = time.monotonic() - started
    ms = sorted(t * 1000 for t in timings)

    return {
        "concurrency": concurrency,
        "requests": len(timings),
        "errors": dict(errors),
        "stopped_early": len(timings) < requests,
        "p50_ms": round(statistics.median(ms)) if ms else None,
        "p95_ms": round(ms[max(0, int(len(ms) * 0.95) - 1)]) if ms else None,
        "max_ms": round(ms[-1]) if ms else None,
        "per_second": round(len(timings) / elapsed, 2) if elapsed else None,
    }


def signer_name() -> str:
    if config.MOCK:
        return "mock"

    from nifi_healer.cdp.client import LiveClient

    try:
        signer = LiveClient._signer()

    except Exception as exc:
        return f"unusable ({type(exc).__name__})"

    return type(signer).__name__ if signer else "none"


def cleanup(client) -> list:
    """Delete every probe flow; returns the names that could not be deleted."""

    failed = []

    for name, crn in sorted(probe_flows(client).items()):
        result = client.df("delete-flow", "--flow-crn", crn)
        print(f"  {'deleted' if 'error' not in result else 'COULD NOT DELETE'} {name}")

        if "error" in result:
            failed.append(name)

    return failed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--sizes", default=DEFAULT_SIZES, help=f"padded flow sizes in bytes (default {DEFAULT_SIZES})")
    parser.add_argument("--requests", type=int, default=50, help="console downloads per load phase (default 50)")
    parser.add_argument("--concurrency", default="6,12", help="concurrent load phases after the sequential one")
    parser.add_argument("--load-case", default="500000b", help="the flow the load phases download (default 500000b)")
    parser.add_argument("--keep", action="store_true", help="leave the probe flows in the catalog")
    parser.add_argument("--reuse", action="store_true", help="test the probe flows already in the catalog")
    parser.add_argument("--cleanup", action="store_true", help="only delete the probe flows")
    parser.add_argument("--out", help="where the JSON report goes (default provision/out/probe_large_flows_<time>.json)")
    args = parser.parse_args()

    if not config.PREFIX:
        raise SystemExit("error: SELFHEAL_PREFIX is empty; probe flows must carry the prefix.")

    client = _common.Tenant().client
    report = {
        "started": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "client": config.DF_CLIENT,
        "console": config.CDP_CONSOLE_URL,
        "signer": signer_name(),
        "profile": os.environ.get("CDP_DEFAULT_PROFILE") or os.environ.get("CDP_PROFILE") or "default",
        "key_from": "env" if os.environ.get("CDP_ACCESS_KEY_ID") and os.environ.get("CDP_PRIVATE_KEY") else "file",
        "on_cai": bool(os.environ.get("CDSW_PROJECT")),
        "host": platform.node(),
        "python": platform.python_version(),
        # urllib, unlike cdpcli, takes its proxy from these and its CAs from the system store.
        "proxy": [k for k in ("HTTPS_PROXY", "https_proxy", "NO_PROXY", "no_proxy") if os.environ.get(k)],
        "ca_bundle": [k for k in ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE", "AWS_CA_BUNDLE") if os.environ.get(k)],
    }
    print("NFH-56 large-flow probe\n" + "".join(f"  {k}: {v}\n" for k, v in report.items()), flush=True)

    if args.cleanup:
        return 1 if cleanup(client) else 0

    # A signed GET of no version: the console answers 500 "Failed to Read
    # Vault Key" once it is reachable and has taken the key. Anything else
    # (unreachable, a TLS error, 401) fails every fallback, so stop here.
    check = client.console_get(CONSOLE_FLOW_VERSION_PATH.format(version_id=MISSING_ID))
    report["console_check"] = {k: str(check.get(k))[:300] for k in ("error", "detail") if k in check}
    print(f"console check: {report['console_check']}\n", flush=True)

    if not config.MOCK and "Vault Key" not in (check.get("detail") or ""):
        raise SystemExit("error: the console did not answer a signed request as expected; see the console "
                         "check above. Nothing was imported.")

    sizes = [int(s) for s in args.sizes.split(",") if s.strip()]
    built = cases(sizes)
    existing = probe_flows(client)

    if existing and not args.reuse:
        raise SystemExit(f"error: {len(existing)} probe flow(s) are already in the catalog. "
                         "Rerun with --reuse to test them, or --cleanup to delete them.")

    # -- import -------------------------------------------------------

    flows, results = {}, {}
    directory = tempfile.mkdtemp(prefix="nfh56-")
    print("import", flush=True)

    for label, definition, ascii_only in built:
        name, size = flow_name(label), len(render(definition, ascii_only))
        results[label] = {"bytes": size}

        if name in existing:
            flows[label] = existing[name]
            print(f"  {label:>12}  {size:>9} B  reused")
            continue

        started = time.monotonic()
        result = client.df("import-flow-definition", "--file", write(directory, label, definition, ascii_only),
                           "--name", name, "--description", "NFH-56 probe; deleted by the same script.",
                           "--comments", "v1")
        seconds = time.monotonic() - started

        if "error" in result:
            results[label]["import_error"] = (result.get("stderr") or result["error"])[:300]
            print(f"  {label:>12}  {size:>9} B  IMPORT FAILED  {results[label]['import_error'][:150]}")
            continue

        flows[label] = result["crn"]
        results[label]["import_ms"] = round(seconds * 1000)
        print(f"  {label:>12}  {size:>9} B  imported in {seconds:.1f}s")

    versioned = None

    if VERSIONED_CASE in flows:
        versioned = sized(packs.build(BASE_PACK), int(VERSIONED_CASE[:-1]), FILLER_V2)

        if 2 not in versions_of(client, flows[VERSIONED_CASE]):
            result = client.df("import-flow-definition-version", "--flow-crn", flows[VERSIONED_CASE],
                               "--file", write(directory, VERSIONED_CASE + "-v2", versioned, True),
                               "--comments", "v2")
            print(f"  {VERSIONED_CASE:>12}  v2  {'IMPORT FAILED' if 'error' in result else 'imported'}")

    # -- correctness and versions ------------------------------------

    print("\ncorrectness", flush=True)
    sent = {label: definition for label, definition, _ in built}

    for label in [label for label, _, _ in built if label in flows]:
        numbers = versions_of(client, flows[label])
        expected = {1: sent[label], 2: versioned} if label == VERSIONED_CASE else {1: sent[label]}
        results[label]["versions"] = {}

        for number, definition in expected.items():
            if number not in numbers:
                results[label]["versions"][number] = {"flow_definition": "MISSING"}
                continue

            outcome = read(client, numbers[number], definition)
            results[label]["versions"][number] = outcome
            print(f"  {label:>12} v{number}  get-flow-version {outcome['get_flow_version']:<6}"
                  f"  flow_definition {outcome['flow_definition']:<8} {outcome['flow_definition_ms']:>6} ms"
                  + (f"  {outcome['cli_error']}" if outcome["cli_error"] else "")
                  + (f"  {outcome['error']}" if outcome["error"] else ""))

    # -- load ---------------------------------------------------------

    report["load"] = []

    if args.load_case in flows and args.requests > 0:
        version_crn = versions_of(client, flows[args.load_case]).get(1)
        version_id = client.flow_version_id(version_crn) if version_crn else ""
        path = CONSOLE_FLOW_VERSION_PATH.format(version_id=version_id)
        print(f"\nload: {args.load_case}, {args.requests} console downloads per phase", flush=True)

        for concurrency in [1] + [int(c) for c in args.concurrency.split(",") if c.strip()]:
            phase = load_phase(client, path, sent[args.load_case], args.requests, concurrency)
            report["load"].append(phase)
            print(f"  ×{concurrency:<3} {phase['requests']:>4} requests  p50 {phase['p50_ms']} ms  "
                  f"p95 {phase['p95_ms']} ms  max {phase['max_ms']} ms  {phase['per_second']}/s"
                  f"  errors {phase['errors'] or 0}" + ("  STOPPED EARLY" if phase["stopped_early"] else ""),
                  flush=True)

    else:
        print(f"\nload: skipped ({args.load_case} was not imported)")

    # -- failures -----------------------------------------------------

    print("\nfailures", flush=True)
    wrong_id = client.console_get(CONSOLE_FLOW_VERSION_PATH.format(version_id=MISSING_ID))
    any_flow = next(iter(flows.values()), "")
    missing = client.flow_definition(f"{any_flow}/v.999") if any_flow else {"error": "no flow imported"}
    report["failures"] = {
        "wrong_version_id": {k: str(wrong_id.get(k))[:200] for k in ("error", "detail") if k in wrong_id}
                            or "DID NOT FAIL",
        "missing_version": {k: str(missing.get(k))[:200] for k in ("error", "stderr", "detail") if k in missing}
                           or "DID NOT FAIL",
    }

    for name, outcome in report["failures"].items():
        print(f"  {name}: {outcome}")

    # -- summary ------------------------------------------------------

    report["cases"] = results
    reads = [v for r in results.values() for v in (r.get("versions") or {}).values()]
    bad = [f"{label} v{n}" for label, r in results.items()
           for n, v in (r.get("versions") or {}).items() if v["flow_definition"] not in ("cli", "console")]
    report["summary"] = {
        "reads": len(reads),
        "through_cli": sum(1 for v in reads if v["flow_definition"] == "cli"),
        "through_console": sum(1 for v in reads if v["flow_definition"] == "console"),
        "bad": bad,
        "import_failures": [label for label, r in results.items() if "import_error" in r],
        "load_errors": sum(sum(p["errors"].values()) for p in report["load"]),
    }
    print("\nsummary\n" + "".join(f"  {k}: {v}\n" for k, v in report["summary"].items()))

    if not args.keep:
        print("cleanup")
        report["undeleted"] = cleanup(client)

    out = args.out or os.path.join(os.path.dirname(os.path.abspath(__file__)), "out",
                                   f"probe_large_flows_{time.strftime('%Y%m%d-%H%M%S')}.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)

    with open(out, "w") as handle:
        json.dump(report, handle, indent=2)

    print(f"\nreport: {out}")

    return 1 if bad or report["summary"]["load_errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
