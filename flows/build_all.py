"""
Build every pack's flow definition into flows/out/<pack>.json.

    python flows/build_all.py              # build and write
    python flows/build_all.py --check      # build, check, and fail if
                                           # flows/out is stale

Checks, beyond the builder's structural ones:

  * every KPI names a component that exists, of the right kind
  * KPI component names are unique within a pack, because a chart is
    identified by (componentType, componentName, metric) and nothing else
  * every expected/acceptable action targets a component that exists
  * every connection and processor with a KPI has an explicit name
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config  # noqa: E402
from flows import packs  # noqa: E402
from flows.builder.lib import check, iter_connections, iter_processors  # noqa: E402


def names_by_kind(definition: dict) -> dict:
    names = {"connection": [], "processor": []}

    for _, _, connection in iter_connections(definition):
        names["connection"].append(connection["name"])

    for _, _, processor in iter_processors(definition):
        names["processor"].append(processor["name"])

    return names


def check_pack(pack, definition: dict) -> list:
    problems = []
    names = names_by_kind(definition)
    everything = set(names["connection"]) | set(names["processor"])

    for kind in ("connection", "processor"):
        seen = set()

        for name in names[kind]:
            if name and name in seen:
                problems.append(f"two {kind}s named {name!r}: their charts would be indistinguishable")

            seen.add(name)

    for kpi in pack.kpis:
        if kpi.kind == "system":
            continue

        if not kpi.component:
            problems.append(f"{kpi.scenario}: a {kpi.kind} KPI with no component name")

        elif kpi.component not in names[kpi.kind]:
            problems.append(f"{kpi.scenario}: KPI names {kpi.kind} {kpi.component!r}, which does not exist")

    for scenario in pack.scenarios:
        for proposal in scenario.expected + scenario.acceptable:
            target = proposal.get("target")

            if proposal["action"] in ("NO_ACTION", "SCALE_DEPLOYMENT", "UPDATE_PARAMETER"):
                continue

            if target not in everything:
                problems.append(f"{scenario.id}: {proposal['action']} targets {target!r}, which does not exist")

            dlq = proposal.get("dlq")

            if dlq and dlq not in names["processor"]:
                problems.append(f"{scenario.id}: DLQ {dlq!r} does not exist")

        for proposal in scenario.expected:
            if proposal["action"] == "UPDATE_PARAMETER" and proposal.get("parameter") not in pack.parameters:
                problems.append(f"{scenario.id}: parameter {proposal.get('parameter')!r} is not declared")

    return problems


def render(definition: dict) -> str:
    return json.dumps(definition, indent=2, sort_keys=True) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--check", action="store_true",
                        help="check every pack and fail if flows/out differs from a fresh build")
    parser.add_argument("--pack", action="append", help="only this pack (repeatable)")
    args = parser.parse_args()

    os.makedirs(config.FLOWS_OUT_DIR, exist_ok=True)
    failed = False
    unverified = set()

    for name in args.pack or packs.PACKS:
        pack = packs.PACKS[name]
        definition = packs.build(name)
        report = check(definition)
        problems = report["problems"] + check_pack(pack, definition)
        unverified.update(report["unverified"])
        path = os.path.join(config.FLOWS_OUT_DIR, f"{name}.json")
        text = render(definition)

        if args.check:
            current = open(path).read() if os.path.exists(path) else None

            if current != text:
                problems.append(f"{path} is stale; run python flows/build_all.py")

        else:
            with open(path, "w") as handle:
                handle.write(text)

        status = "FAIL" if problems else "ok"
        print(f"[{status}] {packs.flow_name(name):28} {len(pack.scenarios)} scenario(s), "
              f"{len(pack.kpis)} KPI(s), {pack.cluster_size} x{pack.nodes}")

        for problem in problems:
            print(f"    - {problem}")

        failed = failed or bool(problems)

    if unverified:
        print("\nnot yet confirmed against a live NiFi 2.6 (confirmed by provision/01 --canary):")

        for item in sorted(unverified):
            print(f"    {item}")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
