"""
The scenario packs, and how a pack becomes a flow definition.

    from flows import packs
    packs.PACKS["p1-throughput"]         # Pack
    packs.build("p1-throughput")         # flow definition dict
    packs.scenario_for_group("S01 Slow Consumer")

The flow and the deployment are both named PREFIX + pack name, e.g.
nfh-p1-throughput. Each scenario is a child process group of the root.
"""

import config
from flows.builder.lib import ProcessGroup, flow_definition, parameter_context
from flows.scenarios import (p1_throughput, p2_backpressure, p3_errors, p4_dataloss_latency,
                             p5_parameters, p6_p7_sizing, p8_orders, p9_clickstream,
                             p10_iot, p11_payments, p12_warehouse, p13_billing, p14_capacity,
                             p15_bursts, p16_slow_burn, p17_nightly_batch)

PACKS = {
    pack.name: pack
    for pack in (
        p1_throughput.PACK,
        p2_backpressure.PACK,
        p3_errors.PACK,
        p4_dataloss_latency.PACK,
        p5_parameters.PACK,
        p6_p7_sizing.P6,
        p6_p7_sizing.P7,
        p8_orders.PACK,
        p9_clickstream.PACK,
        p10_iot.PACK,
        p11_payments.PACK,
        p12_warehouse.PACK,
        p13_billing.PACK,
        p14_capacity.PACK,
        p15_bursts.PACK,
        p16_slow_burn.PACK,
        p17_nightly_batch.PACK,
    )
}

SCENARIOS = {s.id: s for pack in PACKS.values() for s in pack.scenarios}

# What a provision script covers when it is named no pack: the first
# rollout wave. Later waves are provisioned by name, one batch at a time.
DEFAULT = [name for name, pack in PACKS.items() if pack.wave == 1]


def flow_name(pack_name: str) -> str:
    return f"{config.PREFIX}{pack_name}"


def pack_of(scenario_id: str):
    for pack in PACKS.values():
        if any(s.id == scenario_id for s in pack.scenarios):
            return pack

    raise KeyError(scenario_id)


def scenario_for_group(group_name: str):
    """The scenario whose process group has this name, else None."""

    for scenario in SCENARIOS.values():
        if scenario.group_name == group_name:
            return scenario

    return None


def build(pack_name: str) -> dict:
    pack = PACKS[pack_name]
    root = ProcessGroup(
        pack.name, flow_name(pack.name),
        comments=f"Self-healing test pack: {pack.title}. Built by flows/build_all.py.",
        parameter_context=pack.parameter_context,
    )

    for scenario in pack.scenarios:
        group = root.group(
            scenario.id, scenario.group_name,
            comments=f"{scenario.fault} (symptom: {scenario.symptom})",
            parameter_context=pack.parameter_context if scenario.parameters else None,
        )
        scenario.build(group)

    contexts = []

    if pack.parameter_context:
        contexts.append(parameter_context(
            pack.parameter_context, pack.parameters,
            description=f"Parameters for {flow_name(pack.name)}.",
        ))

    return flow_definition(root, contexts)
