"""
Pack p23-siblings: three faults among 24 healthy sibling groups.

S51, S52 and S53 are size twins (scale.py) of S02's slow run schedule,
S14's auto-terminated failures and S28's stopped lane, each in a group
about the size of its siblings. S54 to S77 are controls: 8 to 12
processors of healthy filler each, about 240 in all. Each fault must be
found in its own group, and none of the siblings may alert, however many
groups the poller reads.
"""

from flows.scenarios import p1_throughput, p4_dataloss_latency, p11_payments, scale
from flows.scenarios.base import Pack

S51 = scale.twin(p1_throughput.S02, "S51", processors=12, title="Slow Run Schedule Among Siblings")
S52 = scale.twin(p4_dataloss_latency.S14, "S52", processors=14, title="Auto-terminated Failures Among Siblings")
S53 = scale.twin(p11_payments.S28, "S53", processors=24, title="Stopped Lane Among Siblings")
CONTROLS = tuple(scale.control(f"S{54 + i}", processors=8 + 2 * (i % 3), offset=3 * i % len(scale.DOMAINS))
                 for i in range(24))

PACK = Pack(
    name="p23-siblings",
    title="Faults among 24 sibling groups",
    scenarios=(S51, S52, S53) + CONTROLS,
    wave=3,
)
