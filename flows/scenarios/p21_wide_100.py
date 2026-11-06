"""
Pack p21-wide-100: three first-wave faults, each in a group of 100 processors.

Size twins (scale.py): S46 is S01's slow consumer, S47 S03's pipeline
bottleneck and S48 S24's fan-out into one task, each renamed and set
among about 95 healthy processors in small flows of their own. The
fault, its fix and its traps are the twin's, so what changes between
S01 and S46 is only how much flow the agent has to read past to find it.
"""

from flows.scenarios import p1_throughput, p9_clickstream, scale
from flows.scenarios.base import Pack

S46 = scale.twin(p1_throughput.S01, "S46", processors=100)
S47 = scale.twin(p1_throughput.S03, "S47", processors=100)
S48 = scale.twin(p9_clickstream.S24, "S48", processors=100)

PACK = Pack(
    name="p21-wide-100",
    title="Wide groups of 100",
    scenarios=(S46, S47, S48),
    wave=3,
)
