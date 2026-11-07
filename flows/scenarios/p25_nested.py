"""
Pack p25-nested: three faults one and two groups down, below input and
output ports (NFH-96).

S98, S99 and S100 are nested twins (scale.nest) of S01's slow consumer,
S09's failure self-loop and S14's auto-terminated failures: each fault
sits whole in "Sxx Region" or "Sxx Site" inside the scenario's group,
among healthy filler on every level and crossings that carry FlowFiles
down through the ports and back up. S09's and S14's DLQs are beside
their validators, deepest down, where a route to them must stay. S101
and S102 are nested controls, one and two groups down.
"""

from flows.scenarios import p1_throughput, p3_errors, p4_dataloss_latency, scale
from flows.scenarios.base import Pack

S98 = scale.nest(p1_throughput.S01, "S98", depth=2, processors=60)
S99 = scale.nest(p3_errors.S09, "S99", depth=3, processors=80)
S100 = scale.nest(p4_dataloss_latency.S14, "S100", depth=3, processors=75)
S101 = scale.control("S101", processors=40, offset=4, depth=2, title="Healthy Groups One Down")
S102 = scale.control("S102", processors=60, offset=9, depth=3, title="Healthy Groups Two Down")

PACK = Pack(
    name="p25-nested",
    title="Faults in nested groups, below ports",
    scenarios=(S98, S99, S100, S101, S102),
    wave=3,
)
