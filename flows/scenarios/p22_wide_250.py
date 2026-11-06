"""
Pack p22-wide-250: two faults, each in a group of 250 processors, each
beside a healthy flow that looks like it.

Size twins (scale.py) with a decoy: S49 is S22's merge deadlock beside a
merge that batches, whose input climbs to 5,000 and empties every 500 s;
S50 is S09's failure self-loop beside a validator that retries its own
failures and keeps up. Each decoy's queue is charted for the agent, a
trap says why it is healthy, and changing it is a wrong fix that leaves
the fault in place.
"""

from flows.scenarios import p3_errors, p8_orders, scale
from flows.scenarios.base import Pack

S49 = scale.twin(p8_orders.S22, "S49", processors=250, decoy="batch")
S50 = scale.twin(p3_errors.S09, "S50", processors=250, decoy="retry")

PACK = Pack(
    name="p22-wide-250",
    title="Wide groups of 250, with decoys",
    scenarios=(S49, S50),
    wave=3,
)
