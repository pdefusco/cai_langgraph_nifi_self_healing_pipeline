"""
Pack p24-healthy-500: 500 healthy processors in 20 groups, and no fault.

S78 to S97 are controls (scale.py): 25 processors of healthy filler
each. The whole deployment must stay quiet, so this is the false alarm
rate, and the poll and prompt load, at the largest size we test.
"""

from flows.scenarios import scale
from flows.scenarios.base import Pack

CONTROLS = tuple(scale.control(f"S{78 + i}", processors=25, offset=2 * i % len(scale.DOMAINS)) for i in range(20))

PACK = Pack(
    name="p24-healthy-500",
    title="Controls: 500 healthy processors",
    scenarios=CONTROLS,
    wave=3,
)
