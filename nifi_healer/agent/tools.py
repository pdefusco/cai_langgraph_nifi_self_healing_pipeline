"""
Read-only tools the agent may call before it proposes (NFH-91), with
config.TOOLS on. It asks for one by replying with a JSON object with a
"tool" key instead of the proposal; the answer comes back as the next
message (agent/nodes._ask). No function calling: the endpoint's model may
not have it, and a reasoning model's thinking is already read past.

    check_fix     {"steps": [...]}: what validation and the capacity check
                  would send these steps back for on a first attempt
    kpi_history   {"component": ..., "metric": ...}: one KPI's last 2 h,
                  from the store (kpi/readings.history)

    reads = Reads(get_client())        # one diagnosis plans many fixes on the same flow
    calls = Calls(check, incident, store, now)
    answer = calls.run(request)        # the next message, or None once refused
    calls.trace                        # [{tool, args, result}], for the validation report

Each proposal (each draw, with SELFHEAL_SAMPLES) has its own Calls, up to
config.TOOL_CALLS; a call after that is told none are left, once. The
draws run in threads, so the tools run one at a time. Nothing here writes:
check_fix plans the edit and stops there, as the vote does.
"""

import copy
import json
import threading

import config
from nifi_healer.agent import prompts
from nifi_healer.kpi import readings

NAMES = ("check_fix", "kpi_history")

# Tool runs share the mock tenant and the store with the other draws.
LOCK = threading.Lock()


class Reads:
    """A client whose flow reads are remembered, so checking a fix costs no CDF call after the first."""

    CACHED = ("flow_facts", "flow_definition", "flow_configuration", "deployment_configuration")

    def __init__(self, client):
        self._client, self._seen, self._lock = client, {}, threading.Lock()

    def __getattr__(self, name):
        found = getattr(self._client, name)

        if name not in self.CACHED:
            return found

        def read(*args):
            with self._lock:
                if (name,) + args not in self._seen:
                    value = found(*args)

                    if isinstance(value, dict) and "error" in value:
                        return value        # asked again next time

                    self._seen[(name,) + args] = value

                return copy.deepcopy(self._seen[(name,) + args])

        return read


class Calls:
    """One proposal's tool calls: `check(steps)` gives its problems ([] for none)."""

    def __init__(self, check, incident: dict, store, now: float, budget: int = 0):
        self.check, self.incident, self.store, self.now = check, incident, store, now
        self.left = budget or config.TOOL_CALLS
        self.refused = False
        self.trace = []

    def run(self, request: dict) -> str | None:
        """The message that answers `request`; None when it was already told no calls are left."""

        if self.left <= 0:
            if self.refused:
                return None

            self.refused = True

            return prompts.TOOL_NONE_LEFT

        self.left -= 1
        name = str(request.get("tool") or "")
        args = {k: v for k, v in request.items() if k != "tool"}

        try:
            with LOCK:
                result = self._answer(name, args)

        except Exception as exc:  # the agent can still propose without it
            result = f"{name} failed: {exc}"

        self.trace.append({"tool": name, "args": args, "result": result})

        return prompts.TOOL_RESULT.format(tool=name, result=result, left=prompts.tool_calls_left(self.left))

    def _answer(self, name: str, args: dict) -> str:
        if name == "check_fix":
            steps = args.get("steps")

            if not isinstance(steps, list) or not all(isinstance(s, dict) for s in steps):
                return 'check_fix needs "steps": a list of catalog steps, as in a proposal'

            problems = self.check(steps)

            return "These steps would be sent back:\n" + "\n".join(f"- {p}" for p in problems) if problems else \
                "No problems: validation and the capacity check would pass these steps."

        if name == "kpi_history":
            return readings.history(self.incident, self.store, self.now, args.get("component"), args.get("metric"))

        return f"There is no tool {json.dumps(name)}; the tools are {', '.join(NAMES)}."
