"""
The commands and options the installed cdpcli actually accepts, read
from its bundled service models and its df CLI extensions.

The mock tenant checks every argv against this, so a command that would
fail on a live tenant (wrong plane, misspelt option, missing required
option) fails in mock mode too, instead of being simulated happily.
"""

import functools
import importlib
import os
import re

import yaml

PLANES = ("df", "dfworkload")

# Accepted by every command (cdpcli/clidriver.py global arguments).
GLOBAL_OPTIONS = {
    "debug", "endpoint-url", "no-verify-tls", "ca-bundle", "cli-read-timeout",
    "cli-connect-timeout", "output", "color", "profile", "auth-config", "form-factor",
    "cdp-region", "access-token", "no-paginate", "cli-input-json", "generate-cli-skeleton",
}

# cdpcli replaces a paginated operation's page token and size with these.
PAGING_OPTIONS = {"max-items", "starting-token", "page-size"}

# The df CLI extensions (cdpcli/extensions/df/register.py) that add
# commands with no path in the service model.
EXTENSIONS = (
    "addflowtodeployment", "changeflowversion", "changeflowversionindeployment",
    "createdeployment", "createparametergroup", "retrychangeflowversion",
    "retrychangeflowversionindeployment",
)


def kebab(name: str) -> str:
    return re.sub(r"(?<=[a-z0-9])([A-Z])", r"-\1", name).lower()


def _shape(spec: dict, op: dict) -> dict:
    for parameter in op.get("parameters") or []:
        ref = (parameter.get("schema") or {}).get("$ref", "")

        if ref:
            return spec["definitions"][ref.split("/")[-1]]

    return {}


def _entry(shape: dict, paginated: bool) -> dict:
    options = {kebab(p) for p in shape.get("properties") or {}}
    required = {kebab(p) for p in shape.get("required") or []}

    if paginated:
        options = (options - {"page-token", "page-size"}) | PAGING_OPTIONS

    return {"options": options, "required": required}


@functools.cache
def operations() -> dict:
    """{(plane, command): {"options": {...}, "required": {...}}}"""

    import cdpcli

    root = os.path.dirname(cdpcli.__file__)
    found = {}

    for plane in PLANES:
        with open(os.path.join(root, "data", plane, f"{plane}.yaml")) as handle:
            spec = yaml.safe_load(handle)

        for body in spec["paths"].values():
            op = body.get("post") or {}

            if op.get("operationId"):
                found[(plane, kebab(op["operationId"]))] = _entry(
                    _shape(spec, op), "x-paging-default-max-items" in op)

    for name in EXTENSIONS:
        module = importlib.import_module(f"cdpcli.extensions.df.{name}")
        request = next(v for k, v in module.OPERATION_SHAPES.items() if k.endswith("Request"))
        found[("df", module.OPERATION_CLI_NAME)] = _entry(request, False)

    return found


def check(plane: str, command: str, options) -> str:
    """Why cdpcli would reject this command line, or "" if it would not."""

    entry = operations().get((plane, command))

    if entry is None:
        alternatives = [p for p in PLANES if (p, command) in operations()]
        hint = f" (it is a {alternatives[0]} command)" if alternatives else ""

        return f"cdp {plane}: invalid choice: {command!r}{hint}"

    unknown = sorted(set(options) - entry["options"] - GLOBAL_OPTIONS)

    if unknown:
        return f"cdp {plane} {command}: unknown option(s): " + ", ".join(f"--{o}" for o in unknown)

    missing = sorted(entry["required"] - set(options))

    if missing:
        return ("the following arguments are required: "
                + ", ".join(f"--{o}" for o in missing))

    return ""
