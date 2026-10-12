#!/usr/bin/env python3
"""
Install the project's dependencies and run the offline test suite, as a
CAI job (PBJ jobs run a Python script, so this wraps cdsw-build.sh).

    python provision/cai_build.py [--skip-tests]

Packages land in the project's user site (/home/cdsw/.local), so the
Application and every later job or session reuse them.
"""

import argparse
import os
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def run(argv: list[str]) -> int:
    print(f"[build] $ {' '.join(argv)}", flush=True)
    return subprocess.call(argv, cwd=REPO)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--skip-tests", action="store_true", help="install only")
    args = parser.parse_args()

    status = run(["bash", "cdsw-build.sh"])
    if status or args.skip_tests:
        return status
    return run([sys.executable, "-m", "pytest", "-q", "tests"])


if __name__ == "__main__":
    sys.exit(main())
