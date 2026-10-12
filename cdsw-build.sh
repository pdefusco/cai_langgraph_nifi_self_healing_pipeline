#!/bin/bash
# Installs the project's dependencies into the CAI runtime, then checks
# the generated flow definitions are current. Run it once from a CAI
# session terminal (the Application reuses the project's packages), or
# let a CAI build run it.
set -euo pipefail

cd "$(dirname "$0")"

pip3 install --upgrade pip
pip3 install -r requirements.txt

python3 flows/build_all.py --check
