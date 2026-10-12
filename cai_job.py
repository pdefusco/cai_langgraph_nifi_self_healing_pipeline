# CAI job entry point: runs a repo script as an ordinary Python process.
#
#   Job script: cai_job.py    Arguments: provision/00_selftest.py --json
#
# PBJ Workbench runs a job script cell by cell in an IPython kernel, where
# __file__ is undefined, a module docstring is not __doc__ and the script's
# directory is not on sys.path. The provision/ and flows/ scripts rely on
# all three, so they run in a subprocess instead. The working directory is
# the project root (/home/cdsw); arguments arrive in sys.argv.
import subprocess
import sys

argv = sys.argv[1:]
if not argv:
    raise SystemExit("usage: cai_job.py <script.py> [args...]")

print(f"[cai_job] $ python {' '.join(argv)}", flush=True)
status = subprocess.call([sys.executable, "-u", *argv])
if status:
    raise RuntimeError(f"{argv[0]} exited with status {status}")
