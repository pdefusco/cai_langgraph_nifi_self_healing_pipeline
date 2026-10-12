# CAI job entry point: runs a repo script as an ordinary Python process.
#
#   Job script: cai_job.py    Arguments: provision/00_selftest.py --json
#
# PBJ Workbench runs a job script cell by cell in an IPython kernel, where
# __file__ is undefined, a module docstring is not __doc__ and the script's
# directory is not on sys.path. The provision/ and flows/ scripts rely on
# all three, so they run in a subprocess instead. The working directory is
# the project root (/home/cdsw).
#
# Arguments: in the kernel sys.argv is ipykernel's own (-f <kernel.json>);
# the job's Arguments field arrives as $JOB_ARGUMENTS. $CAI_JOB_ARGS (a
# per-job environment variable) is the fallback; outside a kernel,
# `python cai_job.py <script.py> ...` works as usual.
import os
import shlex
import subprocess
import sys

if os.environ.get("JOB_ARGUMENTS", "").strip():
    argv = shlex.split(os.environ["JOB_ARGUMENTS"])
elif os.environ.get("CAI_JOB_ARGS", "").strip():
    argv = shlex.split(os.environ["CAI_JOB_ARGS"])
elif sys.argv[1:2] != ["-f"]:
    argv = sys.argv[1:]
else:
    argv = []

if not argv:
    seen = [name for name in ("JOB_ARGUMENTS", "CAI_JOB_ARGS") if name in os.environ]
    raise SystemExit(
        "usage: cai_job.py <script.py> [args...] (as the job's Arguments, "
        f"or $CAI_JOB_ARGS); set: {seen or 'neither'}; sys.argv={sys.argv}"
    )

print(f"[cai_job] $ python {' '.join(argv)}", flush=True)
status = subprocess.call([sys.executable, "-u", *argv])
if status:
    raise RuntimeError(f"{argv[0]} exited with status {status}")
