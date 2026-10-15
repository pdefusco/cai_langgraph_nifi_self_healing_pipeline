"""
A long-lived CDP CLI process, so a read pays cdpcli's imports once per
worker instead of once per call (cdp/client.py's WorkerPool runs a few).

    python nifi_healer/cdp/worker.py

One request per line on stdin, {"args": [...]}; one reply per line on
stdout, {"rc": int, "stdout": str, "stderr": str}, as the one-shot
subprocess would have exited and printed. Each call gets a fresh
CLIDriver (cheap; the imports are what cost), with sys.stdout and
sys.stderr captured. The protocol runs on copies of fds 0 and 1, which
then point at /dev/null, so nothing cdpcli writes can corrupt a reply.

Standard library and cdpcli only: it runs by path, not as part of the
package. Credentials come from the environment it was started with,
never from a request.
"""

import io
import json
import logging
import os
import sys


def _call(args: list) -> dict:
    from cdpcli.clidriver import CLIDriver

    out, err = io.StringIO(), io.StringIO()
    stdout, stderr, stdin = sys.stdout, sys.stderr, sys.stdin
    handlers = list(logging.getLogger().handlers)
    sys.stdout, sys.stderr, sys.stdin = out, err, io.StringIO()

    try:
        rc = CLIDriver().main([str(a) for a in args])

    except SystemExit as exc:        # argparse usage errors
        rc = exc.code if isinstance(exc.code, int) else 1

    except Exception as exc:         # what the one-shot process would have died of
        err.write(f"{type(exc).__name__}: {exc}\n")
        rc = 255

    finally:
        sys.stdout, sys.stderr, sys.stdin = stdout, stderr, stdin
        root = logging.getLogger()

        # Each main() adds a handler to the root logger; drop them, or they pile up.
        for handler in list(root.handlers):
            if handler not in handlers:
                root.removeHandler(handler)

    return {"rc": rc or 0, "stdout": out.getvalue(), "stderr": err.getvalue()}


def serve() -> None:
    requests = os.fdopen(os.dup(0), "r", encoding="utf-8")
    replies = os.fdopen(os.dup(1), "w", encoding="utf-8")
    null = os.open(os.devnull, os.O_RDWR)
    os.dup2(null, 0)
    os.dup2(null, 1)

    import cdpcli.clidriver  # noqa: F401  (the import is the point of a worker)

    replies.write(json.dumps({"ready": True}) + "\n")
    replies.flush()

    for line in requests:
        try:
            args = json.loads(line)["args"]

        except (ValueError, KeyError, TypeError):
            reply = {"rc": 2, "stdout": "", "stderr": "worker: a request must be {\"args\": [...]}"}

        else:
            reply = _call(args)

        replies.write(json.dumps(reply) + "\n")
        replies.flush()


if __name__ == "__main__":
    serve()
