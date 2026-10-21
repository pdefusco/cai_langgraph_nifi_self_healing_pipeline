"""
The agent's graph, drawn from the graph itself into docs/ARCHITECTURE.md.

    python -m nifi_healer.agent.diagram            # rewrite the drawing
    python -m nifi_healer.agent.diagram --check    # fail if it is stale

Only the block between BEGIN and END is generated; the rest of the file
is hand-written. LangGraph draws a conditional edge dotted, labelled with
the route its router returned.
"""

import argparse
import os
import sys

from nifi_healer.agent import graph

OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                   "docs", "ARCHITECTURE.md")
BEGIN = "<!-- agent-graph:begin (python -m nifi_healer.agent.diagram) -->"
END = "<!-- agent-graph:end -->"


def mermaid() -> str:
    return graph.build().get_graph().draw_mermaid().strip()


def render(text: str) -> str:
    """The file's text with its drawing replaced by the graph's own."""

    head, found, rest = text.partition(BEGIN)

    if not found or END not in rest:
        raise ValueError(f"{OUT} has no {BEGIN} ... {END} block")

    return f"{head}{BEGIN}\n```mermaid\n{mermaid()}\n```\n{END}{rest.split(END, 1)[1]}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("--check", action="store_true", help="fail if the drawing is stale")
    args = parser.parse_args()

    with open(OUT) as handle:
        current = handle.read()

    text = render(current)

    if args.check:
        if current != text:
            print(f"{OUT} is stale; run python -m nifi_healer.agent.diagram")

            return 1

        return 0

    with open(OUT, "w") as handle:
        handle.write(text)

    print(f"wrote {OUT}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
