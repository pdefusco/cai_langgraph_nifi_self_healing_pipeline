"""
docs/ARCHITECTURE.md draws the agent's graph from the graph itself, and
its node table names every node.
"""

from nifi_healer.agent import diagram, graph


def test_the_graph_drawing_is_current():
    with open(diagram.OUT) as handle:
        current = handle.read()

    assert current == diagram.render(current), "run python -m nifi_healer.agent.diagram"


def test_the_node_table_names_every_node():
    with open(diagram.OUT) as handle:
        text = handle.read()

    nodes = set(graph.build().get_graph().nodes) - {"__start__", "__end__"}

    assert nodes and all(f"\n| {node} |" in text for node in nodes)
