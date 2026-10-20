"""
flows/scenarios/README.md is generated from the scenario definitions:
it lists every scenario, and it is not stale.
"""

from flows import packs, scenario_docs


def test_the_scenario_readme_is_current():
    with open(scenario_docs.OUT) as handle:
        current = handle.read()

    assert current == scenario_docs.render(), "run python flows/scenario_docs.py"


def test_every_scenario_has_a_section():
    text = scenario_docs.render()

    for scenario in packs.SCENARIOS.values():
        assert f"### {scenario.group_name}\n" in text
