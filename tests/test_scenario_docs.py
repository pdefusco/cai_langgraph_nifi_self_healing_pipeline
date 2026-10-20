"""
flows/scenarios/README.md is generated from the scenario definitions:
it lists every scenario, and it is not stale.
"""

from flows import packs, scenario_docs, scenario_walkthroughs


def test_the_scenario_readme_is_current():
    with open(scenario_docs.OUT) as handle:
        current = handle.read()

    assert current == scenario_docs.render(), "run python flows/scenario_docs.py"


def test_every_scenario_has_a_section():
    text = scenario_docs.render()

    for scenario in packs.SCENARIOS.values():
        assert f"### {scenario.group_name}\n" in text


def test_every_scenario_and_pack_has_a_walkthrough():
    assert set(scenario_walkthroughs.WALKTHROUGHS) == set(packs.SCENARIOS)
    assert set(scenario_walkthroughs.PACKS) == set(packs.PACKS)


def test_every_scenario_has_a_diagram():
    assert scenario_docs.render().count("```mermaid\n") == len(packs.SCENARIOS)


def test_a_design_fix_target_is_highlighted():
    s01 = scenario_docs.render().split("### S01 ")[1].split("### S02 ")[0]

    assert '["<b>S01 Consumer</b>' in s01
    assert "class p1 fix" in s01
