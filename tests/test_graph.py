"""Tests for DAGGraph: build, validate, topological sort, ancestors, descendants."""

import pytest

from dag.cell import Cell
from dag.graph import CycleError, DAGGraph


def _make_cell(name: str, depends_on: list[Cell] | None = None) -> Cell:
    return Cell(name=name, func=lambda: None, depends_on=depends_on or [])


class TestDAGGraph:
    """Test the four-cell pipeline from the issue spec."""

    def setup_method(self) -> None:
        self.raw_data = _make_cell("raw_data")
        self.agent_episodes = _make_cell("agent_episodes", [self.raw_data])
        self.summary = _make_cell("summary", [self.agent_episodes])
        self.comparison = _make_cell("comparison", [self.raw_data, self.agent_episodes])

        self.graph = DAGGraph()
        self.graph.build([self.raw_data, self.agent_episodes, self.summary, self.comparison])

    def test_validate_passes_for_acyclic_graph(self) -> None:
        self.graph.validate()  # should not raise

    def test_topological_order_deps_before_dependents(self) -> None:
        order = self.graph.topological_order()
        names = [c.name for c in order]

        # raw_data must come before everything else
        assert names.index("raw_data") < names.index("agent_episodes")
        assert names.index("raw_data") < names.index("comparison")
        # agent_episodes before summary and comparison
        assert names.index("agent_episodes") < names.index("summary")
        assert names.index("agent_episodes") < names.index("comparison")

    def test_topological_order_contains_all_cells(self) -> None:
        order = self.graph.topological_order()
        assert len(order) == 4

    def test_descendants_of_raw_data(self) -> None:
        desc = self.graph.descendants(self.raw_data)
        names = {c.name for c in desc}
        assert names == {"agent_episodes", "summary", "comparison"}

    def test_descendants_of_agent_episodes(self) -> None:
        desc = self.graph.descendants(self.agent_episodes)
        names = {c.name for c in desc}
        assert names == {"summary", "comparison"}

    def test_descendants_of_leaf(self) -> None:
        desc = self.graph.descendants(self.summary)
        assert desc == []

    def test_ancestors_of_comparison(self) -> None:
        anc = self.graph.ancestors(self.comparison)
        names = {c.name for c in anc}
        assert names == {"raw_data", "agent_episodes"}

    def test_ancestors_of_root(self) -> None:
        anc = self.graph.ancestors(self.raw_data)
        assert anc == []


class TestCycleDetection:
    def test_cycle_raises_cycle_error(self) -> None:
        a = _make_cell("A")
        b = _make_cell("B", [a])
        # Create a cycle: A depends on B, B depends on A
        a.depends_on = [b]

        graph = DAGGraph()
        graph.build([a, b])

        with pytest.raises(CycleError, match="Cycle detected"):
            graph.validate()

    def test_self_cycle_raises_cycle_error(self) -> None:
        a = _make_cell("A")
        a.depends_on = [a]

        graph = DAGGraph()
        graph.build([a])

        with pytest.raises(CycleError, match="Cycle detected"):
            graph.validate()
