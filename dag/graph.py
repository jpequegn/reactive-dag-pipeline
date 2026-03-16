"""DAGGraph — build, validate, and query the dependency graph."""

from __future__ import annotations

import networkx as nx

from dag.cell import Cell


class CycleError(Exception):
    """Raised when the dependency graph contains a cycle."""


class DAGGraph:
    """Directed acyclic graph built from Cell dependency declarations."""

    def __init__(self) -> None:
        self._graph: nx.DiGraph = nx.DiGraph()
        self._cells: dict[str, Cell] = {}

    def build(self, cells: list[Cell]) -> None:
        """Create a networkx DiGraph from cell.depends_on edges."""
        self._graph = nx.DiGraph()
        self._cells = {c.name: c for c in cells}

        for cell in cells:
            self._graph.add_node(cell.name)

        for cell in cells:
            for dep in cell.depends_on:
                # Edge from dependency → dependent (dep must run before cell)
                self._graph.add_edge(dep.name, cell.name)

    def validate(self) -> None:
        """Raise CycleError if the graph contains cycles."""
        if not nx.is_directed_acyclic_graph(self._graph):
            cycle = nx.find_cycle(self._graph)
            path = " -> ".join(f"{u}" for u, _ in cycle)
            raise CycleError(f"Cycle detected: {path} -> {cycle[0][0]}")

    def topological_order(self) -> list[Cell]:
        """Return cells in execution order (all deps before dependents)."""
        return [self._cells[name] for name in nx.topological_sort(self._graph)]

    def descendants(self, cell: Cell) -> list[Cell]:
        """All cells that transitively depend on this cell."""
        desc_names = nx.descendants(self._graph, cell.name)
        # Return in topological order for consistency
        return [
            self._cells[name]
            for name in nx.topological_sort(self._graph)
            if name in desc_names
        ]

    def ancestors(self, cell: Cell) -> list[Cell]:
        """All cells this cell transitively depends on."""
        anc_names = nx.ancestors(self._graph, cell.name)
        return [
            self._cells[name]
            for name in nx.topological_sort(self._graph)
            if name in anc_names
        ]
