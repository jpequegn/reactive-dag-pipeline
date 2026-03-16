"""PipelineEngine — reactive execution of DAG cells."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Callable

from dag.cell import Cell
from dag.graph import DAGGraph
from dag.store import CellStore


class PipelineEngine:
    """Execute cells in topological order, with reactive invalidation."""

    def __init__(
        self, cells: list[Cell], *, store: CellStore | None = None
    ) -> None:
        self._cells = cells
        self._graph = DAGGraph()
        self._graph.build(cells)
        self._graph.validate()
        self._store = store

    def run_all(self) -> None:
        """Execute all cells in topological order, loading cached outputs when fresh."""
        for c in self._cells:
            c.status = "pending"
        must_run: set[str] = set()
        for c in self._graph.topological_order():
            if c.name not in must_run and self._store and self._store.is_fresh(c):
                c.output = self._store.load(c)
                c.status = "done"
            else:
                self._execute_cell(c)
                # Descendants must re-execute since this cell's output changed
                for desc in self._graph.descendants(c):
                    must_run.add(desc.name)

    def invalidate(self, cell: Cell) -> None:
        """Mark cell and all its descendants as stale."""
        cell.status = "stale"
        for desc in self._graph.descendants(cell):
            desc.status = "stale"

    def run_stale(self) -> None:
        """Re-execute only stale cells in topological order."""
        for c in self._graph.topological_order():
            if c.status == "stale":
                self._execute_cell(c)

    def update(self, cell: Cell, new_func: Callable) -> None:
        """Update a cell's function, invalidate it and descendants, re-run stale."""
        cell.func = new_func
        self.invalidate(cell)
        self.run_stale()

    def _execute_cell(self, cell: Cell) -> None:
        """Execute a single cell, collecting dep outputs as positional args."""
        cell.status = "running"
        # Collect outputs from dependencies in depends_on order
        dep_outputs = [dep.output for dep in cell.depends_on]
        start = time.monotonic()
        try:
            cell.output = cell.func(*dep_outputs)
            cell.status = "done"
            if self._store:
                self._store.save(cell)
        except Exception as exc:
            cell.status = "error"
            cell.error = exc
            # Mark all descendants as error too
            for desc in self._graph.descendants(cell):
                desc.status = "error"
            return
        finally:
            cell.duration_seconds = time.monotonic() - start
            cell.last_run = datetime.now(timezone.utc)
