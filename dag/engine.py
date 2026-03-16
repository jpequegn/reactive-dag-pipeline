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
        self,
        cells: list[Cell],
        *,
        store: CellStore | None = None,
        on_status_change: Callable[[Cell, str], None] | None = None,
    ) -> None:
        self._cells = cells
        self._graph = DAGGraph()
        self._graph.build(cells)
        self._graph.validate()
        self._store = store
        self._on_status_change = on_status_change
        self._cached: set[str] = set()

    @property
    def cached_cells(self) -> set[str]:
        """Names of cells that were loaded from cache in the last run."""
        return self._cached

    def _set_status(self, cell: Cell, status: str) -> None:
        cell.status = status
        if self._on_status_change:
            self._on_status_change(cell, status)

    def run_all(self) -> None:
        """Execute all cells in topological order, loading cached outputs when fresh."""
        self._cached = set()
        for c in self._cells:
            self._set_status(c, "pending")
        must_run: set[str] = set()
        for c in self._graph.topological_order():
            if c.name not in must_run and self._store and self._store.is_fresh(c):
                c.output = self._store.load(c)
                self._cached.add(c.name)
                self._set_status(c, "done")
            else:
                self._execute_cell(c)
                # Descendants must re-execute since this cell's output changed
                for desc in self._graph.descendants(c):
                    must_run.add(desc.name)

    def invalidate(self, cell: Cell) -> None:
        """Mark cell and all its descendants as stale."""
        self._set_status(cell, "stale")
        for desc in self._graph.descendants(cell):
            self._set_status(desc, "stale")

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
        self._set_status(cell, "running")
        # Collect outputs from dependencies in depends_on order
        dep_outputs = [dep.output for dep in cell.depends_on]
        start = time.monotonic()
        try:
            cell.output = cell.func(*dep_outputs)
            if self._store:
                self._store.save(cell)
            self._set_status(cell, "done")
        except Exception as exc:
            cell.error = exc
            self._set_status(cell, "error")
            # Mark all descendants as error too
            for desc in self._graph.descendants(cell):
                self._set_status(desc, "error")
            return
        finally:
            cell.duration_seconds = time.monotonic() - start
            cell.last_run = datetime.now(timezone.utc)
