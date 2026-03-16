"""PipelineRenderer — live terminal status table using Rich."""

from __future__ import annotations

import time

from rich.console import Console
from rich.live import Live
from rich.table import Table

from dag.cell import Cell
from dag.engine import PipelineEngine
from dag.store import CellStore

_STATUS_DISPLAY = {
    "pending": ("[dim]pending[/dim]", ""),
    "running": ("[blue bold]running[/blue bold]", "..."),
    "done": ("[green]done[/green]", ""),
    "stale": ("[yellow]stale[/yellow]", ""),
    "error": ("[red bold]error[/red bold]", ""),
}


class PipelineRenderer:
    """Render pipeline execution as a live-updating Rich table."""

    def __init__(self, name: str = "pipeline") -> None:
        self._name = name
        self._console = Console()
        self._cells: list[Cell] = []
        self._engine: PipelineEngine | None = None
        self._live: Live | None = None
        self._start_time: float = 0.0

    def _build_table(self) -> Table:
        table = Table(title=f"Pipeline: {self._name}")
        table.add_column("Cell", style="bold")
        table.add_column("Status")
        table.add_column("Duration", justify="right")
        table.add_column("Cached", justify="center")

        for c in self._cells:
            style_text, default_dur = _STATUS_DISPLAY.get(
                c.status, (c.status, "")
            )
            duration = (
                f"{c.duration_seconds:.3f}s"
                if c.duration_seconds is not None
                else default_dur
            )
            cached = ""
            if self._engine and c.status == "done":
                cached = (
                    "[dim]yes[/dim]"
                    if c.name in self._engine.cached_cells
                    else "no (ran)"
                )
            table.add_row(c.name, style_text, duration, cached)

        return table

    def _on_status_change(self, cell: Cell, status: str) -> None:
        if self._live:
            self._live.update(self._build_table())

    def run(
        self,
        cells: list[Cell],
        *,
        store: CellStore | None = None,
    ) -> PipelineEngine:
        """Run the pipeline with a live-updating status table."""
        self._cells = cells
        self._engine = PipelineEngine(
            cells,
            store=store,
            on_status_change=self._on_status_change,
        )
        self._start_time = time.monotonic()

        with Live(self._build_table(), console=self._console, refresh_per_second=10) as live:
            self._live = live
            self._engine.run_all()
            # Final update
            live.update(self._build_table())
            self._live = None

        # Summary line
        elapsed = time.monotonic() - self._start_time
        n_cached = len(self._engine.cached_cells)
        n_ran = len(cells) - n_cached
        self._console.print(
            f"\n[bold]{n_ran} cells ran, {n_cached} cached, "
            f"total {elapsed:.3f}s[/bold]"
        )

        return self._engine

    def status(self, cells: list[Cell]) -> None:
        """Print a static status table (no execution)."""
        self._cells = cells
        self._console.print(self._build_table())
