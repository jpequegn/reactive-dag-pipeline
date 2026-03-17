"""Pipeline file watcher — live reactive re-execution on save."""

from __future__ import annotations

import os
import time
from pathlib import Path

from rich.console import Console
from rich.live import Live
from rich.table import Table

from dag.cell import Cell
from dag.engine import PipelineEngine
from dag.loader import load_pipeline
from dag.store import CellStore, _bytecode_hash


def _collect_hashes(cells: list[Cell]) -> dict[str, str]:
    """Snapshot bytecode hashes for all cells."""
    return {c.name: _bytecode_hash(c) for c in cells}


def _diff_hashes(
    old: dict[str, str], new: dict[str, str]
) -> list[str]:
    """Return names of cells whose bytecode changed."""
    changed = []
    for name, new_hash in new.items():
        if old.get(name) != new_hash:
            changed.append(name)
    return changed


_STATUS_DISPLAY = {
    "pending": ("[dim]pending[/dim]", ""),
    "running": ("[blue bold]running[/blue bold]", "..."),
    "done": ("[green]done[/green]", ""),
    "stale": ("[yellow]stale[/yellow]", ""),
    "error": ("[red bold]error[/red bold]", ""),
}


class PipelineWatcher:
    """Watch a pipeline file and reactively re-execute on changes."""

    def __init__(
        self,
        pipeline_path: str,
        *,
        poll_interval: float = 0.5,
    ) -> None:
        self._path = Path(pipeline_path).resolve()
        self._name = self._path.stem
        self._poll_interval = poll_interval
        self._console = Console()
        self._store = CellStore(Path.home() / ".dag" / "cache" / self._name)
        self._cells: list[Cell] = []
        self._engine: PipelineEngine | None = None
        self._hashes: dict[str, str] = {}
        self._live: Live | None = None
        self._run_count = 0
        self._last_message = ""

    def _build_table(self) -> Table:
        title = f"Pipeline: {self._name}  [dim](watching — Ctrl+C to stop)[/dim]"
        table = Table(title=title)
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

        if self._last_message:
            table.caption = self._last_message

        return table

    def _on_status_change(self, cell: Cell, status: str) -> None:
        if self._live:
            self._live.update(self._build_table())

    def _run_pipeline(self, changed_cells: list[str] | None = None) -> None:
        """Execute the pipeline, optionally with surgical invalidation."""
        self._engine = PipelineEngine(
            self._cells,
            store=self._store,
            on_status_change=self._on_status_change,
        )
        start = time.monotonic()

        if changed_cells:
            # First load everything from cache
            self._engine.run_all()
            # Then surgically invalidate changed cells + descendants
            for name in changed_cells:
                target = next((c for c in self._cells if c.name == name), None)
                if target:
                    self._store.invalidate(target)
                    self._engine.invalidate(target)
                    for desc in self._engine._graph.descendants(target):
                        self._store.invalidate(desc)
            self._engine.run_stale()
        else:
            self._engine.run_all()

        elapsed = time.monotonic() - start
        n_cached = len(self._engine.cached_cells)
        n_ran = len(self._cells) - n_cached

        if changed_cells:
            changed_str = ", ".join(changed_cells)
            self._last_message = (
                f"[bold]Changed: {changed_str} — "
                f"{n_ran} re-ran, {n_cached} cached, {elapsed:.3f}s[/bold]"
            )
        else:
            self._last_message = (
                f"[bold]Initial run — {n_ran} ran, {n_cached} cached, "
                f"{elapsed:.3f}s[/bold]"
            )

        self._run_count += 1

    def _reload_and_diff(self) -> list[str] | None:
        """Re-import the pipeline and return names of changed cells."""
        try:
            new_cells = load_pipeline(str(self._path))
        except Exception as exc:
            self._last_message = f"[red bold]Reload error: {exc}[/red bold]"
            return None

        new_hashes = _collect_hashes(new_cells)
        changed = _diff_hashes(self._hashes, new_hashes)

        # Update state
        self._cells = new_cells
        self._hashes = new_hashes

        return changed if changed else None

    def watch(self) -> None:
        """Main watch loop — run initial pipeline then poll for changes."""
        self._console.print(
            f"[bold]Watching[/bold] {self._path} "
            f"[dim](poll every {self._poll_interval}s, Ctrl+C to stop)[/dim]\n"
        )

        # Initial run
        self._cells = load_pipeline(str(self._path))
        self._hashes = _collect_hashes(self._cells)

        with Live(
            self._build_table(),
            console=self._console,
            refresh_per_second=10,
        ) as live:
            self._live = live

            self._run_pipeline()
            live.update(self._build_table())

            last_mtime = os.stat(self._path).st_mtime

            try:
                while True:
                    time.sleep(self._poll_interval)
                    current_mtime = os.stat(self._path).st_mtime

                    if current_mtime != last_mtime:
                        last_mtime = current_mtime
                        changed = self._reload_and_diff()

                        if changed is not None:
                            self._run_pipeline(changed_cells=changed)

                        live.update(self._build_table())

            except KeyboardInterrupt:
                self._live = None

        self._console.print("\n[dim]Watch stopped.[/dim]")
