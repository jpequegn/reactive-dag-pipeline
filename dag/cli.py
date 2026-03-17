"""CLI entrypoint for the reactive DAG pipeline."""

from __future__ import annotations

from pathlib import Path

import click

from dag.engine import PipelineEngine
from dag.loader import load_pipeline
from dag.renderer import PipelineRenderer
from dag.store import CellStore


def _store_for(pipeline_path: str) -> CellStore:
    """Create a CellStore scoped to the pipeline file name."""
    name = Path(pipeline_path).stem
    return CellStore(Path.home() / ".dag" / "cache" / name)


@click.group()
@click.version_option()
def main() -> None:
    """Reactive DAG Pipeline — build and run reactive computation graphs."""


@main.command()
@click.argument("pipeline", type=click.Path(exists=True))
@click.option("--cell", "cell_name", default=None, help="Run only from this cell downward.")
def run(pipeline: str, cell_name: str | None) -> None:
    """Run all cells in a pipeline file."""
    cells = load_pipeline(pipeline)
    store = _store_for(pipeline)
    name = Path(pipeline).stem
    renderer = PipelineRenderer(name=name)

    if cell_name is not None:
        # Run the full pipeline first (loading cache), then invalidate+run from cell
        engine = PipelineEngine(cells, store=store)
        engine.run_all()
        target = next((c for c in cells if c.name == cell_name), None)
        if target is None:
            raise click.ClickException(f"Cell '{cell_name}' not found in pipeline")
        engine.invalidate(target)
        engine.run_stale()
        renderer.status(cells)
    else:
        renderer.run(cells, store=store)


@main.command()
@click.argument("pipeline", type=click.Path(exists=True))
def status(pipeline: str) -> None:
    """Show cell status table without executing."""
    cells = load_pipeline(pipeline)
    store = _store_for(pipeline)
    name = Path(pipeline).stem

    # Load cached state
    for c in cells:
        if store.is_fresh(c):
            c.output = store.load(c)
            c.status = "done"

    renderer = PipelineRenderer(name=name)
    renderer.status(cells)


@main.command()
@click.argument("pipeline", type=click.Path(exists=True))
@click.option("--cell", "cell_name", default=None, help="Invalidate from this cell downward.")
@click.option("--all", "invalidate_all", is_flag=True, help="Clear all cache.")
def invalidate(pipeline: str, cell_name: str | None, invalidate_all: bool) -> None:
    """Mark cells as stale so they re-execute on next run."""
    cells = load_pipeline(pipeline)
    store = _store_for(pipeline)

    if invalidate_all:
        for c in cells:
            store.invalidate(c)
        click.echo(f"Cleared cache for all {len(cells)} cells.")
        return

    if cell_name is None:
        raise click.ClickException("Specify --cell NAME or --all")

    target = next((c for c in cells if c.name == cell_name), None)
    if target is None:
        raise click.ClickException(f"Cell '{cell_name}' not found in pipeline")

    engine = PipelineEngine(cells, store=store)
    store.invalidate(target)
    descendants = engine._graph.descendants(target)
    for desc in descendants:
        store.invalidate(desc)

    click.echo(f"Invalidated {target.name} and {len(descendants)} descendant(s).")


@main.command()
@click.argument("pipeline", type=click.Path(exists=True))
@click.option("--interval", default=0.5, help="Poll interval in seconds.", show_default=True)
def watch(pipeline: str, interval: float) -> None:
    """Watch a pipeline file and re-execute reactively on save."""
    from dag.watcher import PipelineWatcher

    watcher = PipelineWatcher(pipeline, poll_interval=interval)
    watcher.watch()


@main.command()
@click.argument("pipeline", type=click.Path(exists=True))
def graph(pipeline: str) -> None:
    """Print the dependency graph as ASCII."""
    from networkx.readwrite.text import generate_network_text

    from dag.graph import DAGGraph

    cells = load_pipeline(pipeline)
    dag = DAGGraph()
    dag.build(cells)

    click.echo(f"Pipeline: {Path(pipeline).stem}")
    click.echo()
    for line in generate_network_text(dag._graph):
        click.echo(line)


if __name__ == "__main__":
    main()
