# Reactive DAG Pipeline — Implementation Plan

## What We're Building

A reactive Python execution engine where cells have explicit dependencies and changing a cell automatically re-executes only its downstream dependents — nothing else. A mini-Marimo built from scratch.

## Why This Matters

Marimo got a dedicated Software Engineering Daily episode because it solves a real problem: Jupyter notebooks are fundamentally broken for data work. Sequential execution creates hidden state. "Run all" is wasteful. Cells that depend on each other have no formal relationship — it's implicit and fragile.

The reactive DAG model fixes this:
- Every cell declares its inputs explicitly
- The engine builds a dependency graph
- Change cell A → only cells that (transitively) depend on A re-execute
- Cells that don't depend on A are untouched

This is the same model used in spreadsheets (Excel/Sheets), build systems (Make/Bazel), and reactive UI frameworks (React/Svelte). Building it teaches you one of the most reusable primitives in computing.

## Core Concept

```python
@cell
def raw_data():
    """No dependencies — runs once."""
    return load_p3_episodes()

@cell(depends_on=[raw_data])
def agent_episodes(raw_data):
    """Depends on raw_data. Re-runs when raw_data changes."""
    return [e for e in raw_data if any('agent' in t.lower() for t in e['topics'])]

@cell(depends_on=[agent_episodes])
def summary(agent_episodes):
    """Depends on agent_episodes. Re-runs when agent_episodes changes."""
    return f"{len(agent_episodes)} agent-related episodes found"

@cell(depends_on=[raw_data, agent_episodes])
def comparison(raw_data, agent_episodes):
    """Depends on two cells."""
    return f"{len(agent_episodes)} / {len(raw_data)} episodes are agent-related"

pipeline = Pipeline([raw_data, agent_episodes, summary, comparison])
pipeline.run()

# Now change raw_data's function body:
# → agent_episodes re-runs (depends on raw_data)
# → summary re-runs (depends on agent_episodes)
# → comparison re-runs (depends on both)
# → Nothing else runs
```

## Architecture

```
dag/
├── __init__.py
├── cell.py          # @cell decorator, Cell dataclass
├── graph.py         # DAGGraph: build, topological sort, change detection
├── engine.py        # PipelineEngine: execute, invalidate, re-run
├── store.py         # CellStore: cache outputs, track mtimes
├── renderer.py      # Rich terminal: show execution status per cell
└── cli.py           # `dag run`, `dag status`, `dag invalidate`

examples/
├── p3_analysis.py   # P³ podcast data analysis pipeline
└── git_stats.py     # Git commit analysis pipeline

tests/
├── test_cell.py
├── test_graph.py
└── test_engine.py

pyproject.toml
README.md
```

## Implementation Phases

### Phase 1: Cell decorator (cell.py)

The `@cell` decorator transforms a function into a managed node in the DAG.

```python
@dataclass
class Cell:
    name: str                    # function name
    func: Callable
    depends_on: list[Cell]       # explicit upstream dependencies
    output: Any                  # cached result
    status: str                  # pending | running | done | error | stale
    last_run: datetime | None
    duration_seconds: float | None
    error: Exception | None

def cell(func=None, *, depends_on: list = []):
    """Decorator. Usage: @cell or @cell(depends_on=[other_cell])"""
    ...
```

Key: `depends_on` takes the actual function objects (not strings), so Python's name resolution handles it naturally.

### Phase 2: DAG graph (graph.py)

Build the directed graph from cell dependency declarations and validate it.

```python
class DAGGraph:
    def build(self, cells: list[Cell]) -> nx.DiGraph:
        """Build networkx DiGraph from cell.depends_on declarations."""

    def topological_order(self) -> list[Cell]:
        """Return cells in execution order (all deps before dependents)."""

    def validate(self):
        """Raise CycleError if graph has cycles. Raise on unknown deps."""

    def descendants(self, cell: Cell) -> list[Cell]:
        """All cells that transitively depend on this cell."""

    def ancestors(self, cell: Cell) -> list[Cell]:
        """All cells this cell transitively depends on."""
```

Cycle detection: `nx.is_directed_acyclic_graph()`. If False, find and report the cycle with `nx.find_cycle()`.

### Phase 3: Execution engine (engine.py)

The core reactive logic.

```python
class PipelineEngine:
    def run_all(self):
        """Execute all cells in topological order."""

    def invalidate(self, cell: Cell):
        """Mark cell and all descendants as stale."""

    def run_stale(self):
        """Re-execute only stale cells, in topological order."""

    def update(self, cell: Cell, new_func: Callable):
        """Update a cell's function, invalidate it and descendants, re-run stale."""
```

Execution of a single cell:
1. Check all `depends_on` cells are in `done` status
2. Collect their outputs as arguments
3. Call `cell.func(*args)` — args are outputs of deps, in depends_on order
4. Store result in `cell.output`
5. Set status to `done`, record `last_run` and `duration_seconds`
6. On exception: set status to `error`, store exception, mark all descendants as `error` too

### Phase 4: Output cache (store.py)

Persist cell outputs to disk so pipelines survive across sessions.

```python
class CellStore:
    # ~/.dag/cache/{pipeline_name}/{cell_name}.pkl
    def save(self, cell: Cell): ...
    def load(self, cell: Cell) -> Any | None: ...
    def invalidate(self, cell: Cell): ...
    def is_fresh(self, cell: Cell) -> bool:
        """True if cached output exists and cell hasn't been modified."""
```

Freshness check: compare `cell.func.__code__.co_consts` hash against stored hash. If function body changed → stale.

### Phase 5: Terminal renderer (renderer.py)

Show pipeline execution status live using `rich.live` and `rich.table`.

```
Pipeline: p3_analysis
┌─────────────────────┬──────────┬──────────┬────────────┐
│ Cell                │ Status   │ Duration │ Last Run   │
├─────────────────────┼──────────┼──────────┼────────────┤
│ raw_data            │ ✓ done   │ 1.2s     │ 2 min ago  │
│ agent_episodes      │ ✓ done   │ 0.1s     │ 2 min ago  │
│ summary             │ 🔄 running│ ...      │            │
│ comparison          │ ⏳ pending│          │            │
└─────────────────────┴──────────┴──────────┴────────────┘
```

Color coding: green = done, yellow = stale, blue = running, red = error, grey = pending.

### Phase 6: Change detection and partial re-execution

The killer feature: when you edit a cell's function, only re-run what's needed.

```python
engine = PipelineEngine(pipeline)
engine.run_all()
# All 4 cells done.

# Edit agent_episodes filter condition:
engine.update(agent_episodes, new_func=new_agent_episodes)
# → raw_data: untouched (not downstream of agent_episodes)
# → agent_episodes: re-runs (changed)
# → summary: re-runs (downstream)
# → comparison: re-runs (downstream)
```

Implementation: `update()` calls `invalidate(cell)` which marks the cell and all `nx.descendants()` as stale, then calls `run_stale()`.

### Phase 7: CLI

```bash
dag run examples/p3_analysis.py          # run all cells
dag run examples/p3_analysis.py --watch  # re-run on file save (fswatch integration)
dag status examples/p3_analysis.py       # show cell status table
dag invalidate examples/p3_analysis.py --cell raw_data  # force re-run from cell
dag graph examples/p3_analysis.py        # print ASCII dependency graph
```

### Phase 8: P³ analysis example (examples/p3_analysis.py)

Build a real, useful analysis pipeline using P³ data:

```python
@cell
def raw_episodes():
    return load_from_duckdb("~/Code/parakeet-podcast-processor/data/p3.duckdb")

@cell(depends_on=[raw_episodes])
def agent_episodes(raw_episodes):
    return filter_by_topic(raw_episodes, "agent")

@cell(depends_on=[raw_episodes])
def recent_episodes(raw_episodes):
    return filter_by_date(raw_episodes, days=14)

@cell(depends_on=[agent_episodes, recent_episodes])
def recent_agent_episodes(agent_episodes, recent_episodes):
    ids = {e['id'] for e in recent_episodes}
    return [e for e in agent_episodes if e['id'] in ids]

@cell(depends_on=[recent_agent_episodes])
def topic_frequency(recent_agent_episodes):
    return count_topics(recent_agent_episodes)
```

This example makes the reactive model concrete: change the date filter → only downstream cells re-run.

## Key Design Decisions

**Why function objects as `depends_on`, not strings?**
Strings require a registry and fail silently on typos. Function objects get Python's scope resolution for free. If the dep doesn't exist, you get a `NameError` at definition time.

**Why pickle for output cache?**
Simplest approach for arbitrary Python objects. Downside: not human-readable. Document clearly. Alternative for production: JSON with custom encoder, but pickle is right for this project.

**Why not async execution?**
Independent cells (same DAG level) could run in parallel. Start synchronous. Async is a clear follow-on: change `run_stale()` to use `asyncio.gather()` for cells with no shared deps.

**Why hash the function bytecode for freshness?**
`func.__code__.co_consts` captures the function's compiled constants. A simpler approach (mtime of the .py file) would invalidate all cells in the file when any cell changes. Bytecode hashing gives per-cell granularity.

**What we're NOT building**
- Web UI (follow-on, see Marimo for reference)
- Streaming outputs (cells produce final results, not streams)
- Parallel execution (single-threaded first)
- Remote execution

## Acceptance Criteria

1. P³ analysis pipeline runs end-to-end: all cells execute in topological order
2. Edit one cell's function → only downstream cells re-run (verified by timing log)
3. Cycle detection: pipeline with A→B→A raises `CycleError` with cycle description
4. Output cache: close and reopen pipeline, unmodified cells load from cache (no re-execution)
5. `dag status` shows correct status for all cells after partial re-run

## Learning Outcomes

After building this you will understand:
- Why topological sort is the foundational algorithm for dependency-aware execution
- How spreadsheets, Make, React, and Marimo all implement the same core primitive
- What "change propagation" means in a dependency graph
- Why sequential notebooks create hidden state bugs (you'll feel this viscerally while building)
- How to hash function bytecode for change detection
