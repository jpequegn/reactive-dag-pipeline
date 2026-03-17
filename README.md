# Reactive DAG Pipeline

A lightweight reactive DAG execution engine. Define cells with explicit dependencies — change one cell and only its downstream dependents re-execute.

## Install

```bash
uv pip install -e .
```

## Quick Start

```python
from dag import cell, PipelineEngine

@cell
def raw_data():
    return [1, 2, 3, 4, 5]

@cell(depends_on=[raw_data])
def filtered(raw_data):
    return [x for x in raw_data if x > 2]

@cell(depends_on=[filtered])
def summary(filtered):
    return f"{len(filtered)} items"

engine = PipelineEngine([raw_data, filtered, summary])
engine.run_all()
print(summary.output)  # "3 items"
```

## CLI

```bash
dag run examples/p3_analysis.py          # run all cells with live status table
dag status examples/p3_analysis.py       # show cell states without executing
dag invalidate examples/p3_analysis.py --cell raw_episodes  # force re-run from cell
dag graph examples/p3_analysis.py        # print ASCII dependency graph
```

## Reactive Partial Re-execution Demo

The P³ analysis pipeline has 6 cells analyzing podcast episodes:

```
dag run examples/p3_analysis.py
```

All 6 cells execute. Now change `RECENT_DAYS` from 14 to 7 in the file:

```
dag run examples/p3_analysis.py
```

Only 4 cells re-execute (`recent_episodes` and its descendants). `raw_episodes` and `agent_episodes` load from cache — their function bodies haven't changed.

This is the core value: change one thing, re-run only what's affected.

## Architecture

```
dag/
├── cell.py       # @cell decorator, Cell dataclass
├── graph.py      # DAGGraph: build, validate, topological sort
├── engine.py     # PipelineEngine: run_all, invalidate, run_stale
├── store.py      # CellStore: persist outputs with bytecode freshness
├── renderer.py   # Rich terminal: live status table
├── loader.py     # Import pipeline files
└── cli.py        # dag run, status, invalidate, graph
```
