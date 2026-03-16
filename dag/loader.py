"""Load a pipeline definition from a Python file."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from dag.cell import Cell


def load_pipeline(path: str) -> list[Cell]:
    """Import a Python file and return its Cell objects.

    The file should either define a ``CELLS`` list or the loader
    will collect all module-level ``Cell`` instances.
    """
    filepath = Path(path).resolve()
    if not filepath.exists():
        raise FileNotFoundError(f"Pipeline file not found: {filepath}")

    spec = importlib.util.spec_from_file_location("_pipeline", filepath)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {filepath}")

    module = importlib.util.module_from_spec(spec)
    sys.modules["_pipeline"] = module
    spec.loader.exec_module(module)

    # Prefer explicit CELLS list
    if hasattr(module, "CELLS"):
        return list(module.CELLS)

    # Otherwise collect all Cell instances from module namespace
    cells = [v for v in vars(module).values() if isinstance(v, Cell)]
    if not cells:
        raise ValueError(f"No Cell objects found in {filepath}")

    return cells
