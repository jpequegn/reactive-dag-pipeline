"""CellStore — persist cell outputs with bytecode-based freshness."""

from __future__ import annotations

import hashlib
import json
import pickle
from pathlib import Path
from typing import Any

from dag.cell import Cell


def _hash_code(h: hashlib._Hash, code: object) -> None:
    """Recursively hash a code object, handling nested code (genexprs, lambdas)."""
    import types

    if not isinstance(code, types.CodeType):
        return
    h.update(code.co_code)
    for const in code.co_consts:
        if isinstance(const, types.CodeType):
            _hash_code(h, const)
        else:
            h.update(repr(const).encode())


def _bytecode_hash(cell: Cell) -> str:
    """SHA-256 of the cell's function bytecode and constants (recursive)."""
    h = hashlib.sha256()
    _hash_code(h, cell.func.__code__)
    return h.hexdigest()


class CellStore:
    """Cache cell outputs to disk with bytecode-based freshness checks."""

    def __init__(self, cache_dir: str | Path) -> None:
        self._cache_dir = Path(cache_dir).expanduser()
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def _output_path(self, cell: Cell) -> Path:
        return self._cache_dir / f"{cell.name}.pkl"

    def _sidecar_path(self, cell: Cell) -> Path:
        return self._cache_dir / f"{cell.name}.json"

    def save(self, cell: Cell) -> None:
        """Pickle cell output and store bytecode hash in a JSON sidecar."""
        with open(self._output_path(cell), "wb") as f:
            pickle.dump(cell.output, f)

        sidecar = {"bytecode_hash": _bytecode_hash(cell)}
        with open(self._sidecar_path(cell), "w") as f:
            json.dump(sidecar, f)

    def load(self, cell: Cell) -> Any | None:
        """Return cached output if the bytecode hash still matches, else None."""
        if not self.is_fresh(cell):
            return None
        with open(self._output_path(cell), "rb") as f:
            return pickle.load(f)  # noqa: S301

    def is_fresh(self, cell: Cell) -> bool:
        """True if cached output exists and function body is unchanged."""
        sidecar_path = self._sidecar_path(cell)
        output_path = self._output_path(cell)

        if not sidecar_path.exists() or not output_path.exists():
            return False

        with open(sidecar_path) as f:
            sidecar = json.load(f)

        return sidecar.get("bytecode_hash") == _bytecode_hash(cell)

    def invalidate(self, cell: Cell) -> None:
        """Delete cache file and sidecar for a cell."""
        self._output_path(cell).unlink(missing_ok=True)
        self._sidecar_path(cell).unlink(missing_ok=True)
