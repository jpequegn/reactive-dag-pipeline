"""Cell dataclass — a node in the reactive DAG."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable


@dataclass
class Cell:
    """A single computation node in the DAG."""

    name: str
    func: Callable
    depends_on: list[Cell] = field(default_factory=list)
    output: Any = None
    status: str = "pending"  # pending | running | done | error | stale
    last_run: datetime | None = None
    duration_seconds: float | None = None
    error: Exception | None = None

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Cell):
            return NotImplemented
        return self.name == other.name

    def __repr__(self) -> str:
        return f"Cell({self.name!r})"
