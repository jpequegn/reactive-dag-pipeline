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


def cell(func: Callable | None = None, *, depends_on: list[Cell] | None = None) -> Cell:
    """Decorator to create a Cell from a function.

    Usage:
        @cell
        def raw_data(): ...

        @cell(depends_on=[raw_data])
        def processed(raw_data): ...
    """
    if func is not None:
        # Called as @cell (no parentheses)
        return Cell(name=func.__name__, func=func, depends_on=depends_on or [])

    # Called as @cell(depends_on=[...])
    def wrapper(fn: Callable) -> Cell:
        return Cell(name=fn.__name__, func=fn, depends_on=depends_on or [])

    return wrapper  # type: ignore[return-value]
