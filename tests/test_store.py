"""Tests for CellStore: save, load, is_fresh, invalidate, and engine integration."""

import pytest

from dag.cell import Cell, cell
from dag.engine import PipelineEngine
from dag.store import CellStore


@pytest.fixture
def cache_dir(tmp_path):
    return tmp_path / "cache"


class TestCellStore:
    def test_save_and_load(self, cache_dir) -> None:
        @cell
        def my_cell():
            return [1, 2, 3]

        my_cell.output = [1, 2, 3]
        store = CellStore(cache_dir)
        store.save(my_cell)

        result = store.load(my_cell)
        assert result == [1, 2, 3]

    def test_is_fresh_after_save(self, cache_dir) -> None:
        @cell
        def my_cell():
            return 42

        my_cell.output = 42
        store = CellStore(cache_dir)
        store.save(my_cell)

        assert store.is_fresh(my_cell) is True

    def test_not_fresh_when_no_cache(self, cache_dir) -> None:
        @cell
        def my_cell():
            return 42

        store = CellStore(cache_dir)
        assert store.is_fresh(my_cell) is False

    def test_not_fresh_after_func_change(self, cache_dir) -> None:
        @cell
        def my_cell():
            return 42

        my_cell.output = 42
        store = CellStore(cache_dir)
        store.save(my_cell)

        # Change the function body
        def new_func():
            return 99

        my_cell.func = new_func
        assert store.is_fresh(my_cell) is False

    def test_load_returns_none_when_stale(self, cache_dir) -> None:
        @cell
        def my_cell():
            return 42

        my_cell.output = 42
        store = CellStore(cache_dir)
        store.save(my_cell)

        def new_func():
            return 99

        my_cell.func = new_func
        assert store.load(my_cell) is None

    def test_invalidate_removes_cache(self, cache_dir) -> None:
        @cell
        def my_cell():
            return 42

        my_cell.output = 42
        store = CellStore(cache_dir)
        store.save(my_cell)
        assert store.is_fresh(my_cell) is True

        store.invalidate(my_cell)
        assert store.is_fresh(my_cell) is False


class TestEngineWithStore:
    def _build_pipeline(self):
        @cell
        def raw_data():
            return [1, 2, 3, 4, 5]

        @cell(depends_on=[raw_data])
        def filtered(raw_data):
            return [x for x in raw_data if x > 2]

        @cell(depends_on=[filtered])
        def summary(filtered):
            return f"{len(filtered)} items"

        return [raw_data, filtered, summary], raw_data, filtered, summary

    def test_second_run_loads_from_cache(self, cache_dir) -> None:
        """Acceptance: run, close, reopen — no cells re-execute."""
        cells, raw_data, filtered, summary = self._build_pipeline()
        store = CellStore(cache_dir)

        # First run: executes all cells and saves to cache
        engine = PipelineEngine(cells, store=store)
        engine.run_all()
        first_run_times = {c.name: c.last_run for c in cells}

        # Simulate "reopen": reset cell state
        for c in cells:
            c.output = None
            c.status = "pending"
            c.last_run = None

        # Second run: should load from cache, not re-execute
        engine2 = PipelineEngine(cells, store=store)
        engine2.run_all()

        # Outputs restored
        assert raw_data.output == [1, 2, 3, 4, 5]
        assert filtered.output == [3, 4, 5]
        assert summary.output == "3 items"

        # last_run should be None (loaded from cache, not executed)
        assert all(c.last_run is None for c in cells)

    def test_changed_func_triggers_reexecution(self, cache_dir) -> None:
        """Acceptance: change one cell — only that cell and descendants re-execute."""
        cells, raw_data, filtered, summary = self._build_pipeline()
        store = CellStore(cache_dir)

        engine = PipelineEngine(cells, store=store)
        engine.run_all()

        # Change filtered's function body
        def new_filtered(raw_data):
            return [x for x in raw_data if x > 3]

        filtered.func = new_filtered

        # Reset state to simulate reopen
        for c in cells:
            c.output = None
            c.status = "pending"
            c.last_run = None

        engine2 = PipelineEngine(cells, store=store)
        engine2.run_all()

        # raw_data loaded from cache (unchanged)
        assert raw_data.last_run is None
        # filtered and summary re-executed (func changed / dep changed)
        assert filtered.last_run is not None
        assert filtered.output == [4, 5]
        assert summary.output == "2 items"
