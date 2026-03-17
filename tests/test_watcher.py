"""Tests for watcher: bytecode diffing and surgical invalidation logic."""

import textwrap
from pathlib import Path

from dag.cell import Cell, cell
from dag.store import _bytecode_hash
from dag.watcher import PipelineWatcher, _collect_hashes, _diff_hashes


class TestBytecodeDiffing:
    def test_identical_hashes_no_changes(self) -> None:
        @cell
        def a():
            return 1

        @cell(depends_on=[a])
        def b(a):
            return a + 1

        hashes = _collect_hashes([a, b])
        changed = _diff_hashes(hashes, hashes)
        assert changed == []

    def test_changed_function_detected(self) -> None:
        @cell
        def a():
            return 1

        old_hashes = _collect_hashes([a])

        # Change the function
        def new_a():
            return 999

        a.func = new_a
        new_hashes = _collect_hashes([a])

        changed = _diff_hashes(old_hashes, new_hashes)
        assert changed == ["a"]

    def test_unchanged_function_not_detected(self) -> None:
        @cell
        def a():
            return 1

        @cell(depends_on=[a])
        def b(a):
            return a + 1

        old_hashes = _collect_hashes([a, b])

        # Change only b
        def new_b(a):
            return a + 100

        b.func = new_b
        new_hashes = _collect_hashes([a, b])

        changed = _diff_hashes(old_hashes, new_hashes)
        assert changed == ["b"]
        assert "a" not in changed

    def test_new_cell_detected(self) -> None:
        old_hashes = {"a": "hash1"}
        new_hashes = {"a": "hash1", "b": "hash2"}
        changed = _diff_hashes(old_hashes, new_hashes)
        assert changed == ["b"]


class TestWatcherReloadAndDiff:
    def test_reload_detects_change(self, tmp_path: Path) -> None:
        pipeline_file = tmp_path / "pipeline.py"
        pipeline_file.write_text(textwrap.dedent("""\
            from dag.cell import cell

            @cell
            def a():
                return 1

            @cell(depends_on=[a])
            def b(a):
                return a + 1

            CELLS = [a, b]
        """))

        watcher = PipelineWatcher(str(pipeline_file))
        from dag.loader import load_pipeline

        watcher._cells = load_pipeline(str(pipeline_file))
        watcher._hashes = _collect_hashes(watcher._cells)

        # Modify cell a's function body
        pipeline_file.write_text(textwrap.dedent("""\
            from dag.cell import cell

            @cell
            def a():
                return 999

            @cell(depends_on=[a])
            def b(a):
                return a + 1

            CELLS = [a, b]
        """))

        changed = watcher._reload_and_diff()
        assert changed is not None
        assert "a" in changed
        assert "b" not in changed

    def test_reload_no_change_returns_none(self, tmp_path: Path) -> None:
        pipeline_file = tmp_path / "pipeline.py"
        pipeline_file.write_text(textwrap.dedent("""\
            from dag.cell import cell

            @cell
            def a():
                return 1

            CELLS = [a]
        """))

        watcher = PipelineWatcher(str(pipeline_file))
        from dag.loader import load_pipeline

        watcher._cells = load_pipeline(str(pipeline_file))
        watcher._hashes = _collect_hashes(watcher._cells)

        # Reload without changes
        changed = watcher._reload_and_diff()
        assert changed is None


class TestSurgicalInvalidation:
    def test_surgical_run_only_reruns_changed_and_descendants(
        self, tmp_path: Path
    ) -> None:
        """Changed cell + descendants re-run; upstream stays cached."""
        from dag.engine import PipelineEngine
        from dag.store import CellStore

        @cell
        def a():
            return [1, 2, 3]

        @cell(depends_on=[a])
        def b(a):
            return [x for x in a if x > 1]

        @cell(depends_on=[b])
        def c(b):
            return len(b)

        cells = [a, b, c]
        store = CellStore(tmp_path / "cache")

        # Initial run — all execute
        engine = PipelineEngine(cells, store=store)
        engine.run_all()
        assert all(cell.status == "done" for cell in cells)
        a_last_run = a.last_run

        # Simulate watch: b changed, surgical invalidation
        store.invalidate(b)
        store.invalidate(c)  # descendant of b

        # Fresh engine, run_all loads cache for a, re-runs b and c
        for cell_ in cells:
            cell_.output = None
            cell_.status = "pending"
            cell_.last_run = None

        engine2 = PipelineEngine(cells, store=store)
        engine2.run_all()

        # a should be cached
        assert "a" in engine2.cached_cells
        assert a.last_run is None  # not re-executed

        # b and c should have re-executed
        assert "b" not in engine2.cached_cells
        assert "c" not in engine2.cached_cells
        assert b.last_run is not None
        assert c.last_run is not None
