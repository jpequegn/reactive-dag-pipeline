"""Tests for PipelineRenderer and engine status change callbacks."""

from dag.cell import cell
from dag.engine import PipelineEngine
from dag.renderer import PipelineRenderer


class TestStatusChangeCallback:
    def test_callback_fires_on_status_changes(self) -> None:
        @cell
        def a():
            return 1

        @cell(depends_on=[a])
        def b(a):
            return a + 1

        changes: list[tuple[str, str]] = []

        def on_change(c, status):
            changes.append((c.name, status))

        engine = PipelineEngine([a, b], on_status_change=on_change)
        engine.run_all()

        # Should see: pending, pending, running, done, running, done
        assert ("a", "pending") in changes
        assert ("b", "pending") in changes
        assert ("a", "running") in changes
        assert ("a", "done") in changes
        assert ("b", "running") in changes
        assert ("b", "done") in changes

    def test_cached_cells_tracked(self, tmp_path) -> None:
        from dag.store import CellStore

        @cell
        def a():
            return 42

        store = CellStore(tmp_path / "cache")
        engine = PipelineEngine([a], store=store)
        engine.run_all()
        assert "a" not in engine.cached_cells

        # Reset and run again — should be cached
        a.output = None
        a.status = "pending"
        a.last_run = None

        engine2 = PipelineEngine([a], store=store)
        engine2.run_all()
        assert "a" in engine2.cached_cells


class TestRendererTableBuild:
    def test_build_table_has_correct_columns(self) -> None:
        @cell
        def a():
            return 1

        renderer = PipelineRenderer(name="test")
        renderer._cells = [a]
        renderer._engine = PipelineEngine([a])
        table = renderer._build_table()

        assert table.title == "Pipeline: test"
        assert len(table.columns) == 4
        col_names = [c.header for c in table.columns]
        assert "Cell" in str(col_names[0])
        assert "Status" in str(col_names[1])

    def test_status_method_prints_table(self, capsys) -> None:
        @cell
        def a():
            return 1

        a.status = "done"
        a.duration_seconds = 0.5

        renderer = PipelineRenderer(name="test")
        renderer.status([a])
        # Just verify it doesn't crash — Rich output goes to console
