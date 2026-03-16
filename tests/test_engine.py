"""Tests for PipelineEngine: run_all, invalidate, run_stale, update."""

from dag.cell import cell
from dag.engine import PipelineEngine


def _build_pipeline():
    """4-cell pipeline matching the issue spec."""

    @cell
    def raw_data():
        return [1, 2, 3, 4, 5]

    @cell(depends_on=[raw_data])
    def agent_episodes(raw_data):
        return [x for x in raw_data if x > 2]

    @cell(depends_on=[agent_episodes])
    def summary(agent_episodes):
        return f"{len(agent_episodes)} items"

    @cell(depends_on=[raw_data, agent_episodes])
    def comparison(raw_data, agent_episodes):
        return f"{len(agent_episodes)} / {len(raw_data)}"

    cells = [raw_data, agent_episodes, summary, comparison]
    return cells, raw_data, agent_episodes, summary, comparison


class TestRunAll:
    def test_all_cells_done(self) -> None:
        cells, *_ = _build_pipeline()
        engine = PipelineEngine(cells)
        engine.run_all()
        assert all(c.status == "done" for c in cells)

    def test_outputs_are_correct(self) -> None:
        cells, raw_data, agent_episodes, summary, comparison = _build_pipeline()
        engine = PipelineEngine(cells)
        engine.run_all()

        assert raw_data.output == [1, 2, 3, 4, 5]
        assert agent_episodes.output == [3, 4, 5]
        assert summary.output == "3 items"
        assert comparison.output == "3 / 5"

    def test_duration_recorded(self) -> None:
        cells, *_ = _build_pipeline()
        engine = PipelineEngine(cells)
        engine.run_all()
        assert all(c.duration_seconds is not None for c in cells)
        assert all(c.last_run is not None for c in cells)


class TestInvalidate:
    def test_invalidate_root_marks_all_stale(self) -> None:
        cells, raw_data, agent_episodes, summary, comparison = _build_pipeline()
        engine = PipelineEngine(cells)
        engine.run_all()

        engine.invalidate(raw_data)
        assert raw_data.status == "stale"
        assert agent_episodes.status == "stale"
        assert summary.status == "stale"
        assert comparison.status == "stale"

    def test_invalidate_middle_preserves_upstream(self) -> None:
        cells, raw_data, agent_episodes, summary, comparison = _build_pipeline()
        engine = PipelineEngine(cells)
        engine.run_all()

        engine.invalidate(agent_episodes)
        assert raw_data.status == "done"  # upstream stays done
        assert agent_episodes.status == "stale"
        assert summary.status == "stale"
        assert comparison.status == "stale"


class TestRunStale:
    def test_only_stale_cells_reexecute(self) -> None:
        cells, raw_data, agent_episodes, summary, comparison = _build_pipeline()
        engine = PipelineEngine(cells)
        engine.run_all()

        # Record timestamps
        raw_last_run = raw_data.last_run

        engine.invalidate(agent_episodes)
        engine.run_stale()

        # raw_data should NOT have been re-executed
        assert raw_data.last_run == raw_last_run
        # All stale cells should now be done
        assert all(c.status == "done" for c in cells)

    def test_outputs_correct_after_run_stale(self) -> None:
        cells, raw_data, agent_episodes, summary, comparison = _build_pipeline()
        engine = PipelineEngine(cells)
        engine.run_all()

        engine.invalidate(agent_episodes)
        engine.run_stale()

        assert agent_episodes.output == [3, 4, 5]
        assert summary.output == "3 items"
        assert comparison.output == "3 / 5"


class TestUpdate:
    def test_update_replaces_func_and_reruns(self) -> None:
        cells, raw_data, agent_episodes, summary, comparison = _build_pipeline()
        engine = PipelineEngine(cells)
        engine.run_all()

        # Change agent_episodes to filter differently
        def new_agent_episodes(raw_data):
            return [x for x in raw_data if x > 3]

        engine.update(agent_episodes, new_agent_episodes)

        assert agent_episodes.output == [4, 5]
        assert summary.output == "2 items"
        assert comparison.output == "2 / 5"
        assert raw_data.output == [1, 2, 3, 4, 5]  # unchanged


class TestErrorHandling:
    def test_error_marks_cell_and_descendants(self) -> None:
        @cell
        def good():
            return 1

        @cell(depends_on=[good])
        def bad(good):
            raise ValueError("boom")

        @cell(depends_on=[bad])
        def downstream(bad):
            return bad + 1

        engine = PipelineEngine([good, bad, downstream])
        engine.run_all()

        assert good.status == "done"
        assert bad.status == "error"
        assert isinstance(bad.error, ValueError)
        assert downstream.status == "error"
