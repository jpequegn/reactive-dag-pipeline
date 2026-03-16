"""Tests for the @cell decorator."""

from dag.cell import Cell, cell


class TestCellDecorator:
    def test_bare_decorator_creates_cell(self) -> None:
        @cell
        def raw_data():
            return [1, 2, 3]

        assert isinstance(raw_data, Cell)
        assert raw_data.name == "raw_data"
        assert raw_data.depends_on == []
        assert raw_data.status == "pending"
        assert raw_data.output is None

    def test_bare_decorator_preserves_func(self) -> None:
        @cell
        def raw_data():
            return 42

        assert raw_data.func() == 42

    def test_decorator_with_depends_on(self) -> None:
        @cell
        def raw_data():
            return [1, 2, 3]

        @cell(depends_on=[raw_data])
        def filtered(raw_data):
            return [x for x in raw_data if x > 1]

        assert isinstance(filtered, Cell)
        assert filtered.name == "filtered"
        assert filtered.depends_on == [raw_data]

    def test_four_cell_pipeline(self) -> None:
        """Acceptance criteria: 4 cells with 2-level dependency chain."""

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

        # All are Cell instances
        assert all(isinstance(c, Cell) for c in [raw_data, agent_episodes, summary, comparison])

        # Correct depends_on
        assert raw_data.depends_on == []
        assert agent_episodes.depends_on == [raw_data]
        assert summary.depends_on == [agent_episodes]
        assert comparison.depends_on == [raw_data, agent_episodes]

        # All start as pending
        assert all(c.status == "pending" for c in [raw_data, agent_episodes, summary, comparison])

    def test_depends_on_takes_cell_objects(self) -> None:
        @cell
        def a():
            return 1

        @cell(depends_on=[a])
        def b(a):
            return a + 1

        assert b.depends_on[0] is a
