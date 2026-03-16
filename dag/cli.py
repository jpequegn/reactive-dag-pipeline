"""CLI entrypoint for the reactive DAG pipeline."""

import click


@click.group()
@click.version_option()
def main() -> None:
    """Reactive DAG Pipeline — build and run reactive computation graphs."""


if __name__ == "__main__":
    main()
