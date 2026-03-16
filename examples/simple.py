"""Simple example pipeline for testing CLI commands."""

from dag.cell import cell

@cell
def raw_data():
    return [1, 2, 3, 4, 5]

@cell(depends_on=[raw_data])
def filtered(raw_data):
    return [x for x in raw_data if x > 2]

@cell(depends_on=[filtered])
def summary(filtered):
    return f"{len(filtered)} items match"

@cell(depends_on=[raw_data, filtered])
def comparison(raw_data, filtered):
    return f"{len(filtered)} / {len(raw_data)} items"

CELLS = [raw_data, filtered, summary, comparison]
