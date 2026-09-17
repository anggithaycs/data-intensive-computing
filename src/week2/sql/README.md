# Week 2 SQL

Start with [silver/monthly_zone_demand.sql](silver/monthly_zone_demand.sql). It is a complete query that counts trips by month and pickup zone.

## What belongs in a SQL file?

Keep the six analytical queries in separate files so they can be read and compared independently. Keep substantial reusable transformations here too. Only very short statements, such as an existence check or a one-line view copy, stay in Python.

| Folder | Contents |
|---|---|
| `silver/` | Six analytical queries over detailed data. |
| `gold/` | Six equivalent analytical queries over saved summaries. |
| `products/` | Daily mobility and taxi-zone aggregations. |
| `views/` | Input projection, coverage, the hourly calendar, hourly demand and weather exposure. |
| `benchmarks/` | Scoped trip input, hourly context lookup and joined trip input templates. |

Silver and Gold queries use matching filenames.

## What stays in Python?

- [analysis.py](../analysis.py) loads the shared view definitions from SQL files.
- [products.py](../products.py) keeps the two simple `SELECT *` view copies inline and loads the two larger product aggregations from files.
- [benchmark.py](../benchmark.py) keeps cache commands and the short empty-month check in Python. Input transformations are loaded from `benchmarks/`. All experiments reuse the six analytical SQL definitions.

The query dictionaries only associate names with loaded SQL. The product dictionary contains file references and the two one-line statements, not large SQL bodies.

## Loading and formatting

[sql_files.py](../sql_files.py) resolves files relative to its own location, independently of the working directory. SQL file reads and benchmark query construction happen outside the query timers.

The calendar SQL uses `{start}` and `{end}` placeholders supplied from observed date coverage. Benchmark templates receive a validated month and a controlled partition filter or broadcast hint from Python.

Use four-space indentation, one selected expression per line and separate lines for major clauses. The same convention applies to inline SQL. Ruff checks Python formatting, but does not reformat SQL inside strings or SQL files.
