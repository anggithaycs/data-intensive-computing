# Week 2 SQL

Start with [silver/monthly_zone_demand.sql](silver/monthly_zone_demand.sql). It is a complete query that counts trips by month and pickup zone.

| Folder | Purpose |
|---|---|
| `views/` | Prepare shared inputs, the hourly calendar and coverage statistics. |
| `silver/` | Answer the six analytical questions from detailed data. |
| `products/` | Define the four aggregations saved as Gold tables. |
| `gold/` | Answer the same six questions using those saved summaries. |
| `benchmarks/` | Define the monthly-filter and zone-join comparisons. Caching and AQE reuse Silver queries. |

Silver and Gold queries have matching filenames. Compare a pair to see how preaggregation changes the work needed to produce the same answer.

## Loading and running

[sql_files.py](../sql_files.py) reads files relative to its own location, so loading does not depend on the current working directory. Python registers the temporary views, reads the SQL and passes it to Spark. Files are loaded outside benchmark timers.

Most files can run directly once their input views exist. Three kinds of internal placeholders are filled by Python:

- `views/calendar_hours.sql` uses `{start}` and `{end}` from the observed date coverage.
- The monthly benchmark files use `{month_start}` from a validated date. `monthly_pickups.sql` also uses `{partition_filter}` for an empty string or the explicit year/month predicates.
- `benchmarks/zone_join.sql` uses `{hint}` for an empty string or the fixed broadcast hint.

These substitutions are controlled by the runner, not arbitrary user-supplied SQL. Small operational commands such as enabling the Spark cache remain in Python.

## Formatting

Use four spaces for indentation, one selected expression per line and separate lines for major clauses. Indent common table expressions, `CASE` branches and join conditions consistently. Ruff checks Python only; it does not format these SQL files.
