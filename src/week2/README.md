# Week 2: run and understand the analytics

Week 2 uses the cleaned taxi data from Week 1 to answer six questions about demand, weather and air quality. It saves reusable summaries and measures whether those summaries and selected Spark optimizations make queries faster.

Two terms appear throughout the documentation:

- **Silver** is the cleaned, detailed data. The integrated Silver table contains one row per accepted taxi trip, with location, weather and air-quality context.
- **Gold** contains saved summaries of Silver data. Queries can reuse these summaries instead of repeating work over millions of trips.

For the reasoning behind the queries, read the [design report](../../reports/week2/design_report.md). For measured results, open the [reports index](../../reports/week2/README.md).

## 1. Understand the workflow

```text
Week 1 Silver tables
        |
        v
Shared SQL views
        |
        +--> Six queries over Silver --> Analytical answers
        |
        +--> Build four Gold tables --> Six queries over Gold
                                                |
                                                v
                                      The same analytical answers
```

The benchmark checks that the answers agree and compares execution times. It also runs four techniques on all six queries: caching, Adaptive Query Execution (AQE), partition pruning and broadcast joins.

Week 2 reads the existing Silver tables; it does not rerun ingestion. It uses the Week 1 Spark environment and requires no additional runtime dependencies.

## 2. Run your first query

Start from the project root after completing Week 1. Set the Java path to your installed JDK:

```powershell
$env:JAVA_HOME = 'C:/path/to/jdk-21'
$env:PYSPARK_PYTHON = (Resolve-Path .venv/Scripts/python.exe).Path

.venv/Scripts/python.exe -m src.week2.run queries --query monthly_zone_demand
```

This query counts pickups by month and zone. The runner prints the first five rows and saves the complete answer as JSON in a new run directory.

## 3. Choose what to run

Use the same environment settings for all commands below.

| Task | Command |
|---|---|
| Run all six queries over Silver | `.venv/Scripts/python.exe -m src.week2.run queries` |
| Build or refresh the four Gold tables | `.venv/Scripts/python.exe -m src.week2.run products` |
| Query an existing Gold table | `.venv/Scripts/python.exe -m src.week2.run queries --gold --query monthly_zone_demand` |
| Rebuild Gold and run the full benchmark | `.venv/Scripts/python.exe -m src.week2.run benchmark` |
| Rebuild Gold, save all six Silver answers and run the benchmark | `.venv/Scripts/python.exe -m src.week2.run all` |

The optional arguments are:

- `--query <query_id>` selects an analytical query for the `queries` stage, including that stage within `all`. It does not narrow the benchmark.
- `--repeats 1` runs a quick benchmark check. The default is five measured repetitions per case, after one warmup.
- `--month 2024-02` selects the month for the partition-pruning experiment. That month must contain trips.

Keep Silver unchanged during a run. After changing Silver data or query definitions, rebuild Gold before using `queries --gold`. The `benchmark` and `all` commands rebuild it automatically.

## 4. Know what the six queries answer

Demand means the number of taxi pickups.

| Query ID | Question | Main result |
|---|---|---|
| `monthly_zone_demand` | How many pickups occur in each zone each month? | Trip counts by month and pickup zone. |
| `weather_trip_distance` | How does average distance differ between dry and wet weather? | Average distance, trip count and valid-distance count for dry, wet and unknown conditions. |
| `air_quality_demand` | How are hourly demand and citywide PM2.5 related? | Correlation, total hours and hours with air-quality data. |
| `weather_demand_variation` | Which zones have the largest difference in demand between dry and wet hours? | Zones ranked by the absolute difference in average pickups per hour. |
| `weekday_peak_hours` | Which hours are busiest on each weekday? | Average demand and rank for each weekday/hour; rank 1 identifies peaks. |
| `monthly_demand_trends` | How does demand change from month to month? | Monthly trips, observed days, daily average and percentage change. |

Calendar fields use New York time; environmental observations are matched by UTC hour. The hourly calendar includes hours with zero trips and handles daylight-saving changes. Missing weather stays `unknown`.

These choices affect interpretation. In particular, zero trips and missing input data cannot be distinguished by the calendar alone, and correlation does not establish causation. The [design report](../../reports/week2/design_report.md) explains coverage, missing values, ties and partial months in detail.

## 5. Understand what the benchmark compares

The benchmark has two parts:

1. **Silver vs Gold:** run all six analytical queries both ways and compare the time needed to produce the same answers.
2. **Optimization matrix:** test caching, AQE, partition pruning and broadcast joins on every analytical query, giving 24 before/after comparisons.

| Technique | Input and change being measured |
|---|---|
| Caching | All six full-period queries read the same `trips` view before and after caching it. Population cost is recorded separately. |
| AQE | All six full-period queries run with adaptive execution and shuffle coalescing off, then on. Automatic broadcasting stays disabled. |
| Partition pruning | Each query uses identical date bounds before and after adding year/month partition predicates. Most use `--month`; monthly trends includes the previous month so its selected-month change can be calculated. |
| Broadcast join | All six queries run over clean trips joined to zones and a materialized hourly weather/PM2.5 lookup, without and with broadcast hints. Both answers must also match integrated Silver. |

Broadcast tests measure joins needed to reconstruct the analytical input. The hourly lookup is derived from integrated trips, and its preparation cost is recorded separately. It is reused on both sides. These tests do not measure ingestion or a broadcast hint applied directly to the already-enriched Silver table.

Pruning tests answer a bounded-period version of each question. Calendar coverage is determined from observed trip dates within that window and is identical on both sides. Monthly trends returns the previous and selected month when both are available; its first displayed month has no earlier comparison. Explicit partition filters may provide little benefit when date filters already enable file skipping or most available partitions are needed.

The suite has **48 distinct cases**: 12 Silver/Gold, six cached, six AQE, 12 pruning and 12 broadcast cases. Caching and AQE reuse the six Silver baselines. The default is **240 timed executions plus 48 warmups**.

Each measurement includes query planning, execution and collection of the complete result. Every result is checked for equivalence outside the timer. The saved measurements include median times, cache population cost and Gold construction costs. Use these values to write your own report.

AQE and automatic broadcasting are disabled in the baseline. Automatic broadcasting stays disabled in the AQE experiment. The run order is fixed and operating-system and JVM caches remain warm, so these results describe this local setup.

## 6. Find the outputs

The input paths and Spark settings come from the Week 1 configuration. Week 2's output roots are defined at the top of [run.py](run.py).

```text
storage/delta/gold/
  daily_mobility_summary/
  taxi_zone_statistics/
  weather_impact_summary/
  air_quality_impact_summary/
  _metadata/

storage/metrics/week2/runs/<execution_time>/
  <query_name>.json       # Full answers, when the queries stage runs
  products.json          # Gold sizes and build times, when products are built
  metrics.json           # Benchmark samples, SQL, plans and completion status
  plans/                 # 24 before/after pairs, grouped by technique
```

A Spark physical plan describes scans, joins, aggregations and exchanges. Each comparison saves two executed `EXPLAIN FORMATTED` plans under `plans/<technique>/<query>_before.txt` and `<query>_after.txt`: 48 files for 24 pairs. There are no separate pre-execution captures.

In `metrics.json`, `comparisons` groups every query by technique and provides before/after medians, speedup, equivalence, scope and plan paths. A speedup below 1 means the optimized case was slower. `cases` retains individual samples, executed plans and SQL; scoped and broadcast cases also record the replacement `trips` SQL. The six Silver/Gold comparisons are included separately. Cache and broadcast-lookup population costs and product build costs must be considered when discussing amortization.

Each product refresh replaces the four Gold snapshots and updates their metadata. Metadata records the source, first creation time, last refresh time, schema version and row count. Creation time is preserved across refreshes; update the schema version in code when changing a product definition.

Writes commit separately for each table. If a build fails, rerun `products` before using Gold. Concurrent refreshes are outside this implementation's scope.

## 7. Read the code and run checks

The analytical queries and substantial transformations live under [sql/](sql/README.md). Only very short checks, cache commands and one-line view copies stay inline in Python. A useful reading order is:

| File | What to look for |
|---|---|
| [sql/silver/monthly_zone_demand.sql](sql/silver/monthly_zone_demand.sql) | Start with one complete analytical query, then explore the other Silver queries. |
| [analysis.py](analysis.py) | `prepare_views()` loads the shared SQL views; `QUERY_KEYS` defines how result rows are matched. |
| [products.py](products.py) | Loads the two larger aggregations from `sql/products/`, keeps two simple view copies inline, and loads analytical queries from `sql/gold/`. |
| [run.py](run.py) | `main()` connects configuration, table loading, query execution, product builds and benchmarks. |
| [benchmark.py](benchmark.py) | `check_equal()` checks answers, `measure()` times queries and `run_benchmarks()` saves measurements and plans. Benchmark input definitions are loaded from `sql/benchmarks/`. |

The Silver and Gold folders use matching query filenames so their definitions can be compared directly. Small Python dictionaries associate analytical query names with loaded SQL. Projections, coverage queries and benchmark templates stay in SQL files. Only very short checks and one-line view copies remain inline. Temporary views are SQL definitions; data is saved only through a cache or a Delta write.

Run these checks from the project root:

```powershell
.venv/Scripts/python.exe tests/test_week2_simple.py
.venv/Scripts/python.exe -m ruff check src/week2 tests/test_week2_simple.py
.venv/Scripts/python.exe -m ruff format --check src/week2 tests/test_week2_simple.py
```

The tests use five hand-built trips to check the six answers, Silver/Gold equivalence, zero-demand hours, daylight-saving changes, missing PM2.5, tied peaks, repeated refreshes, metadata creation times comparison failures, and the complete 24-comparison optimization matrix on partitioned Delta input. Test writes go to unique `.test-output/week2-simple-*` directories.

Write and maintain the Markdown reports manually using `metrics.json`, `products.json` and the saved plans. The runner does not create or overwrite Markdown reports, and there is no automatic PDF packaging step. Existing reports are retained as records of earlier runs.

Ruff formats and checks Python; it does not format SQL inside strings or separate SQL files. SQL uses four-space indentation, one selected expression per line, and separate lines for major clauses. Keep blank lines between meaningful Python processing steps; Ruff permits those within functions.
