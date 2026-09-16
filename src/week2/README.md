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

The benchmark checks that the answers agree and compares execution times. It also runs four focused experiments: caching, Adaptive Query Execution (AQE), partition pruning and broadcast joins.

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
2. **Focused optimizations:** choose one Silver query for each technique and change only that technique.

| Technique | Selected query | Change being measured |
|---|---|---|
| Caching | Weather vs trip distance | Read the same `trips` view before and after caching it. |
| AQE | Weather demand variation | Enable adaptive execution and shuffle partition coalescing. |
| Partition pruning | Pickup counts per zone for one month | Add explicit year/month partition filters to the existing date range. |
| Broadcast join | Monthly pickup counts from clean taxi trips joined with zone information | Add a broadcast hint for the small zone table. |

The full suite has 18 distinct cases: 12 Silver/Gold cases and six additional cases for the focused experiments. Caching and AQE reuse two Silver cases as their baselines. At the default settings, this gives 90 timed executions plus 18 warmups.

Each measurement includes query planning, execution and collection of the complete result. Every result is checked for equivalence outside the timer. The saved measurements include median times, cache population cost and Gold construction costs. Use these values to write your own report.

AQE and automatic broadcasting are disabled in the baseline. Automatic broadcasting stays disabled in the AQE experiment. The run order is fixed and operating-system and JVM caches remain warm, so these results describe this local setup.

## 6. Find the outputs

The input paths and Spark settings come from the Week 1 configuration. Week 2's output roots are defined at the top of [run.py](run.py).

```text
storage/delta/gold/week2_simple/
  daily_mobility_summary/
  taxi_zone_statistics/
  weather_impact_summary/
  air_quality_impact_summary/
  _metadata/

reports/week2_simple/runs/<execution_time>/
  <query_name>.json       # Full answers, when the queries stage runs
  products.json          # Gold sizes and build times, when products are built
  metrics.json           # Benchmark samples, SQL, plans and completion status
  plans/                 # Eight before/after plans for the four focused techniques
```

A Spark physical plan describes the scans, joins, aggregations and data exchanges used to answer a query. Each focused technique saves two executed plans: `<technique>_before.txt` for its baseline and `<technique>_after.txt` for its optimized version. AQE compares disabled versus enabled execution. There are eight standalone plan files in total; `metrics.json` also retains the executed plan for each measured case. No separate initial-plan capture is needed.

Each product refresh replaces the four Gold snapshots and updates their metadata. Metadata records the source, first creation time, last refresh time, schema version and row count. Creation time is preserved across refreshes; update the schema version in code when changing a product definition.

Writes commit separately for each table. If a build fails, rerun `products` before using Gold. Concurrent refreshes are outside this implementation's scope. The `week2_simple` output root also keeps these tables separate from outputs left by earlier implementations.

## 7. Read the code and run checks

SQL lives in separate files under [sql/](sql/README.md). Python loads those files and handles execution, storage and timing. A useful reading order is:

| File | What to look for |
|---|---|
| [sql/silver/monthly_zone_demand.sql](sql/silver/monthly_zone_demand.sql) | Start with one complete analytical query, then explore the other Silver queries. |
| [analysis.py](analysis.py) | `prepare_views()` loads the shared SQL views; `QUERY_KEYS` defines how result rows are matched. |
| [products.py](products.py) | Builds the summaries defined in `sql/products/` and loads the analytical queries in `sql/gold/`. |
| [run.py](run.py) | `main()` connects configuration, table loading, query execution, product builds and benchmarks. |
| [benchmark.py](benchmark.py) | `check_equal()` checks answers, `measure()` times queries and `run_benchmarks()` saves measurements and plans. Focused experiment SQL is in `sql/benchmarks/`. |

The Silver and Gold folders use matching query filenames so their definitions can be compared directly. Small Python dictionaries associate query names with loaded SQL; query bodies remain in the SQL files. Temporary views are SQL definitions; data is saved only through a cache or a Delta write.

Run these checks from the project root:

```powershell
.venv/Scripts/python.exe tests/test_week2_simple.py
.venv/Scripts/python.exe -m ruff check src/week2 tests/test_week2_simple.py
.venv/Scripts/python.exe -m ruff format --check src/week2 tests/test_week2_simple.py
```

The tests use five hand-built trips to check the six answers, Silver/Gold equivalence, zero-demand hours, daylight-saving changes, missing PM2.5, tied peaks, repeated refreshes, metadata creation times and comparison failures. Test writes go to unique `.test-output/week2-simple-*` directories.

Write and maintain the Markdown reports manually using `metrics.json`, `products.json` and the saved plans. The runner does not create or overwrite Markdown reports, and there is no automatic PDF packaging step. Existing reports are retained as records of earlier runs.

Ruff formats and checks Python; it does not format SQL inside strings or separate SQL files. SQL uses four-space indentation, one selected expression per line, and separate lines for major clauses. Keep blank lines between meaningful Python processing steps; Ruff permits those within functions.
