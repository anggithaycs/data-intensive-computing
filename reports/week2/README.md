# Week 2 submission and reproduction guide

This folder contains the submission reports and a copy of the benchmark evidence used in them.

| Document | Purpose |
|---|---|
| [Design report](design_report.md) / [PDF](design_report.pdf) | Analytical requirements, query design, Gold products, optimization strategy, engineering decisions and trade-offs. The PDF fixes the design report at four pages. |
| [Benchmark report](benchmark_report.md) / [PDF](benchmark_report.pdf) | Methodology, before/after timings, physical-plan analysis and performance discussion. |
| [Evidence](evidence/metrics.json) | Original measurements for run `20260916_184848_404205`, with [product costs](evidence/products.json) and [physical plans](evidence/plans/). |

Markdown is the editable source. The PDFs are submission snapshots, prepared once; the analytical runner does not generate or overwrite these reports.

## 1. Prerequisites

Run commands from the project root. Use Python 3.10-3.12, Java 17 or 21, and the pinned packages in [requirements.txt](../../requirements.txt). Week 1 must already have produced the integrated trips, clean taxi trips and zone lookup as Delta tables. Week 2 reads these tables; it does not ingest the source files again.

For a new Python environment:

```powershell
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r requirements.txt
```

Before running Spark, set paths for your machine:

```powershell
$env:JAVA_HOME = 'C:/path/to/jdk-21'
$env:PYSPARK_PYTHON = (Resolve-Path .venv/Scripts/python.exe).Path
```

On Windows, compatible `winutils.exe` and `hadoop.dll` must be available under `HADOOP_HOME/bin`; the shared Spark setup detects `C:/hadoop` when present. Spark needs access to its Java dependency cache and may download Delta dependencies on first use.

Check input paths and Spark settings in [platform_config.yaml](../../config/platform_config.yaml). The reported benchmark used local execution and eight shuffle partitions. Keep Silver unchanged throughout a run.

## 2. Run the analytical queries

Run one Silver query first:

```powershell
.venv/Scripts/python.exe -m src.week2.run queries --query monthly_zone_demand
```

Run all six Silver queries:

```powershell
.venv/Scripts/python.exe -m src.week2.run queries
```

The available query IDs are:

| Query ID | Answer |
|---|---|
| `monthly_zone_demand` | Pickup counts by month and zone. |
| `weather_trip_distance` | Average distance by precipitation category. |
| `air_quality_demand` | Hourly citywide PM2.5/demand correlation. |
| `weather_demand_variation` | Zone ranking by absolute wet/dry demand-rate difference. |
| `weekday_peak_hours` | Demand profile and peak ranks for each weekday. |
| `monthly_demand_trends` | Monthly totals, daily averages and percentage changes. |

The runner prints the first five rows and saves each complete answer as JSON. The SQL is in [src/week2/sql/silver/](../../src/week2/sql/silver/); shared input views are in [sql/views/](../../src/week2/sql/views/).

## 3. Generate and query the Gold products

Build or refresh all four products:

```powershell
.venv/Scripts/python.exe -m src.week2.run products
```

This writes the daily mobility, taxi zone, weather impact and air quality impact summaries under `storage/delta/gold/week2_simple/`, together with their metadata. Definitions are in [sql/products/](../../src/week2/sql/products/).

Run all six queries against the saved Gold tables, or select one:

```powershell
.venv/Scripts/python.exe -m src.week2.run queries --gold
.venv/Scripts/python.exe -m src.week2.run queries --gold --query monthly_zone_demand
```

Gold SQL is in [sql/gold/](../../src/week2/sql/gold/), using the same filenames as the Silver queries. Rebuild Gold after changing Silver or SQL. A refresh overwrites the current Gold snapshots; it does not change Silver. Writes commit per table, so rerun `products` after a failed build before consuming Gold.

## 4. Reproduce the benchmark experiments

To rebuild Gold and run the same experiment configuration as the report:

```powershell
.venv/Scripts/python.exe -m src.week2.run benchmark --repeats 5 --month 2024-02
```

To also save all six Silver answers in the same execution:

```powershell
.venv/Scripts/python.exe -m src.week2.run all --repeats 5 --month 2024-02
```

The benchmark automatically performs these comparisons:

1. All six Silver queries versus their equivalent Gold queries.
2. Weather-distance SQL before and after caching `trips`.
3. Weather-demand variation with AQE off and on.
4. February pickup counts with date-only versus explicit year/month filters.
5. A taxi/zone join without versus with a broadcast hint.

There are 18 distinct cases and 90 timed executions at five repeats, plus one warmup per case. Caching and AQE reuse two Silver baselines. Each timed execution collects the entire answer and checks equivalence outside the timer. Cache population and Gold build costs are saved separately.

Use `--repeats 1` for a quick smoke check, not as a substitute for the reported benchmark. `--month` must select a month containing trips. `--query` only narrows the analytical query stage; it never narrows the benchmark. Timings will vary with hardware, cache state and other machine activity.

## 5. Locate and interpret the evidence

New runtime output continues to use the existing location:

```text
reports/week2_simple/runs/<execution_time>/
  <query_name>.json   # When the queries stage runs
  products.json      # Gold sizes and build times
  metrics.json       # Status, samples, statistics, SQL and plans
  plans/             # Eight files: before/after for each focused technique
```

The terminal prints the exact output directory. Confirm `metrics.json` has `"status": "success"` before using the whole run in a report. Use median times for the main comparison, and consult the individual samples, minimum, maximum and standard deviation when discussing variability.

The submission's [evidence folder](evidence/metrics.json) copies the successful run named above without changing its measurements. Its `plans/` directory contains eight files: baseline and optimized versions for each focused technique. Historical `metrics.json` is kept unchanged, including its embedded plans. [provenance.json](evidence/provenance.json) identifies the source and SHA-256 hashes of copied files. Original runs remain under `reports/week2_simple/runs/`.

All eight files contain executed plans in Spark's formatted explain output. Compare the two configurations for each technique:

| Technique | Before | After |
|---|---|---|
| Caching | [Uncached](evidence/plans/caching_before.txt) | [Cached](evidence/plans/caching_after.txt) |
| Partition pruning | [Date-only filter](evidence/plans/partition_pruning_before.txt) | [Explicit partition filters](evidence/plans/partition_pruning_after.txt) |
| Broadcast join | [No hint](evidence/plans/broadcast_join_before.txt) | [Broadcast hint](evidence/plans/broadcast_join_after.txt) |
| AQE | [Disabled](evidence/plans/aqe_before.txt) | [Enabled](evidence/plans/aqe_after.txt) |

Future runs save these same eight filenames. Their `metrics.json` retains executed plans for all cases alongside the timings, but no separate pre-execution plans are captured.

The saved benchmark predates the extraction of SQL into separate files. The 12 analytical queries were checked to match the recorded SQL apart from whitespace, and the refactored code passed the small fixture tests. Run the commands above to obtain fresh measurements for the current code; do not relabel historical timings as a new execution.

## 6. Validate the implementation

```powershell
.venv/Scripts/python.exe tests/test_week2_simple.py
.venv/Scripts/python.exe -m pip install -r requirements-dev.txt
.venv/Scripts/python.exe -m ruff check src/week2 tests/test_week2_simple.py
.venv/Scripts/python.exe -m ruff format --check src/week2 tests/test_week2_simple.py
```

Tests check known answers, Silver/Gold equivalence, daylight-saving transitions, missing context, tied peaks, repeated refreshes and focused benchmark SQL. They write to unique directories beneath `.test-output/`. Ruff checks Python; the SQL files use a consistent manual layout.

For a deeper code walkthrough, see [src/week2/README.md](../../src/week2/README.md) and the [SQL guide](../../src/week2/sql/README.md).
