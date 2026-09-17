# Week 2 submission and reproduction guide

This folder contains the submission deliverables for Week 2 analytical processing and query optimization.

| Document | Purpose |
|---|---|
| [Design report](design_report.md) | Analytical requirements, query design, Gold products, optimization strategy, engineering decisions and trade-offs. |
| [Benchmark report](benchmark_report.md) | Methodology, before/after timings comparing every query across all five optimization techniques, physical plan analysis, and performance discussion. |
| [Benchmark Run Data](../../storage/metrics/week2/runs/20260917_161256_727995/) | Measurements for run `20260917_161256_727995`, including `comparison.json`, `metrics.json`, and physical execution plans under `plans/`. |

For the detailed codebase walkthrough and SQL architecture, see [src/week2/README.md](../../src/week2/README.md).

## 1. Prerequisites

Run commands from the project root using Python 3.10–3.12, Java 17 or 21, and the pinned dependencies:

```powershell
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r requirements.txt
$env:JAVA_HOME = 'C:/path/to/jdk-21'
$env:PYSPARK_PYTHON = (Resolve-Path .venv/Scripts/python.exe).Path
```

On Windows, ensure `winutils.exe` and `hadoop.dll` are present in `HADOOP_HOME/bin` (or `C:/hadoop`). Week 2 reads the existing Week 1 Silver Delta tables (`integrated_taxi_trips`, clean taxi trips, and zone lookup).

## 2. Quick reproduction commands

### Run analytical queries over Silver
```powershell
# Run all six Silver queries
.venv/Scripts/python.exe -m src.week2.run queries

# Or run a single query (e.g. monthly_zone_demand)
.venv/Scripts/python.exe -m src.week2.run queries --query monthly_zone_demand
```

### Build and query Gold products
```powershell
# Build or refresh the four Gold Delta tables
.venv/Scripts/python.exe -m src.week2.run products

# Query Gold tables
.venv/Scripts/python.exe -m src.week2.run queries --gold
```

### Reproduce the benchmark suite
To rebuild Gold and run the complete benchmark matrix (testing all six queries across all five optimizations):

```powershell
.venv/Scripts/python.exe -m src.week2.run benchmark --repeats 5 --month 2024-02
```

To also execute the queries stage and save all Silver answers in the same run:
```powershell
.venv/Scripts/python.exe -m src.week2.run all --repeats 5 --month 2024-02
```

The benchmark runs **48 distinct cases** (8 configurations $\times$ 6 queries), yielding 240 measured executions across 5 repeats (plus 48 warmups):
1. **Silver vs Gold**: All 6 queries over full-period detailed Silver vs preaggregated Gold products.
2. **Caching**: All 6 queries over uncached vs in-memory cached `trips`.
3. **AQE & Coalescing**: All 6 queries with AQE off vs on (isolating shuffle coalescing).
4. **Partition Pruning**: All 6 queries with date filter alone vs date + explicit year/month partition filters on February 2024.
5. **Broadcast Join**: All 6 queries joining clean trips with zone and hourly context lookups without vs with broadcast hints.

## 3. Benchmark outputs

Benchmark executions store full artifacts under:
```text
storage/metrics/week2/runs/<execution_time>/
  comparison.json     # Before/after timings, speedups, and plan references
  metrics.json        # Status, samples, SQL, and plan text
  products.json       # Gold table sizes and build durations
  plans/              # Physical EXPLAIN FORMATTED plans grouped by technique
```

The canonical benchmark measurements analyzed in the report are recorded in [`storage/metrics/week2/runs/20260917_161256_727995/`](../../storage/metrics/week2/runs/20260917_161256_727995/).

## 4. Automated tests

Run the test suite and linters from the project root:

```powershell
.venv/Scripts/python.exe tests/test_week2_simple.py
.venv/Scripts/python.exe -m pip install -r requirements-dev.txt
.venv/Scripts/python.exe -m ruff check src/week2 tests/test_week2_simple.py
.venv/Scripts/python.exe -m ruff format --check src/week2 tests/test_week2_simple.py
```

