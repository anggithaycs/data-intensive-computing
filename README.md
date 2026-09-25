# Week 3 Urban Data Platform

This branch contains the Week 3 work for operating and maintaining the urban data platform built in Weeks 1 and 2. Week 3 focuses on incremental releases, schema evolution, validation, monitoring, analytical refreshes and platform evaluation.

## Week 3 status

### Task 1: Generating incremental update datasets

Task 1 is implemented. The generator creates a reproducible second release containing:

- Taxi: 7% new trips and 1% exact duplicate trips.
- Weather: a seven-day hourly continuation with a new numeric `humidity` column.
- Air Quality: a seven-day continuation preserving the source observation multiplicity with a new numeric `aqi` column.

The environmental release window is:

```text
2025-01-01 01:00:00 through 2025-01-08 00:00:00
```

Run the generator from the repository root:

```bash
PYTHONPATH=. .venv/bin/python scripts/generate_week3_task1.py
```

The generator uses a fixed seed (`20250925`), overwrites the update outputs, validates the generated schemas and writes measured evidence and the Task 1 report.

Generated outputs:

```text
data/updates/yellow_tripdata_2024_update01.parquet
data/updates/weather_update.csv
data/updates/air_quality_update.csv
storage/metrics/week3/task1_incremental_updates.json
reports/week3/task1.md
```

The Taxi update is a Spark Parquet directory and may contain multiple `part-*.parquet` files. Do not manually edit the generated files; rerun the generator when regenerating Task 1 data.

## Reproducing the environment

The project requires Python 3.10–3.12, Java 17 or 21, Apache Spark and Delta Lake. Create the virtual environment and install the pinned dependencies:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

The first Spark run may need network access to download the Delta JVM dependencies. On macOS or Linux, use:

```bash
PYTHONPATH=. .venv/bin/python scripts/generate_week3_task1.py
```

On Windows, use the equivalent `.venv\Scripts\python.exe` path and configure `JAVA_HOME` before starting Spark.

## Input datasets

The original Week 1 input datasets are local files under `data/` and are intentionally not modified by Week 3:

```text
data/yellow_tripdata_2024-01.parquet
data/yellow_tripdata_2024-02.parquet
data/yellow_tripdata_2024-03.parquet
data/weather.csv
data/hourly_88101_2024.csv
data/taxi_zone_lookup (1).csv
```

These raw datasets are excluded from Git because of their size. Obtain them using the assignment links and place them in the paths above before running the generator.

## Week 3 artifacts

| Path | Purpose |
|---|---|
| `scripts/generate_week3_task1.py` | Reproducible Task 1 update generator and validator |
| `data/updates/` | Generated second-release datasets |
| `storage/metrics/week3/task1_incremental_updates.json` | Measured Task 1 evidence |
| `reports/week3/task1.md` | Task 1 results and discussion |

## Week 3 roadmap

The complete Week 3 assignment contains five tasks:

1. Generate incremental update datasets and support schema evolution.
2. Maintain analytical consistency and refresh affected data products.
3. Build a pipeline monitoring system backed by Delta tables.
4. Extend validation to isolate duplicates, invalid values, missing references and unsupported schema changes.
5. Evaluate incremental processing, refresh, validation and monitoring overhead.

Only Task 1 is implemented in this branch at present. Task 2 and later tasks should build on the update files and measured evidence produced by Task 1.
