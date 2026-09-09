# Week 1 Urban Data Integration Platform

Spark ingestion, validation, Delta storage, taxi enrichment, and storage benchmarks for the four Week 1 datasets.

## Setup and execution

Use Python 3.10-3.12, Java 17 or 21, and the pinned runtime dependencies:

```powershell
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r requirements.txt
$env:JAVA_HOME = 'C:/path/to/jdk-21'
$env:PYSPARK_PYTHON = (Resolve-Path .venv/Scripts/python.exe).Path
.venv/Scripts/python.exe scripts/run_week1.py
```

Run from the project root. Windows needs compatible `winutils.exe` and `hadoop.dll` under `HADOOP_HOME/bin`; the session factory detects `C:/hadoop` when present. The first Spark run needs network access to resolve Delta JVM dependencies. Keep Java's user dependency cache writable.

Place Q1 2024 yellow taxi Parquet, `weather.csv`, `hourly_88101_2024.csv`, and `taxi_zone_lookup (1).csv` in `data/`, using the assignment download links. Dataset paths are specified in `config/platform_config.yaml`; relative paths resolve against the project root in the CLI.

The default run ingests all four sources, integrates them, benchmarks three taxi layouts, saves JSON metrics, and regenerates `reports/week1/benchmark_report.md`. For an ingestion/integration rebuild without benchmarking:

```powershell
.venv/Scripts/python.exe scripts/run_week1.py --skip-benchmark
```

Both commands rebuild Bronze/Silver snapshots. A full benchmark uses fresh layout directories. Defaults are `local[*]`, requested `8g` driver memory, eight shuffle/writer partitions and five measured query repetitions. Allow additional memory and disk space for source materialization, quarantine, integrated output and benchmark copies.

## Code organization

- `src/common`: configuration validation, logging, and Spark session setup.
- `src/ingestion`: shared read/prepare/classify/publish lifecycle and four domain transformations.
- `src/integration`: endpoint zone, weather and air joins plus trip-identity verification.
- `src/storage`: independent raw-input storage experiments and query measurements.
- `scripts`: platform orchestration and report generation.
- `tests`: fast configuration/orchestration tests, isolated Spark fixtures, and post-run acceptance checks.
- `reports/week1`: canonical editable submission sources and generated PDFs/diagram.

## Configuration and contracts

Each dataset has explicit source/output paths, format, required source columns, rename/cast mappings, keys, SQL validity rules, quality thresholds, timezone and partitions. Cast keys use standardized required source names. `selected_columns` defines the clean weather/air/zone schema after validation; it never silently skips missing requested fields. Domain transforms remain Python code.

`datasets.weather.timezone` defaults to UTC as an assumption because the CSV has no timezone declaration; confirm its export settings. Rule placeholders resolve from dataset `quality_rules`, with `{source_timezone}` supplied by the dataset's `timezone`. `paths.metadata_path` is the ingestion audit destination. Empty `partition_cols: []` means unpartitioned, including integration.

The runner validates configuration before starting Spark, then resolves all source, rule, output and integration schemas before writing any table. Unsupported dataset names, missing Week 1 sources, invalid placeholders, missing casts/output columns and invalid partition columns fail explicitly. Data errors discovered while reading or executing can still fail later; commits are per table, not platform-wide.

Construct ingestors with an explicit `dataset_spec`, `schema_version` and `metadata_path`. There is no alternate path/threshold keyword API. The spec is copied to prevent caller mutation. A runner execution shares one run ID across its ingestion audit records and JSON summary. Standalone ingestor runs generate their own IDs.

## Output and data semantics

Original files remain unchanged. Bronze preserves source fields with sanitized names and an ingestion timestamp. Clean sources and integrated trips are snapshot Delta tables. Quarantine appends invalid and duplicate rows per dataset, including rejection reasons, run ID, schema version and rejection time. Successful ingestion metadata appends counts, paths, duration and completion time. Duration excludes its final audit write.

Ingestion materializes raw input and one classified result on disk for reuse by counts and writes. Every successful ingestion checks `initial_records = valid_records + rejected_records + out_of_scope_records`. Rejected counts include duplicates; invalid counts exclude duplicates. Air observations outside NYC are counted separately. Duplicate survivors use deterministic lexicographic payload order; this is a snapshot policy, not correction resolution.

Taxi timestamps become UTC instants while local pickup date/year/month/day/hour retain New York meaning. EPA uses GMT fields. Missing passenger counts default to one and missing tips/tolls to zero; malformed casts are still rejected. Total, tips and tolls must be finite and nonnegative. Weather retains null observations; observed precipitation must be finite/nonnegative, pressure finite/positive, cloud cover 0-8 and weather code 1-27. Other configured bounds are in YAML. Blank borough labels are rejected.

Endpoint context matches the containing UTC hour. Missing labels become `Unknown`; precipitation and pollution remain null when unavailable. `pickup/dropoff_weather_matched`, precipitation-missing flags, and PM2.5 source labels expose coverage. Integration verifies unique, non-null trip IDs and exact input/output ID sets before publication.

`pickup_pm25_borough` is the nullable same-hour borough estimate. `pickup_pm25_citywide` is the same-hour mean across available NYC sites, identical across trips within that hour. `pickup_pm25` prefers borough, then citywide; `pickup_pm25_source` identifies that choice. The same fields exist for dropoff. Site identifiers are required so instruments are averaged within sites before equal site weighting. Use citywide values for citywide analysis and borough values with coverage reporting for borough comparisons.

Read outputs by Delta path, for example:

```python
from src.common.spark_session import get_spark_session

spark = get_spark_session()
trips = spark.read.format("delta").load("storage/delta/silver/integrated_taxi_trips")
trips.groupBy("pickup_borough").count().show()
spark.stop()
```

## Checks and reports

```powershell
.venv/Scripts/python.exe tests/test_configuration_and_runner.py
.venv/Scripts/python.exe tests/test_week1_regressions.py
.venv/Scripts/python.exe tests/test_week1_acceptance.py
.venv/Scripts/python.exe -m ruff check src scripts tests
.venv/Scripts/python.exe -m ruff format --check src scripts tests
```

The first suite requires no Spark session. Spark fixtures write only under `.test-output/`, including audit records. Run the acceptance suite after a full data run; it reads configured Delta outputs. Install the pinned Ruff development dependency with `pip install -r requirements-dev.txt`; rules live in `pyproject.toml`.

JSON summaries are saved as timestamped artifacts and an atomically replaced latest file. Artifact failures cause a failing command exit; an earlier processing exception is preserved. Ingestion-only runs replace latest JSON too, so choose a timestamped successful full run when regenerating a benchmark:

```powershell
.venv/Scripts/python.exe scripts/render_benchmark_report.py --metrics storage/metrics/<successful-full-run>.json
```

The renderer requires successful raw-taxi measurements, three layouts and equivalent query results. It uses explicit layout order and the single query-statistics structure. Fresh directories avoid obsolete-file contamination. Warm queries, fixed ingestion order and one ingestion trial per layout limit the conclusions; inspect samples and physical plans in the JSON.

All editable deliverables live in `reports/week1/`. The PDF builder reads those files directly; importing it has no build side effects. It needs ReportLab, pypdf, pdf2image, Pillow and Poppler in a separate artifact environment:

```powershell
python scripts/build_week1_deliverables.py --metrics storage/metrics/<successful-full-run>.json
```

Set `POPPLER_BIN` if Poppler is not on PATH. The builder requires an explicitly chosen successful raw-input metrics artifact, regenerates its benchmark source, and copies the JSON into `reports/week1/evidence/`. Rebuild and visually inspect PDFs after editing sources. The Spark runner regenerates benchmark Markdown, not PDFs. The original benchmark report in `evidence/` and `reports/week1_validation.md` are historical; current measurements are identified in the canonical benchmark report.

## Next tasks

Week 2 can add SQL queries and analytical Delta products using the current output contract, AQE switch and benchmark result checks. Week 3 requires a correction identity policy (fare/distance changes alter the taxi hash), explicit schema evolution for `humidity`/`aqi`, merge publication and affected-product refresh. Week 4 should reuse preparation and integration with target-specific features and temporal splits. No speculative orchestration or ML framework is needed in Week 1.
