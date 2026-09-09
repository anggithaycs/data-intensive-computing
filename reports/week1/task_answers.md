# Week 1 Task Answers and Discussion Points

This document answers the Week 1 tasks using the implemented Spark and Delta Lake platform. It explains what each dataset represents, how the platform stores and validates records, and how it enriches taxi trips without changing their identity. The final section describes the storage benchmark and the limits of its findings.

---

## Task 1: Study the Data (Data Catalog & Entity Analysis)

The first step was to establish what one row represents in each source. That determines how records can be identified, how datasets can be joined, and how their size is likely to grow. The following catalog summarizes these differences; the sections below explain the assumptions behind the keys.

### Dataset overview and profiling summary

| Dataset | Primary Entity | Primary / Surrogate Key | Temporal Attributes | Join Attributes | Volume Growth Pattern |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Taxi Trips** | Single completed yellow taxi trip | `trip_id` (SHA-256 composite hash) | `pickup_datetime`, `dropoff_datetime` (UTC), `pickup_local_datetime` | `PULocationID`, `DOLocationID`, containing UTC hour | Monthly batch accumulation (~3M trips/month) |
| **Weather** | Hourly meteorological observation | `weather_observation_id` (`YYYY_M_D_H`) | `observation_datetime` (UTC), `year`, `month`, `day`, `hour` | `observation_datetime` (UTC hour) | Linear temporal growth (24 records/day per station) |
| **Air Quality** | Hourly monitor occurrence of PM2.5 | `air_quality_observation_id` (Monitor + Time hash) | `observation_datetime` (UTC from GMT), `date`, `hour` | `observation_datetime` (UTC hour), `borough` | Multiplicative: hours × monitoring sites × parameters |
| **Taxi Zones** | Geographic zone reference | `location_id` (Integer 1–265) | Static reference (no native timestamps) | `location_id` (matches PU/DO IDs) | Infrequent reference updates / boundary revisions |

---

### Detailed Entity Analysis

#### 1. Yellow taxi trips

Each taxi record describes a completed yellow-taxi journey. The raw Parquet source has no guaranteed unique trip identifier, so Silver derives `trip_id` from seven attributes:

```text
trip_id = SHA-256(vendor_id, pickup_datetime, dropoff_datetime,
                 pickup_location_id, dropoff_location_id, fare_amount, trip_distance)
```

This hash provides a repeatable way to identify duplicates within a snapshot. When valid records share the key, one survives into Silver and the additional occurrences are quarantined under `duplicate_key`. The policy has limits: indistinguishable physical trips can collapse into one record, and correcting fare or distance changes the hash. It therefore cannot serve as a correction-stable physical-trip identifier.

The source fields `tpep_pickup_datetime` and `tpep_dropoff_datetime` represent New York wall-clock time in `America/New_York`. Ingestion converts them into UTC `pickup_datetime` and `dropoff_datetime`, while retaining local timestamps and local calendar keys for analysis of daily and hourly patterns.

Categorical attributes include `vendor_id`, `rate_code_id`, `store_and_fwd_flag`, `payment_type`, `pickup_location_id` and `dropoff_location_id`. Quantitative attributes include `passenger_count`, `trip_distance`, `fare_amount`, `tip_amount`, `tolls_amount`, `total_amount` and the derived `trip_duration_minutes`. New monthly batches add roughly three million trips, making this the platform’s main growing fact table.

#### 2. Hourly weather observations

Each weather row represents one hourly observation for New York City. The key, `weather_observation_id`, combines the source year, month, day and hour; for example, `2024_2_8_18`. The join timestamp is stored as UTC `observation_datetime`. Because the CSV does not declare its timezone, the configured UTC source timezone remains an assumption to confirm against the export settings.

The condition fields are `weather_condition_code`, with configured values from 1 to 27, and `cloud_cover_oktas`, with values from 0 to 8. Measurements include `temperature_c`, `relative_humidity_pct`, `precipitation_mm`, `wind_speed_kmh` and `air_pressure_hpa`.

A single hourly series grows by 24 records per day and contains 8,784 rows for the leap year 2024. The current key assumes one series; adding multiple stations would require a station or source identifier.

#### 3. Hourly air quality: EPA PM2.5

Each air-quality row represents an hourly monitor occurrence for fine particulate matter, PM2.5 (parameter 88101). Its key hashes `state_code`, `county_code`, `site_number`, `poc` and `observation_datetime`. Combining `Date GMT` and `Time GMT` produces the UTC observation timestamp.

The raw EPA source contains approximately 8.14 million national observations. Ingestion restricts the analysis to New York State (36) and the five NYC counties: Bronx (005), Kings/Brooklyn (047), New York/Manhattan (061), Queens (081) and Richmond/Staten Island (085). After scope filtering and validation, the clean table contains 51,855 NYC readings.

The table grows with the number of reporting hours, sites and instruments, and with EPA reporting cycles. The current key assumes the supplied PM2.5 parameter. Supporting multiple pollutants would require extending the key and checking units explicitly.

#### 4. Taxi zone lookup

The zone lookup maps `LocationID` to `borough`, `zone` and `service_zone`. Silver standardizes the key to `location_id`. The supplied file contains 265 reference entries, with IDs from 1 to 265, and contains labels and identifiers rather than polygon geometries. IDs 264 and 265 represent the special unknown/NV entries and remain valid reference rows.

Zones change through occasional reference updates rather than regular observation batches. They therefore play a different role from taxi trips, hourly weather and monitor readings. Integration connects these datasets at their appropriate location and time keys while retaining one row per accepted taxi trip.

---

## Task 2: Design Your Storage Architecture

### Directory structure and data layers

The platform separates source snapshots, clean analytical data and audit records into Delta Lake layers. Bronze retains ingested source fields, Silver contains validated and integrated tables, and quarantine records why rows were rejected. Each table has its own directory and transaction log:

```text
storage/delta/
├── bronze/                         # Raw Ingestion Layer (Full Source Fidelity)
│   ├── raw_taxi_trips/
│   ├── raw_weather/
│   ├── raw_air_quality/
│   └── raw_taxi_zones/
├── silver/                         # Standardized, Validated, & Integrated Layer
│   ├── clean_taxi_trips/           # Partitioned by (pickup_year, pickup_month)
│   ├── clean_weather/              # Unpartitioned hourly lookup
│   ├── clean_air_quality/          # Unpartitioned NYC scoped lookup
│   ├── clean_taxi_zones/           # Unpartitioned spatial dimension
│   └── integrated_taxi_trips/      # Partitioned master analytical fact table
├── quarantine/                     # Data Quality Audit Layer (Append-only)
│   ├── taxi_trips/                 # Tagged with _rejection_reason & _validation_errors
│   ├── weather/
│   ├── air_quality/
│   └── taxi_zones/
├── metadata/                       # Operational Governance
│   └── ingestion_runs/             # Audit log of row counts, run IDs, durations
└── benchmark/                      # Task 6 Storage Experiments
    ├── strategy_a_unpartitioned/
    ├── strategy_b_partitioned/
    └── strategy_c_monthly_partitioned/
```

### Table organization and storage policy

| Layer | Directory Path | Storage Format | Partitioning Scheme | Purpose & Retention Policy |
| :--- | :--- | :--- | :--- | :--- |
| **Bronze** | `storage/delta/bronze/raw_*` | Delta Lake | None | Overwrite the source snapshot, retaining source fields with sanitized names and an ingestion timestamp for audit and replay. |
| **Silver (Clean)** | `storage/delta/silver/clean_*` | Delta Lake | Taxi: `pickup_year`, `pickup_month`<br>Weather/Air/Zone: None | Validated, deduplicated, standardized single-source domain tables. |
| **Silver (Integrated)** | `storage/delta/silver/integrated_taxi_trips` | Delta Lake | `pickup_year`, `pickup_month` | Contextually enriched analytical master table joining trips with weather, air, and zones. |
| **Quarantine** | `storage/delta/quarantine/*` | Delta Lake | None (Append-only audit log) | Preserves all rejected records with `_validation_errors` and `_rejection_reason`. |
| **Metadata** | `storage/delta/metadata/ingestion_runs` | Delta Lake | None (Append-only) | Audit log recording schema versions, row counts, durations, and run IDs. |
| **Benchmark** | `storage/delta/benchmark/<strategy>/<run_id>` | Delta Lake | Per benchmark strategy (A: none, B: date, C: month) | Isolated experimental directories ensuring clean performance benchmarking. |

---

### Architectural Discussion Points

#### 1. Which datasets should be treated as lookup tables?

Taxi zones form a spatial lookup: their identifiers supply borough and zone labels for each trip endpoint. Weather acts as a temporal lookup because each trip can be matched to an hourly observation. The borough-hour and citywide-hour air-quality aggregates also act as lookups during integration, although the underlying monitor records are observation facts.

Here, “lookup” describes how a table is used in a join. It does not mean that the source is static; weather and air-quality observations continue to arrive over time.

#### 2. Which datasets should remain unpartitioned, and why?

Weather (8,784 rows), cleaned NYC air quality (51,855 rows) and zone labels (265 rows) are small inputs. Fine-grained directory partitions would add metadata and small files without a demonstrated benefit for the current joins, so these tables remain unpartitioned.

An unpartitioned Delta table can still contain several files and be read by multiple Spark tasks. Whether a lookup can be broadcast depends on its actual size and available memory, rather than a universal row-count or gigabyte threshold.

#### 3. Which datasets need a different partitioning strategy?

Taxi data arrives in much larger monthly batches, at roughly three million rows and around 150 MB of source Parquet per month. The current layout partitions clean and integrated trips by local `pickup_year` and `pickup_month`. This allows queries with suitable time filters to skip irrelevant partitions.

That choice reflects the current workload. A large fact table does not automatically benefit from every form of partitioning, and a reference table is not inherently unsuitable for it. The useful distinction is whether the layout matches the data volume and query filters.

#### 4. When does partitioning become harmful?

Partitioning can create many small files, increasing scheduling and metadata work relative to useful data processing. Files much smaller than a typical 128 MB Spark/HDFS block are one warning sign, although that size is not a universal cutoff.

Keys with many distinct values, such as `trip_id`, exact timestamps or vehicle IDs, can create excessive directories and pressure the Delta log, file handles and scheduler. Unevenly distributed keys can also produce skew, leaving some tasks running after others finish. Finally, queries that cannot filter by the partition columns may still scan every directory, gaining little from the layout while paying its metadata cost.

#### 5. How should the architecture change at twenty times the volume?

First determine whether growth means more trips per day or a longer date range. Re-measure daily and monthly layouts, writer parallelism, target file sizes and compaction before changing the policy. Partitioning alone does not determine file size.

As sources expand, reassess whether lookups still fit broadcast joins and whether data should be organized by region. Shared object storage and a catalog are possible infrastructure extensions. No twentyfold-scale experiment has been performed, so the current results do not establish an optimum for that scale.

---

## Task 3: Build a Generic Ingestion Framework

### Ingestion Engine Architecture & Lifecycle

The shared `BaseDatasetIngestor` controls the ingestion sequence, while subclasses supply transformations specific to each source. Every dataset follows the same eight stages:

1. **Raw Ingestion**: Reads raw Parquet or CSV. Raw CSV is loaded as strings with `FAILFAST` parsing to catch malformed files.
2. **Schema & Required Column Validation**: Asserts all mandatory source attributes are present.
3. **Column Standardization & Safe Type Casting**: Maps source names to lower `snake_case`. Applies `try_cast()` against configured target types, capturing failures in `_cast_errors` rather than crashing.
4. **Scope Filtering & Domain Transformations**: Applies domain-specific logic (e.g. NYC county filtering, timestamp assembly).
5. **Quality Rule Evaluation**: Evaluates named SQL expressions declared in `platform_config.yaml`, tagging failures in `_validation_errors`.
6. **Deterministic Snapshot Deduplication**: Partitions valid and invalid rows separately by key, ranking them lexicographically by full-row JSON payload.
7. **Bronze / Silver / Quarantine Publication**: Raw rows overwrite Bronze after classification.  Rows with zero errors and rank 1 route to Silver; rows with errors or rank > 1 append to Quarantine.
8. **Ingestion Metadata Publication**: Records execution duration, row counts (initial, valid, rejected, duplicate, out-of-scope), and schema version to Delta metadata.

---

### Shared and dataset-specific responsibilities

| Component | Classification | Implementation Location | Responsibility & Justification |
| :--- | :--- | :--- | :--- |
| **File Readers** | Generic | `BaseDatasetIngestor.load_raw_data()` | Handles Parquet and CSV reading with consistent failfast parsing options. |
| **Schema Contract** | Generic | `BaseDatasetIngestor.load_raw_data()` | Ensures mandatory fields exist before transformations proceed. |
| **Safe Casting** | Generic | `BaseDatasetIngestor.standardize_column_names()` | Applies `try_cast()` and accumulates conversion errors into `_cast_errors`. |
| **Rule Evaluation** | Generic | `BaseDatasetIngestor.classify_records()` | Interpolates YAML parameters into Spark SQL expressions across all datasets. |
| **Deduplication** | Generic | `BaseDatasetIngestor.classify_records()` | Lexicographical Window ranking guarantees deterministic survivor selection. |
| **Delta Writers** | Generic | `BaseDatasetIngestor.save_to_delta()` | Atomic overwrite for Silver, append with schema merge for Quarantine. |
| **Taxi Transforms** | Dataset-Specific | `TaxiTripsIngestor.normalize_and_transform()` | Parses NY local wall-clock time, converts to UTC, derives `trip_id` SHA-256 hash. |
| **Weather Transforms** | Dataset-Specific | `WeatherIngestor.normalize_and_transform()` | Combines year/month/day/hour into ISO timestamp string and `weather_observation_id`. |
| **Air Quality Filter** | Dataset-Specific | `AirQualityIngestor.filter_scope()` | Restricts national EPA records to NY State (36) and 5 NYC county codes. |
| **Zone Transforms** | Dataset-Specific | `TaxiZoneIngestor.normalize_and_transform()` | Trims string labels and standardizes `LocationID` into `location_id`. |

---

### Metadata Management & Row Reconciliation

Each ingestion verifies that every input row is accounted for:

```text
initial_records = valid_records + rejected_records + out_of_scope_records
```

Rejected records include duplicate occurrences, while invalid-record counts exclude duplicates. If reconciliation fails, Silver is not published for that ingestion. This check applies to an individual dataset; there is no transaction covering the whole platform.

Successful ingestion statistics are appended to `storage/delta/metadata/ingestion_runs`. They include initial, valid, rejected, duplicate and out-of-scope counts, execution duration and schema version. Failures are recorded in the runner JSON, while comprehensive failure monitoring remains future work.

### Adding twenty new datasets next year

New sources can reuse the ingestion lifecycle, but each still needs an explicit contract. Add a dataset specification in `platform_config.yaml` with source and output paths, casts, keys and validation rules, then register the ingestor in `INGESTOR_REGISTRY`. Where source meaning requires new transformations, implement them in a `BaseDatasetIngestor` subclass. A new file format also needs a reader.

The runner instantiates registered ingestors and applies the shared lifecycle. Representative fixtures should verify each new contract. The current runner still requires the Week 1 sources, and adding a source does not automatically define its integration joins; those relationships need separate implementation.

---

## Task 4: Design a Common Data Model

### Canonical Data Types & Timestamp Standardization

The common model gives timestamps, names and units consistent meanings across sources. Temporal join fields use Spark `TimestampType` in a UTC session, with `YYYY-MM-DDTHH:mm:ssZ` as an ISO-8601 interchange representation. New York taxi wall-clock timestamps are converted from `America/New_York`, while local fields such as `pickup_local_datetime` remain available for calendar analysis.

Column names use `snake_case`, with unit suffixes where defined. Examples include `temperature_c`, `wind_speed_kmh`, `precipitation_mm`, `trip_duration_minutes` and `relative_humidity_pct`. Monetary amounts and continuous measurements use `DoubleType`, while integer counts use `IntegerType` or `LongType`. This is sufficient for the analytical prototype; exact monetary accounting would need an explicit decimal policy.

Missing values are handled according to their meaning. A missing taxi `passenger_count` defaults to 1, and missing `tip_amount` and `tolls_amount` default to 0.0. These assumptions can affect aggregates. Malformed non-null values remain validation errors even if a later default could replace them.

Unmatched pickup or dropoff locations receive `Unknown` labels so the trip is retained. Missing weather and air-quality measurements remain null. Flags such as `pickup_weather_matched = false` and source tags such as `pickup_pm25_source = 'missing'` let analysts distinguish absent context from an observed value.

---

### Transformation inventory

| Domain | Source Field | Silver Standardized Field | Target Data Type | Transformation & Semantic Rule |
| :--- | :--- | :--- | :--- | :--- |
| **Taxi** | `tpep_pickup_datetime` | `pickup_datetime` | `timestamp (UTC)` | Converted from `America/New_York` wall time to UTC instant. |
| **Taxi** | `tpep_pickup_datetime` | `pickup_local_datetime` | `timestamp` | Retained local wall-clock timestamp for NYC diurnal/calendar analytics. |
| **Taxi** | `PULocationID / DOLocationID` | `pickup/dropoff_location_id` | `int` | Renamed and cast to integer foreign keys. |
| **Taxi** | *(Pickup & Dropoff times)* | `trip_duration_minutes` | `double` | Derived: `round((epoch_dropoff - epoch_pickup) / 60.0, 2)`. |
| **Taxi** | *(7 Core Attributes)* | `trip_id` | `string` | Derived SHA-256 surrogate hash for deterministic identity. |
| **Weather** | `year, month, day, hour` | `observation_datetime` | `timestamp (UTC)` | Concatenated into ISO string, parsed, and converted to UTC. |
| **Weather** | `temp, rhum, prcp, wspd` | `temperature_c, relative_humidity_pct...` | `double` | Renamed with unit suffixes; unselected metadata columns dropped. |
| **Air Quality** | `Date GMT, Time GMT` | `observation_datetime` | `timestamp (UTC)` | Combined into UTC instant; avoids EPA standard-time DST confusion. |
| **Air Quality** | `County Code` | `borough` | `string` | Mapped FIPS county codes (005, 047, 061, 081, 085) to borough names. |
| **Taxi Zones** | `LocationID, Borough, Zone` | `location_id, borough, zone` | `int, string` | Standardized casing, trimmed string whitespace. |

---

## Task 5: Build the Integration Pipeline

### Contextual Integration Topology

`UrbanDataIntegrationPipeline` enriches each accepted taxi trip with location, weather and air-quality context at both endpoints. Its output must retain exactly one row per accepted trip.

First, broadcast joins attach pickup and dropoff borough, zone and service-zone labels from `clean_taxi_zones`. Weather is matched by truncating each endpoint timestamp to its containing UTC hour, using an expression such as `date_trunc('hour', pickup_datetime)`, and left-joining to `clean_weather.observation_datetime`. This associates the trip with an hourly observation without searching for a nearby hour, interpolating or carrying earlier observations forward.

Air quality requires aggregation before joining because an hour can contain readings from several instruments and sites. The pipeline first averages instruments within each physical site and hour, then gives each reporting site equal weight when calculating borough and citywide hourly means. The three site identifiers must be present for this calculation.

Separate joins retain `pickup_pm25_borough` and `pickup_pm25_citywide`. The borough value remains null when local coverage is missing, while the citywide value describes the available NYC sites in that hour. The convenience field `pickup_pm25` prefers the borough estimate and otherwise uses the citywide estimate. `pickup_pm25_source` records `borough`, `citywide` or `missing`. Corresponding dropoff fields use the dropoff time and borough.

Keeping both estimates makes their different meanings visible. Borough comparisons need coverage reporting, and a citywide estimate should not be interpreted as a measurement from every borough. Neither estimate describes exposure along the taxi’s route.

---

### Strict Identity Preservation Assertions

Joins can accidentally multiply rows or lose trips. Before publication, the pipeline therefore checks that both source and integrated tables have unique, non-null `trip_id` values. The condition `count(*) == countDistinct(trip_id)` supports this check.

It then uses anti-joins in both directions to detect IDs present in only one table:

```python
assert source_ids.join(integrated_ids, "trip_id", "left_anti").count() == 0
assert integrated_ids.join(source_ids, "trip_id", "left_anti").count() == 0
```

Together, these checks verify that the output preserves the exact source trip-ID set without duplicates. They do not prove that the source hash uniquely identifies every physical trip; that remains a limitation of the identity policy.

---

## Task 6: Benchmark Your Design

### Storage Strategies Evaluated

`StorageBenchmarkRunner` compares three layouts by independently reading and cleaning the same full raw taxi dataset for each one. Each strategy writes to a fresh Delta directory:

- **Strategy A (Unpartitioned)**: All records written into a single Delta root directory.
- **Strategy B (Daily Partitioned)**: Partitioned by `pickup_date` (91 distinct partitions for Q1 2024).
- **Strategy C (Monthly Partitioned)**: Partitioned by `pickup_year` and `pickup_month` (3 distinct partitions for Q1 2024).

---

### Analytical Benchmark Queries

Each layout runs the same five queries. The first three cover the required aggregate analyses, while the last two examine selective date filters:

- **Q1 (Trips per Borough)** counts trips by pickup borough, joining the zone lookup and aggregating across the full dataset.
- **Q2 (Average Trip Duration per Day)** groups trips by local pickup day and calculates their average duration.
- **Q3 (Average Fare per Borough)** calculates the average fare for each pickup borough.
- **Q4 (Point Date Filter - Feb 14)** restricts the analysis to February 14, testing how each layout handles a single-day filter.
- **Q5 (Range Month Filter - Feb 2024)** restricts the analysis to February 2024, testing how each layout handles a month filter.

---

### Benchmark Takeaways and Limits

The [benchmark report](benchmark_report.md) contains the measured comparisons and identifies the packaged JSON artifact. All three layouts produced equivalent query results. Ingestion has one fixed-order trial per layout, and query repetitions run on a warm system, so means should be interpreted alongside sample variability.

Timings alone cannot establish how much partition pruning or compression contributed to a difference. Monthly partitioning remains the current workload-dependent default, and should be reassessed against the Week 2 queries.

### Configuration and validation limits

Before any table write, the runner resolves source schemas, rules, clean-output schemas and the integration schema. Cast fields must use standardized required source names, and requested output columns must exist. Clean-column projections are applied after validation and duplicate classification. The complete mappings, casts, defaults and projections are maintained in `config/platform_config.yaml`.

Taxi totals, tips and tolls must be finite and nonnegative. When weather values are present, precipitation must be finite and nonnegative, pressure must be finite and positive, cloud cover must be between 0 and 8, and condition codes must be between 1 and 27. Missing observations remain null, malformed casts remain rejected after defaults, and zones require nonblank boroughs. Comprehensive unit and reference-integrity checks remain future work.

The weather source timezone still needs confirmation from export metadata, and a single citywide weather series cannot describe every local condition. EPA timestamps use GMT fields, but borough PM2.5 coverage remains sparse. These limits should accompany analyses that use the integrated context.

Sources: assignment_full_text.txt; config/platform_config.yaml; src/common; src/ingestion; src/integration; src/storage; and benchmark_report.md with its packaged metrics artifact.
