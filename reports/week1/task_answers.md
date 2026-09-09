# Week 1 Task Answers and Discussion Points

This document provides comprehensive, question-by-question technical answers for every Week 1 task in the assignment. It reflects the operational Apache Spark and Delta Lake implementation, detailing dataset characteristics, storage architecture design, data quality frameworks, integration strategies, and empirical benchmark findings.

---

## Task 1: Study the Data (Data Catalog & Entity Analysis)

Before building the platform, each incoming dataset was profiled to determine its fundamental entity, primary key, join capabilities, temporal grain, and volume characteristics.

### Dataset Overview & Profiling Summary

| Dataset | Primary Entity | Primary / Surrogate Key | Temporal Attributes | Join Attributes | Volume Growth Pattern |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Taxi Trips** | Single completed yellow taxi trip | `trip_id` (SHA-256 composite hash) | `pickup_datetime`, `dropoff_datetime` (UTC), `pickup_local_datetime` | `PULocationID`, `DOLocationID`, containing UTC hour | High-velocity streaming accumulation (~3M trips/month) |
| **Weather** | Hourly meteorological observation | `weather_observation_id` (`YYYY_M_D_H`) | `observation_datetime` (UTC), `year`, `month`, `day`, `hour` | `observation_datetime` (UTC hour) | Linear temporal growth (24 records/day per station) |
| **Air Quality** | Hourly monitor occurrence of PM2.5 | `air_observation_id` (Monitor + Time hash) | `observation_datetime` (UTC from GMT), `date`, `hour` | `observation_datetime` (UTC hour), `borough` | Multiplicative: hours × monitoring sites × parameters |
| **Taxi Zones** | Geographic zone polygon reference | `location_id` (Integer 1–265) | Static reference (no native timestamps) | `location_id` (matches PU/DO IDs) | Infrequent reference updates / boundary revisions |

---

### Detailed Entity Analysis

#### 1. Yellow Taxi Trips
- **Primary Entity**: A single completed physical yellow-taxi passenger journey within New York City.
- **Primary Key Rationale**: The raw Parquet file contains no native unique identifier (medallions and driver hack licenses were removed by the NYC TLC for privacy). To guarantee row identity without arbitrary auto-incrementing integers, Silver derives `trip_id` as a deterministic SHA-256 hash over seven intrinsic attributes:
  $$\text{trip\_id} = \text{SHA-256}(\text{vendor\_id} \mathbin{\Vert} \text{pickup} \mathbin{\Vert} \text{dropoff} \mathbin{\Vert} \text{locations} \mathbin{\Vert} \text{fare} \mathbin{\Vert} \text{distance})$$
- **Duplicate Policy**: This policy deterministically separates valid trips from true pipeline duplicates. If two records share the same hash, exactly one record survives into Silver, while duplicate occurrences route to Quarantine under `duplicate_key`.
- **Temporal Attributes**: Raw `tpep_pickup_datetime` and `tpep_dropoff_datetime` represent local New York wall-clock time (`America/New_York`). The ingestor converts these to standard UTC (`pickup_datetime`) while retaining `pickup_local_datetime` and local calendar keys for analytics.
- **Categorical Attributes**: `vendor_id`, `rate_code_id`, `store_and_fwd_flag`, `payment_type`, `pickup_location_id`, `dropoff_location_id`.
- **Quantitative Attributes**: `passenger_count`, `trip_distance`, `fare_amount`, `tip_amount`, `tolls_amount`, `total_amount`, `trip_duration_minutes`.
- **Growth Vector**: Continuous horizontal accumulation of monthly batches; requires temporal partitioning to remain performant.

#### 2. Hourly Weather Observations
- **Primary Entity**: A single hourly surface weather observation for New York City.
- **Primary Key**: Composite key `weather_observation_id` constructed from `year_month_day_hour` (e.g. `2024_2_8_18`).
- **Temporal Grain**: Discrete 1-hour intervals. Timestamps are stored in UTC (`observation_datetime`).
- **Categorical & Condition Coded Attributes**: `weather_condition_code` (WMO code 1–27), `cloud_cover_oktas` (0–8).
- **Quantitative Attributes**: `temperature_c`, `relative_humidity_pct`, `precipitation_mm`, `wind_speed_kmh`, `air_pressure_hpa`.
- **Growth Vector**: Linear temporal growth (8,784 rows per year for a single station); stays lightweight.

#### 3. Hourly Air Quality (EPA PM2.5)
- **Primary Entity**: An hourly monitor occurrence measurement of Fine Particulate Matter (PM2.5, Parameter 88101).
- **Primary Key**: Hash of `state_code`, `county_code`, `site_number`, `poc`, and `observation_datetime`.
- **Scope & Target Geography**: While the raw EPA source contains 8.14M national observations, the Silver ingestion engine scopes strictly to New York State (code 36) and the five NYC counties (Bronx: 005, Kings/Brooklyn: 047, New York/Manhattan: 061, Queens: 081, Richmond/Staten Island: 085). This filters the table down to ~25,000 highly relevant NYC readings.
- **Temporal Attributes**: `Date GMT` and `Time GMT` are combined into UTC `observation_datetime`.
- **Growth Vector**: Dependent on active physical monitoring sites and frequency of EPA reporting cycles.

#### 4. Taxi Zone Lookup
- **Primary Entity**: A geographic zone polygon within New York City's five boroughs.
- **Primary Key**: `LocationID` (integer 1 to 265) mapping directly to `location_id` in Silver.
- **Attributes**: `borough`, `zone`, `service_zone`.
- **Data Quality Note**: Location IDs 264 and 265 represent 'Unknown / NV' (outside NYC or missing GPS); retained as valid reference rows.

> [!NOTE]
> **Key Architectural Principle**: The platform integrates four distinct data frequencies: a high-velocity streaming fact table (Taxi Trips), two continuous temporal-spatial observation series (Weather & Air Quality), and a static reference dimension (Taxi Zones). The common data model bridges these disparate grains without distorting primary entities or multiplying trip records.

---

## Task 2: Design Your Storage Architecture

### Directory Structure & Medallion Table Organization

To provide enterprise-grade data governance, data lineage, and auditability, the platform implements a **Delta Lake Medallion Architecture** rather than flat domain folders:

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

### Table Organization & Storage Policy

| Layer | Directory Path | Storage Format | Partitioning Scheme | Purpose & Retention Policy |
| :--- | :--- | :--- | :--- | :--- |
| **Bronze** | `storage/delta/bronze/raw_*` | Delta Lake | None (Preserves raw order) | Append/Overwrite raw ingested files. Complete source fidelity for audit & replay. |
| **Silver (Clean)** | `storage/delta/silver/clean_*` | Delta Lake | Taxi: `pickup_year`, `pickup_month`<br>Weather/Air/Zone: None | Validated, deduplicated, standardized single-source domain tables. |
| **Silver (Integrated)** | `storage/delta/silver/integrated_taxi_trips` | Delta Lake | `pickup_year`, `pickup_month` | Contextually enriched analytical master table joining trips with weather, air, and zones. |
| **Quarantine** | `storage/delta/quarantine/*` | Delta Lake | None (Append-only audit log) | Preserves all rejected records with `_validation_errors` and `_rejection_reason`. |
| **Metadata** | `storage/delta/metadata/ingestion_runs` | Delta Lake | None (Append-only) | Audit log recording schema versions, row counts, durations, and run IDs. |
| **Benchmark** | `storage/delta/benchmark/<strategy>/<run_id>` | Delta Lake | Per benchmark strategy (A: none, B: date, C: month) | Isolated experimental directories ensuring clean performance benchmarking. |

---

### Architectural Discussion Points

#### 1. Which datasets should conceptually be treated as lookup tables?
- **Taxi Zones**: A pure spatial dimension table mapping location IDs to borough and zone descriptors.
- **Weather**: Conceptually treated as a temporal lookup table during integration. Although it records physical observations, its hourly grain means it serves as a lightweight reference table matched against taxi pickup hours.
- **Air Quality Aggregates**: The pre-aggregated borough-hourly and citywide-hourly tables act as spatio-temporal lookup tables during the integration join, despite the underlying raw sensor data being an observation fact table.
- **Key Distinction**: Calling a dataset a "lookup" describes its role in the join topology, not an assumption that source data is static.

#### 2. Which datasets should NOT be partitioned, and why?
- **Weather (8,784 rows)**: Too small. Partitioning by day or month would create hundreds of tiny files (<10 KB), wasting storage in file-system metadata and slowing down reads.
- **Cleaned Air Quality (~25,000 NYC rows)**: Partitioning into 12 monthly folders produces files <50 KB. Keeping it unpartitioned allows Spark to read the entire table in a single partition and broadcast it into memory.
- **Taxi Zones (265 rows)**: Static lookup of negligible size (~20 KB). Partitioning would be entirely dysfunctional.
- **Rule of Thumb**: In Delta Lake, datasets under a few gigabytes perform best unpartitioned to enable in-memory **Broadcast Joins**.

#### 3. Which datasets require different partitioning strategies?
- **Yellow Taxi Trips**: Highly dynamic, massive transaction fact table (~3M rows/month, ~150 MB Parquet/month). Requires partitioning by `pickup_year` and `pickup_month` to prune partitions during time-bounded analytical queries.
- **Strategy Difference**: Fact tables with millions of rows benefit from time partitioning; dimension and reference tables do not.

#### 4. Under what conditions does partitioning become harmful?
- **The "Small Files Problem"**: When partitioned directories contain files significantly smaller than Spark/HDFS block size (128 MB).
- **High Cardinality Keys**: Partitioning by high-cardinality attributes (such as `trip_id`, exact timestamp, or vehicle ID) creates tens of thousands of directories, overwhelming the Delta log, JVM file handles, and Spark scheduler.
- **Uncorrelated Queries**: If queries do not include the partition column in their `WHERE` clause, Spark must scan every directory, incurring massive metadata open/close overhead.
- **Partition Skew**: Partitioning on an unevenly distributed column leads to straggler tasks and CPU idle time.

#### 5. Storage Architecture Adaptations if Data Volume Increases 20×:
- **Shift Taxi Partitions to Daily Grain**: If monthly trips grow from 3M to 60M (~3 GB/month), monthly partitions will exceed 1 GB. Sub-partitioning by `pickup_date` (or `pickup_year` / `pickup_month` / `pickup_day`) will maintain optimal ~128–256 MB Parquet file sizes.
- **Liquid Clustering / Z-Ordering**: Implement Delta Lake Liquid Clustering or `Z-ORDER BY (pickup_location_id, pickup_datetime)` to optimize multi-dimensional queries without physical directory explosion.
- **Air Quality Sensor Scaling**: If monitor networks expand nationwide (160M+ rows), partition by `state_code` and `observation_year`.
- **Target File Size & Auto-Compaction**: Enable `spark.databricks.delta.autoCompact` and tune `targetFileSize` to 256MB.
- **Cloud Object Storage & Catalog**: Move from local filesystem storage to AWS S3 / Azure ADLS with Unity Catalog.

---

## Task 3: Build a Generic Ingestion Framework

### Ingestion Engine Architecture & Lifecycle

The platform implements an object-oriented, template-method ingestion engine centered around `BaseDatasetIngestor`. Every dataset executes through an identical, deterministic 8-stage lifecycle:

1. **Raw Ingestion & Bronze Persistence**: Reads raw Parquet or CSV. Raw CSV is loaded as strings with `FAILFAST` parsing to catch malformed files.
2. **Schema & Required Column Validation**: Asserts all mandatory source attributes are present.
3. **Column Standardization & Safe Type Casting**: Maps source names to lower `snake_case`. Applies `try_cast()` against configured target types, capturing failures in `_cast_errors` rather than crashing.
4. **Scope Filtering & Domain Transformations**: Applies domain-specific logic (e.g. NYC county filtering, timestamp assembly).
5. **Quality Rule Evaluation**: Evaluates named SQL expressions declared in `platform_config.yaml`, tagging failures in `_validation_errors`.
6. **Deterministic Snapshot Deduplication**: Partitions valid and invalid rows separately by key, ranking them lexicographically by full-row JSON payload.
7. **Silver / Quarantine Routing**: Rows with zero errors and rank 1 route to Silver; rows with errors or rank > 1 append to Quarantine.
8. **Ingestion Metadata Publication**: Records execution duration, row counts (initial, valid, rejected, duplicate, out-of-scope), and schema version to Delta metadata.

---

### Generic vs. Dataset-Specific Responsibilities

| Component | Classification | Implementation Location | Responsibility & Justification |
| :--- | :--- | :--- | :--- |
| **File Readers** | Generic | `BaseDatasetIngestor.load_raw_data()` | Handles Parquet and CSV reading with consistent failfast parsing options. |
| **Schema Contract** | Generic | `BaseDatasetIngestor.verify_required_columns()` | Ensures mandatory fields exist before transformations proceed. |
| **Safe Casting** | Generic | `BaseDatasetIngestor.cast_columns_safely()` | Applies `try_cast()` and accumulates conversion errors into `_cast_errors`. |
| **Rule Evaluation** | Generic | `BaseDatasetIngestor.classify_records()` | Interpolates YAML parameters into Spark SQL expressions across all datasets. |
| **Deduplication** | Generic | `BaseDatasetIngestor.classify_records()` | Lexicographical Window ranking guarantees deterministic survivor selection. |
| **Delta Writers** | Generic | `BaseDatasetIngestor.save_to_delta()` | Atomic overwrite for Silver, append with schema merge for Quarantine. |
| **Taxi Transforms** | Dataset-Specific | `TaxiTripsIngestor.normalize_and_transform()` | Parses NY local wall-clock time, converts to UTC, derives `trip_id` SHA-256 hash. |
| **Weather Transforms** | Dataset-Specific | `WeatherIngestor.normalize_and_transform()` | Combines year/month/day/hour into ISO timestamp string and `weather_observation_id`. |
| **Air Quality Filter** | Dataset-Specific | `AirQualityIngestor.filter_scope()` | Restricts national EPA records to NY State (36) and 5 NYC county codes. |
| **Zone Transforms** | Dataset-Specific | `TaxiZoneIngestor.normalize_and_transform()` | Trims string labels and standardizes `LocationID` into `location_id`. |

---

### Metadata Management & Row Reconciliation

To guarantee complete accounting integrity, every ingestion run enforces a mathematical conservation invariant:
$$\text{Initial Raw Records} = \text{Valid Silver Records} + \text{Rejected Quarantine Records} + \text{Out-of-Scope Records}$$

If this assertion fails, the transaction aborts before Silver is updated. Ingestion statistics are appended to `storage/delta/metadata/ingestion_runs`, providing full operational visibility.

### Extensibility: Adding 20 New Datasets Next Year
Because the framework separates configuration from execution, onboarding 20 new datasets requires zero modification to core ingestion logic:
1. **YAML Declaration**: Add 20 dataset blocks in `platform_config.yaml` defining `source_path`, `silver_path`, `casts`, `key`, and `validation` rules.
2. **Subclass Implementation**: If the dataset requires unique semantic transformations, subclass `BaseDatasetIngestor` (typically ~25 lines of code) and register it in `INGESTOR_REGISTRY`.
3. **Automated Execution**: The runner dynamically instantiates registered ingestors and runs them through the identical Medallion lifecycle.

---

## Task 4: Design a Common Data Model

### Canonical Data Types & Timestamp Standardization

Heterogeneous datasets cannot be meaningfully correlated without strict semantic standardization. The platform enforces:

- **Canonical Timestamp Standard**: All temporal join instants are normalized to UTC `TimestampType` (formatted as ISO-8601: `YYYY-MM-DDTHH:mm:ssZ`). Source timestamps with regional offsets (`America/New_York`) are converted to UTC, while retaining local wall-clock timestamps (`pickup_local_datetime`) for calendar analytics.
- **Naming Conventions**: Strict `snake_case` across all attributes, with explicit physical unit suffixes (e.g. `temperature_c`, `wind_speed_kmh`, `precipitation_mm`, `trip_duration_minutes`, `relative_humidity_pct`).
- **Numeric Precision**: Monetary amounts and continuous sensor measurements use `DoubleType` for analytical throughput; integer counts use `IntegerType` / `LongType`.
- **Null Handling & Imputation Rules**:
  - *Business Defaults*: In Taxi Trips, null `passenger_count` defaults to 1; null `tip_amount` and `tolls_amount` default to 0.0.
  - *Missing Spatial Lookups*: Unmatched pickup/dropoff locations map to `"Unknown"` rather than dropping the trip.
  - *Contextual Weather & Air Quality*: Missing weather/air measurements remain `NULL`; no synthetic values are fabricated. Match flags (`pickup_weather_matched = false`) and source tags (`pickup_pm25_source = 'missing'`) preserve analytical transparency.

---

### Complete Transformation Inventory

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

The integration pipeline (`UrbanDataIntegrationPipeline`) enriches each taxi trip with spatial, meteorological, and environmental context while preserving the exact grain of the taxi fact table (1 row = 1 trip):

- **Spatial Enrichment**: Broadcast hash joins attach pickup and dropoff borough, zone, and service zone from `clean_taxi_zones`.
- **Weather Association**: Taxi `pickup_datetime` is truncated to its containing UTC hour (`date_trunc('hour', pickup_datetime)`) and left-joined to `clean_weather` on `observation_datetime`. This maps each trip to the prevailing meteorological state of its start hour.
- **Air Quality Hierarchical Association**: Fine particulate matter (PM2.5) exhibits spatial variation across NYC. The pipeline employs a two-tier strategy:
  1. *Primary Join*: Matches on exact `(UTC hour, pickup_borough)` against borough-aggregated PM2.5 readings.
  2. *Citywide Fallback*: If a borough's monitoring station is offline, falls back to the NYC-wide hourly mean across all reporting sites.
  3. *Lineage Tagging*: The column `pickup_pm25_source` explicitly records whether the value originated from `'borough'`, `'citywide'`, or is `'missing'`.

---

### Strict Identity Preservation Assertions

A critical vulnerability in relational joins is Cartesian explosion (fanout) or silent row loss. To prevent this, the pipeline executes strict pre-publication identity validation:
- **Non-null & Distinct Assertions**: Asserts `count(*) == countDistinct(trip_id)` on both source and integrated tables.
- **Bi-directional Anti-Joins**: Executes left-anti joins in both directions:
  ```python
  assert source_ids.join(integrated_ids, "trip_id", "left_anti").count() == 0
  assert integrated_ids.join(source_ids, "trip_id", "left_anti").count() == 0
  ```
This mathematically proves that the integration pipeline neither dropped, duplicated, nor altered a single trip ID.

---

## Task 6: Benchmark Your Design

### Storage Strategies Evaluated

To rigorously assess scalability and Delta Lake I/O performance, the benchmark engine (`StorageBenchmarkRunner`) implemented and evaluated three distinct physical storage layouts directly on the full raw dataset:

- **Strategy A (Unpartitioned)**: All records written into a single Delta root directory.
- **Strategy B (Daily Partitioned)**: Partitioned by `pickup_date` (91 distinct partitions for Q1 2024).
- **Strategy C (Monthly Partitioned)**: Partitioned by `pickup_year` and `pickup_month` (3 distinct partitions for Q1 2024).

---

### Analytical Benchmark Queries

Each layout was evaluated across five representative analytical workloads:
- **Q1 (Trips per Borough)**: Large aggregation joining spatial dimensions; requires full table scan.
- **Q2 (Average Trip Duration per Day)**: Daily grouping testing compute over duration metrics.
- **Q3 (Average Fare per Borough)**: Aggregation computing financial metrics by geographic area.
- **Q4 (Point Date Filter - Feb 14)**: Tests partition pruning on a single date (Valentine's Day).
- **Q5 (Range Month Filter - Feb 2024)**: Tests partition pruning on an entire monthly slice.

---

### Benchmark Takeaways & Production Recommendation

- **Ingestion Overhead**: Unpartitioned writes were fastest, as Spark avoided directory creation and file handle thrashing. Daily partitioning suffered write amplification due to managing 91 separate partition writes.
- **Query Pruning**: On point-in-time queries (Q4 and Q5), partitioned tables demonstrated dramatic latency reductions by reading only the targeted Parquet files and bypassing the rest via Delta metadata skipping.
- **Production Verdict**: For the NYC Taxi dataset at current volume (~3M trips/month), **Strategy C (Monthly Partitioning)** provides the optimal balance: it achieves file sizes near ~100–150 MB, provides clean partition pruning for monthly analysis, and completely avoids the file fragmentation penalty of daily partitioning.
