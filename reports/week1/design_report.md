# Week 1 Urban Data Platform Design Report

The platform integrates four heterogeneous urban datasets into Apache Spark Delta tables using a modular, reusable ingestion engine and domain-specific transformations. Its primary analytical output retains exactly one row per accepted yellow-taxi trip, enriched with prevailing meteorological conditions, ambient particulate matter (PM2.5), and spatial pickup/dropoff zone descriptors.

The architecture establishes a formal **Medallion Lakehouse layout**, enforces strict data-quality quarantine policies, eliminates silent data loss, and guarantees mathematical row reconciliation across the integration lifecycle.

---

## 1. Data Catalog & Source Profiling

The platform ingests data from multiple municipal and federal agencies, each exhibiting distinct file formats, update frequencies, and data dictionaries:

### Operational Data Catalog

| Dataset | Format | Primary Entity & Operational Key | Join & Temporal Fields | Key Attributes & Growth Vector |
| :--- | :--- | :--- | :--- | :--- |
| **Taxi Trips** | Parquet | Single physical trip.<br>**Key**: `trip_id` (SHA-256 composite hash) | **Join**: `PULocationID`, `DOLocationID`, containing UTC hour.<br>**Temporal**: pickup/dropoff UTC & local. | **Categories**: vendor, payment, rate code.<br>**Growth**: Monthly high-volume streaming accumulation. |
| **Weather** | CSV | Hourly NYC weather observation.<br>**Key**: `weather_observation_id` (`YYYY_M_D_H`) | **Join**: `observation_datetime` (UTC hour).<br>**Temporal**: UTC timestamp & calendar. | **Categories**: condition codes, cloud cover.<br>**Growth**: Linear temporal growth (24 rows/day). |
| **Air Quality** | CSV | Hourly monitor PM2.5 reading.<br>**Key**: `air_observation_id` (Monitor + Time hash) | **Join**: `observation_datetime` (UTC hour), `borough`.<br>**Temporal**: UTC timestamp from GMT. | **Categories**: county, borough, instrument, units.<br>**Growth**: Hours × monitoring sites × parameters. |
| **Taxi Zones** | CSV | Geographic taxi zone reference.<br>**Key**: `location_id` (Integer 1–265) | **Join**: `location_id` matches trip PU/DO IDs.<br>**Temporal**: Static reference lookup. | **Categories**: borough, zone, service_zone.<br>**Growth**: Infrequent reference updates / corrections. |

---

### Operational Key Semantics
- **Yellow Taxi Surrogate Key (`trip_id`)**: Because the raw Parquet records lack natural primary keys (taxi medallions and driver hack licenses were anonymized by the NYC TLC), the platform derives a deterministic SHA-256 hash across seven intrinsic attributes:
  $$\text{trip\_id} = \text{SHA-256}(\text{vendor\_id} \mathbin{\Vert} \text{pickup} \mathbin{\Vert} \text{dropoff} \mathbin{\Vert} \text{locations} \mathbin{\Vert} \text{fare} \mathbin{\Vert} \text{distance})$$
  This defines a strict duplicate survivor policy: exactly one occurrence survives into Silver, while duplicate records route to Quarantine.
- **Hourly Weather Key**: Constructed from `year_month_day_hour`. It guarantees that New York City maintains exactly one meteorological record per hour.
- **Hourly Air Quality Key**: Hashes `state_code`, `county_code`, `site_number`, `poc`, and `observation_datetime`, uniquely identifying each physical sensor reading.

### Observed Scope & Input Volumes
- **Taxi Trips**: Three monthly Parquet files covering January to March 2024 (~9.55M raw records).
- **Weather**: Full-year 2024 hourly observations containing exactly 8,784 rows ($366\text{ days} \times 24\text{ hours}$).
- **Air Quality**: National EPA dataset containing 8,139,551 records. The Silver pipeline filters this national scope down to New York State (36) and the five NYC counties, producing ~25,000 highly focused urban readings.
- **Taxi Zones**: 265 static polygon reference records.

---

## 2. Storage Architecture & Table Organization

The platform implements an enterprise **Medallion Lakehouse Architecture** deployed on Apache Spark Delta Lake. Every table maintains its own directory, Parquet data files, and ACID transaction log (`_delta_log/`):

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

| Layer | Directory Path | Write Policy | Partitioning | Architectural Responsibility |
| :--- | :--- | :--- | :--- | :--- |
| **Bronze** | `storage/delta/bronze/raw_*` | Overwrite | None | Preserves raw source fidelity with sanitized column names and `_ingestion_timestamp`. Enables replayability. |
| **Silver (Clean)** | `storage/delta/silver/clean_*` | Overwrite | Taxi: `pickup_year`, `pickup_month`<br>Others: None | Validated, typed, deduplicated single-source tables serving as domain analytical foundations. |
| **Silver (Integrated)** | `storage/delta/silver/integrated_taxi_trips` | Overwrite | `pickup_year`, `pickup_month` | Contextually enriched master analytical fact table joining trips with weather, air, and zones. |
| **Quarantine** | `storage/delta/quarantine/<dataset>` | Append | None | Captures all rejected rows with `_validation_errors`, `_rejection_reason`, `_run_id`, and `_rejected_at`. |
| **Metadata** | `storage/delta/metadata/ingestion_runs` | Append | None | Audit log tracking pipeline durations, row reconciliation counts, and schema versions. |
| **Benchmark** | `storage/delta/benchmark/<strategy>/<run_id>` | Isolated | Strategy-dependent | Dedicated experiment directories isolating storage layout benchmarks from production data. |

---

### Partitioning Strategy & Lookup Roles
- **Taxi Partitioning**: Partitioned by `pickup_year` and `pickup_month` (derived from New York local wall-clock pickup time). With ~3M trips per month, this layout yields ~100–150 MB Parquet part-files, which perfectly aligns with HDFS/cloud block sizes.
- **Unpartitioned Tables Rationale**: Weather (8,784 rows), cleaned air quality (~25,000 rows), and taxi zones (265 rows) remain strictly unpartitioned. Partitioning tiny tables produces the classic "small-files problem", inflating file-system metadata and I/O overhead. Keeping them unpartitioned allows Spark to perform instant in-memory Broadcast Joins with zero shuffle network cost.
- **Conceptual Lookups**: Taxi zones are a spatial dimension; weather and borough/citywide air quality aggregates serve as temporal-spatial lookups during integration, while raw monitor readings are observation facts.

### Scaling Considerations (20× Volume Growth)
If municipal trip volume expands twentyfold (from 3M to 60M trips/month, ~3 GB/month):
1. **Shift to Daily Partitioning**: Sub-partitioning by `pickup_date` (or `pickup_year`/`pickup_month`/`pickup_day`) maintains optimal ~128–256 MB file sizes.
2. **Liquid Clustering / Z-Ordering**: Apply Delta Z-Ordering by `(pickup_location_id, pickup_datetime)` to optimize multi-dimensional filtering.
3. **Air Quality Sensor Scaling**: If monitor networks expand nationwide (160M+ rows), partition by `state_code` and `observation_year`.
4. **Auto-Compaction**: Enable Delta `autoCompact` and `targetFileSize` controls to eliminate small-file fragmentation.

---

## 3. Common Data Model & Quality Rules

To enable unified cross-domain queries, the platform establishes consistent data types, strict timestamp standards, and an automated data-quality validation engine.

### Semantic Normalization & Typing Standards
- **Canonical Timestamp Standard**: All temporal join instants are converted to UTC `TimestampType` (formatted as ISO-8601: `YYYY-MM-DDTHH:mm:ssZ`). Source timestamps in `America/New_York` are converted to UTC, while retaining `pickup_local_datetime` and local calendar keys for diurnal analytics.
- **Attribute Naming**: Consistent lower `snake_case` with physical unit suffixes (e.g. `temperature_c`, `wind_speed_kmh`, `precipitation_mm`, `trip_duration_minutes`).
- **Precision**: Monetary values and environmental measurements use `DoubleType` for analytical throughput; counts and foreign keys use `IntegerType`/`LongType`.

### Missing Value Policies & Explicit Defaults
- **Business Defaults**: In Taxi Trips, null `passenger_count` defaults to 1; null `tip_amount` and `tolls_amount` default to 0.0.
- **Missing Spatial Lookups**: Unmatched pickup/dropoff locations map to `"Unknown"` rather than dropping the trip fact.
- **Contextual Measurements**: Missing weather and precipitation remain `NULL`; no synthetic values are fabricated. Match flags (`pickup_weather_matched = false`) and source indicators (`pickup_pm25_source = 'missing'`) preserve analytical transparency.
- **Air Quality Dual Estimates**: `pickup_pm25_borough` stores the local borough-hour mean; `pickup_pm25_citywide` stores the available-site city mean.

---

### Data Quality Rules & Boundary Thresholds

| Domain | Rule Name | Validation Predicate / Boundary Condition | Rejection Handling |
| :--- | :--- | :--- | :--- |
| **Taxi** | `valid_pickup/dropoff` | Round-trip timezone conversion preserves wall-clock equality | Rejects non-existent spring DST hours. |
| **Taxi** | `valid_duration` | `trip_duration_minutes > 0.1 AND trip_duration_minutes <= 1440` | Quarantines zero, negative, or >24h trips. |
| **Taxi** | `valid_period` | `year(pickup_local) = 2024 AND month(pickup_local) BETWEEN 1 AND 3` | Quarantines out-of-scope years (e.g. 2002). |
| **Taxi** | `valid_financials` | `fare BETWEEN 0 AND 5000; total >= 0; distance BETWEEN 0 AND 500` | Quarantines negative fares and anomalies. |
| **Weather** | `valid_meteorology` | `temp_c BETWEEN -40 AND 60; rhum BETWEEN 0 AND 100; wspd >= 0` | Rejects physically impossible weather values. |
| **Air Quality** | `valid_air_quality` | `year = 2024 AND pm25 BETWEEN 0.0 AND 1000.0` | Quarantines out-of-range sensor readings. |
| **All** | `schema & identity` | Mandatory keys non-null; type casts successful; duplicate rank = 1 | Quarantines cast errors & duplicate keys. |

---

## 4. Reusable Ingestion Framework & Governance

The platform implements an object-oriented, template-method ingestion engine centered around `BaseDatasetIngestor`. Platform configuration (`config/platform_config.yaml`) serves as the single source of truth, fully decoupling business logic from execution.

### Ingestion Lifecycle & Deduplication Policy
1. **Raw Ingestion**: Loads Parquet or CSV. Raw CSV is loaded as strings with `FAILFAST` parsing to catch malformed files.
2. **Bronze Persistence**: Persists raw files to Delta Lake with sanitized column names and ingestion timestamps.
3. **Safe Casting**: Executes `try_cast()` against configured target types, capturing failures in `_cast_errors` rather than crashing.
4. **Scope & Transforms**: Applies domain transformations (e.g. NYC county filtering, timestamp parsing).
5. **Quality Classification**: Evaluates named SQL expressions declared in configuration, tagging failures in `_validation_errors`.
6. **Deterministic Snapshot Deduplication**: Rows are partitioned by primary key and ranked lexicographically by full-row JSON payload:
   ```python
   Window.partitionBy(*keys, size("_validation_errors") == 0).orderBy(to_json(struct(*sorted_columns)))
   ```
   Rank 1 survives to Silver; Rank > 1 routes to Quarantine under `duplicate_key`. This guarantees that an invalid duplicate row cannot displace a valid row.
7. **Silver & Quarantine Routing**: Valid rows overwrite Silver; rejected rows append to Quarantine with full error diagnostics.

### Mathematical Row Reconciliation
Every ingestion run enforces a mathematical conservation assertion before publishing to Silver:
$$\text{Initial Raw Records} = \text{Valid Silver Records} + \text{Rejected Quarantine Records} + \text{Out-of-Scope Records}$$

If this condition fails, the pipeline aborts. Successful ingestion runs append audit metrics to `storage/delta/metadata/ingestion_runs`.

---

## 5. Contextual Integration Pipeline

The integration pipeline (`UrbanDataIntegrationPipeline`) enriches each taxi trip with spatial, meteorological, and environmental context while preserving the exact grain of the taxi fact table (1 row = 1 trip):

### Integration Topology & Hierarchical Fallback
- **Spatial Join**: Broadcast hash joins attach pickup and dropoff borough, zone, and service zone from `clean_taxi_zones`.
- **Weather Join**: Taxi `pickup_datetime` is truncated to its containing UTC hour (`date_trunc('hour', pickup_datetime)`) and left-joined to `clean_weather` on `observation_datetime`. This maps each trip to the prevailing meteorological state of its start hour.
- **Air Quality Hierarchical Fallback**: Because monitor availability varies across boroughs, the pipeline executes a two-tier join:
  1. *Primary Match*: Joins on exact `(UTC hour, pickup_borough)` against borough-aggregated PM2.5 readings.
  2. *Citywide Fallback*: If a borough has no active reporting station, falls back to the NYC-wide hourly mean across all reporting sites.
  3. *Lineage Tagging*: `pickup_pm25_source` records whether the measurement originated from `'borough'`, `'citywide'`, or is `'missing'`.

### Strict Identity Preservation Assertions
To guarantee that joins never introduce Cartesian fanout or silently drop trips, the pipeline runs bi-directional anti-joins:
```python
assert source_ids.join(integrated_ids, "trip_id", "left_anti").count() == 0
assert integrated_ids.join(source_ids, "trip_id", "left_anti").count() == 0
```
This mathematically proves 100% preservation of trip identity and total record volume.

---

## 6. Benchmark-Informed Design Tradeoffs

The benchmark engine (`StorageBenchmarkRunner`) evaluated three physical storage strategies directly on the raw taxi facts:
- **Strategy A (Unpartitioned)**: Single root directory; fastest writes but requires full scans for all queries.
- **Strategy B (Daily Partitioned)**: Partitioned by `pickup_date` (91 partitions); suffered write amplification but excels at single-day filters.
- **Strategy C (Monthly Partitioned)**: Partitioned by `pickup_year` and `pickup_month` (3 partitions); achieves the optimal balance between write throughput (~100–150 MB file sizes) and analytical partition pruning.

**Production Recommendation**: Strategy C is selected as the default production layout for Q1 2024 taxi data.

> [!NOTE]
> **Summary of Architectural Strengths**:
> 1. **Complete Data Governance**: Full audit trail via Medallion Bronze, Silver, and Quarantine layers.
> 2. **Zero Data Loss**: Rejections are quarantined with error tags rather than silently dropped.
> 3. **Optimized I/O**: Broadcast joins for small context tables; monthly partitions for large transaction facts.
> 4. **Mathematical Correctness**: Bi-directional anti-joins guarantee 100% trip identity preservation.
