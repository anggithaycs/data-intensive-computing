# Week 1 Urban Data Platform Design Report

The Week 1 platform brings four datasets into a shared Spark and Delta Lake pipeline. It produces one row per accepted taxi trip, with weather, air-quality and location information attached to pickup and dropoff. A common ingestion engine handles the repeated work of reading, validating and storing records, while dataset-specific transformations preserve the meaning of each source. The implementation is a local batch prototype; incremental processing and comprehensive failure monitoring remain future work.

## 1. Data Catalog and Source Profiling

| Dataset | Entity and operational key | Join and temporal attributes |
|---|---|---|
| Taxi Parquet | One recorded trip; trip_id hashes vendor, UTC endpoints, location IDs, fare and distance | Pickup/dropoff location IDs; containing UTC hour; local pickup calendar |
| Weather CSV | One source-hour observation; weather_observation_id combines year/month/day/hour | observation_datetime in UTC; source calendar and UTC date |
| Air quality CSV | One monitor occurrence/hour; air_quality_observation_id hashes state/county/site/POC/time | Derived borough and UTC observation time, date and hour |
| Zone CSV | One zone reference entry; location_id | Both taxi endpoint location IDs; no temporal source field |

The sources differ in both content and growth. Taxi records contain categories such as vendor, payment type, rate code and location IDs, and new monthly batches expand the trip fact table. Weather adds observations by hour or source series, with condition code and cloud cover describing categorical conditions. Air-quality data grows with the number of hours, sites and instruments, and includes county, instrument, parameter and unit categories. Zones change only through occasional reference updates. Their CSV contains borough, zone and service-zone labels and identifiers, but no polygon geometries.

The source keys also carry assumptions. Taxi data has no guaranteed natural key, so its hash defines which records count as duplicates within a snapshot. Indistinguishable physical trips can collapse, while a correction to fare or distance changes the hash. The weather key assumes a single series and would need a source identifier for multiple stations. The air-quality key assumes the supplied PM2.5 parameter; multiple pollutants would require a broader key and explicit unit validation.

### Observed scope and counts

The documented successful ingestion run, `4b1a2ae8e9a249abba67f2ea6051adb6`, processed Q1 2024 taxi files and full-year context sources. The counts below describe that ingestion. The benchmark report separately identifies the metrics artifact used for its storage comparison.

| Dataset | Accepted / input rows | Rejected / out of scope |
|---|---:|---:|
| Taxi | 9,394,330 / 9,554,778 | 160,448 / 0 |
| Weather | 8,784 / 8,784 | 0 / 0 |
| Air quality | 51,855 / 8,139,551 | 30 / 8,087,666 |
| Zones | 265 / 265 | 0 / 0 |

<!-- PAGEBREAK -->

## 2. Storage Architecture and Table Organization

The original CSV and Parquet files remain in `data/`. Each Delta table has a separate directory and transaction log, allowing it to be read directly by path. This local implementation therefore does not require an external catalog. The layers separate source snapshots, clean analytical data, rejected records and operational evidence:

| Path under storage/delta | Purpose | Publication policy |
|---|---|---|
| bronze/raw_* | Four source snapshots, sanitized names and ingestion timestamp | Overwrite; unpartitioned |
| silver/clean_* | Four typed, validated and deduplicated sources | Overwrite; taxi uses local year/month |
| silver/integrated_taxi_trips | Taxi facts with endpoint context | Overwrite after identity checks; local year/month |
| quarantine/<dataset> | Invalid and duplicate records with reasons and run metadata | Append separately per dataset |
| metadata/ingestion_runs | Successful ingestion statistics and completion time | Append |
| benchmark/<strategy>/<run_id> | Independent storage-layout experiments | Fresh directory for each execution |

Bronze retains the source fields with sanitized names and an ingestion timestamp; the original file bytes remain in `data/`. Quarantine keeps each dataset separate because rejected records can have different schemas. Audit and rejection tables allow additive schema merging, but incompatible type changes still need an explicit migration policy. Each Delta commit is atomic for one table, so a run does not commit all platform outputs as a single transaction.

### Partitioning and lookup roles

Clean and integrated taxi tables use `pickup_year` and `pickup_month` from New York local time as partition columns. Weather, NYC air quality and zones remain unpartitioned because they are small lookup inputs. Zone labels provide a spatial dimension. Weather observations and borough-hour or citywide-hour air aggregates provide temporal context, while the underlying monitor readings remain observation facts. An empty partition list disables partitioning for any configured output, including integration.

Directory partitioning determines how data is organized on disk, while writer parallelism determines how Spark distributes the write work. Before writing the integrated table, Spark redistributes records across the configured number of shuffle partitions. Neither the directory layout nor the partition-column choice guarantees a particular file size. Keys with many distinct values, sparsely populated partitions, skew and queries without useful partition filters can all make partitioning less effective.

### Scaling to twenty times the volume

A twentyfold increase could mean many more trips each day or a much longer date range. Those scenarios should be evaluated separately by re-measuring daily and monthly layouts, selective queries, writer parallelism, file sizes and compaction. Expansion to more cities may also require regional organization and a different broadcast strategy. Shared object storage, a catalog and incremental scheduling are possible future extensions. The current work does not include an experiment at twenty times the volume.

<!-- PAGEBREAK -->

## 3. Common Data Model and Quality Rules

All timestamps used for joins are Spark `TimestampType` values in a UTC session. They can be exchanged in the form `YYYY-MM-DDTHH:mm:ssZ`. Taxi wall-clock values are retained in `pickup_local_datetime` and `dropoff_local_datetime`, while the join timestamps are converted from `America/New_York` to UTC. Pickup date, year, month, day and hour retain local calendar meaning.

EPA observations use `Date GMT` and `Time GMT`, avoiding confusion between standard-time source fields and daylight-saving time. The weather CSV does not declare its timezone, so UTC is a configured assumption that must be confirmed from the export settings.

Columns use lowercase `snake_case`, with unit suffixes where defined. Configured identifiers and calendar fields are integers; measurements and monetary values are doubles; dates use `DateType`; labels and hash IDs are strings; and match flags are booleans. Taxi attributes without configured casts retain their source types. Doubles are suitable for this analytical prototype, while exact monetary accounting would require an explicit decimal policy.

### Missing values and validation

A missing taxi passenger count defaults to one, and missing tips or tolls default to zero. These assumptions can affect aggregates and should be considered during analysis. Malformed non-null values still cause rejection, even when a later default could replace them. Missing weather, precipitation and pollution measurements remain null. An unmatched zone label becomes `Unknown`, allowing the trip to remain in the output.

| Domain | Current acceptance conditions |
|---|---|
| Taxi | Valid UTC/local round-trip timestamps; rounded duration above 0.1 and at most 1,440 minutes; Q1 2024 pickup; fare 0-5,000, distance 0-500, passengers 0-10; finite nonnegative total/tips/tolls; present endpoint IDs |
| Weather | Parseable 2024 calendar; temperature -40 to 60, humidity 0-100; finite nonnegative wind and observed precipitation; finite positive observed pressure; cloud cover 0-8 and condition code 1-27 when present |
| Air | UTC year 2024; PM2.5 0-1,000; present monitor and borough fields after NYC scope filtering |
| Zones | Non-null location key and nonblank borough |
| All | Configured casts succeed; configured keys are non-null; one deterministic valid survivor per duplicate key |

These checks cover the configured contract, but they do not enforce every reference relationship, optional attribute or concentration unit. For example, taxi records with unknown location references survive with `Unknown` labels. The configuration contains the full mappings, casts, thresholds and clean-column projections, while [task_answers.md](task_answers.md) explains the main transformations. Schema changes should update both these contracts and the focused test fixtures.

<!-- PAGEBREAK -->

## 4. Reusable Ingestion Framework and Governance

`BaseDatasetIngestor` handles reading, schema checks, column names and casts, classification, duplicate selection, Delta writes and metadata. Four subclasses supply the domain logic: taxi timestamp, calendar and hash derivation; weather timestamp assembly; EPA geography and GMT handling; and zone-label trimming. The registry constructs each ingestor from an explicit dataset specification, schema version and audit path. There is no alternate constructor API for separate path or threshold arguments.

### Execution lifecycle

The runner validates the nested configuration before starting Spark. It then resolves source schemas, rules, output schemas and the integration schema before writing any table. CSV fields are read by header as strings, missing required columns cause explicit failures, and `FAILFAST` parsing rejects structural corruption. Cast mappings must refer to declared source fields by their standardized names. Requested clean-output columns must also exist; they are never silently skipped.

Ingestion first materializes raw rows on disk, then applies scope filtering and domain transformations. It also materializes the classified result so counts and writes can reuse it. Named SQL expressions define validity: a rule that evaluates to false or null rejects the row, and cast errors remain attached even after defaults are applied.

Within each key and validity group, rows are ranked lexicographically by their full-row JSON payload. Separating valid and invalid groups prevents an invalid row from displacing a valid duplicate. The ranking selects a deterministic survivor within a snapshot, but does not resolve corrections across snapshots.

The engine classifies records before publishing them. Raw rows overwrite Bronze, rejected rows append to quarantine, and accepted rows are projected to the clean schema before overwriting Silver. Reusing the classified result avoids calculating duplicate rankings separately for every count and write. Its performance benefit depends on the workload.

### Accounting and metadata

Every ingestion verifies `initial_records = valid_records + rejected_records + out_of_scope_records`. Rejected counts include duplicate occurrences, while invalid counts exclude them. Successful audit records contain the source format, counts, duration, schema version, paths, timezone, partition columns, execution ID and completion time. The duration excludes the final metadata write.

A shared runner ID links ingestion stages to the JSON summary. Timestamped summaries are retained, and the latest summary is replaced atomically. A summary or report failure produces a failing command exit, while any earlier processing exception is preserved. Successful Delta audit records do not provide comprehensive failure monitoring. Because publication happens table by table, a data or runtime failure can leave outputs at different refresh stages.

### Maintenance and future datasets

Adding a source requires a specification, registry entry, representative fixtures and any domain transformations it needs. A new file format also requires a reader. The current runner requires the registered Week 1 sources, and new integration relationships must be implemented separately.

Week 2 can reuse the output schema for SQL queries and analytical products. Week 3 needs a correction-identity policy, schema evolution and merge-based publication. Week 4 can build on preparation and integration with features and temporal splits designed for each prediction target.

<!-- PAGEBREAK -->

## 5. Contextual Integration Pipeline

Integration uses broadcast left joins for the small zone and weather lookups, retaining taxi trips even when context is missing. Each endpoint timestamp is truncated to its containing UTC hour and matched to the exact `observation_datetime`. For example, a January pickup at 08:37 in New York matches the 13:00 UTC observation. The pipeline does not search for the nearest hour, interpolate values or carry earlier observations forward.

Air-quality integration first averages collocated instruments within each physical site and UTC hour. It then gives sites equal weight when calculating borough-hour and citywide-hour means. All three site identifiers are required so that multiple instruments at one site do not give it disproportionate influence.

Separate joins retain `pickup_pm25_borough` and `pickup_pm25_citywide`, rounded to two decimal places. The convenience field `pickup_pm25` prefers the borough value, then the citywide value, and remains null if neither is available. `pickup_pm25_source` records its origin. Corresponding dropoff fields use the dropoff timestamp and borough.

Weather-match and precipitation-missing flags distinguish absent observations from measured values. Borough PM2.5 estimates remain null without local coverage. The citywide series describes the sites that report in each hour, rather than equal coverage of all five boroughs. The documented ingestion and integration run has 929,709 pickup borough matches and 8,464,621 citywide fallbacks.

Use the citywide series for citywide demand analysis. For borough comparisons, use borough estimates and report coverage alongside the results. Neither series measures exposure along a taxi route or establishes a causal relationship.

Before publication, the source and integrated tables must both have unique, non-null trip IDs. Anti-joins in both directions verify that their ID sets match exactly, detecting dropped, duplicated or substituted IDs. These checks preserve the declared one-row-per-trip-ID structure, but cannot establish whether the hash distinguishes every physical trip. The documented run retained all 9,394,330 accepted trip IDs.

## 6. Benchmark-Informed Design Tradeoffs

`StorageBenchmarkRunner` compares unpartitioned, local-day and local-year/month layouts. Each strategy independently reads and cleans the same raw taxi input, uses the same configured writer count, and writes to a fresh directory. Its ingestion timer includes source reading, schema checks, casts, transformations, materialization, validation, deduplication and the Delta write. Shared Bronze, quarantine and audit writes are excluded.

The three required queries calculate trips per pickup borough, average duration per local day and average fare per pickup borough. Two additional queries filter the data to February 14 and to February. The harness records storage sizes, file counts, raw query samples, summary statistics and `EXPLAIN FORMATTED` plans. It also verifies that all layouts produce equivalent query results.

The [benchmark report](benchmark_report.md) is the central numerical comparison, and its packaged JSON identifies the measured run and configuration. Fixed ingestion order, one ingestion trial per layout and warm query repetitions limit the conclusions. Timings alone do not prove partition-pruning effects or establish a universal winner. Monthly partitions remain the current workload-dependent default and should be reassessed against the Week 2 queries.

Sources: assignment_full_text.txt; config/platform_config.yaml; src/ingestion; src/integration/pipeline.py; src/storage; and the selected metrics artifact packaged in evidence/.
