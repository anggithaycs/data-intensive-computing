# Week 3 Task 1: Generating Incremental Update Datasets

## 1. Objective

Task 1 simulates a second data release for the Taxi, Weather, and Air Quality datasets. The platform was extended to process these updates incrementally, preserve existing records, ignore matched business keys, and support safe schema evolution without rebuilding the Silver Delta tables.

## 2. Update Design

### Taxi Trips

The Taxi update contains 668,834 new trips and 95,548 exact duplicate rows, which is approximately 7% new data and approximately 1% duplicates relative to the generated source statistics. New Taxi trips start after the original relevant maximum timestamp, `2024-04-02 18:08:46`, and continue through `2025-01-08 00:00:00`. The original Taxi schema was preserved.

### Weather

The Weather update contains a seven-day hourly continuation from `2025-01-01 01:00:00` through `2025-01-08 00:00:00`. It adds the separate numeric column `humidity`.

### Air Quality

The Air Quality update contains a seven-day hourly continuation from `2025-01-01 01:00:00` through `2025-01-08 00:00:00`. It adds the separate numeric column `aqi`.

Unlike the environmental updates, Taxi uses 7% new trips and 1% exact duplicates calculated from its source row count. Weather adds numeric `humidity`, whereas Air Quality adds numeric `aqi`.

Taxi uses a different release range because its original source data ends earlier than the Weather and Air Quality source data. The generator used fixed seed `20250925`.

## 3. Generated Update Results

These are generated update-file statistics. They are separate from the results of incremental ingestion.

| Metric | Taxi Trips | Weather | Air Quality |
|---|---:|---:|---:|
| Original records | 9,554,778 | 8,784 | 8,139,551 |
| Update rows | 764,382 | 168 | 363,195 |
| New rows | 668,834 | 168 | 363,195 |
| Duplicate rows | 95,548 | 0 | 0 |
| Rejected rows | 0 | 0 | 0 |
| Earliest update timestamp | 2024-04-02T18:10:04 | 2025-01-01T01:00:00 | 2025-01-01T01:00:00 |
| Latest update timestamp | 2025-01-07T23:57:06 | 2025-01-08T00:00:00 | 2025-01-08T00:00:00 |
| Schema before | 2.0.0 | 2.0.0 | 2.0.0 |
| Schema after | 2.0.0 | 2.1.0 | 2.1.0 |
| Schema evolution | None | + humidity | + aqi |

The Taxi generated statistics must be distinguished from ingestion results: 95,548 rows were generated as source-level duplicates, representing 95,542 distinct duplicate trip IDs. During ingestion, 93,650 physical rows matched keys in the deduplicated Silver baseline, while 11,256 candidates were rejected by validation.

For Air Quality, the update file contained 362,187 repeated observation-key rows. There were no target duplicate matches, and 1,008 unique observations were inserted.

## 4. Incremental Ingestion Results

| Dataset | Input Rows | Inserted | Duplicate Matches | Rejected | Updated | Runtime |
|---|---:|---:|---:|---:|---:|---:|
| Taxi | 764,382 | 659,476 | 93,650 | 11,256 | 0 | 51.573 s |
| Weather | 168 | 168 | 0 | 0 | 0 | 14.298 s |
| Air Quality | 363,195 | 1,008 | 0 | 362,187 | 0 | 32.651 s |

The Delta data-changing transaction moved each table from version 0 to version 1. Weather and Air Quality later received automatic `OPTIMIZE` transactions.

## 5. Schema Evolution

Weather changed from schema version `2.0.0` to `2.1.0` by adding `humidity`. The existing `relative_humidity_pct` column remained unchanged. All 8,784 historical Weather rows have `NULL` humidity, and all 168 inserted observations have populated humidity.

Air Quality changed from schema version `2.0.0` to `2.1.0` by adding `aqi`. All 51,855 historical rows have `NULL` AQI, and the 1,008 inserted observations have populated AQI values ranging from 10.0 to 105.0.

Taxi remained at schema version `2.0.0`; no schema change was required. Existing columns and types were preserved for all three datasets.

## 6. Incremental Processing Strategy

The existing Week 1 validation and transformation components were reused:

- `TaxiTripsIngestor` generated and validated `trip_id`.
- `WeatherIngestor` generated `weather_observation_id`.
- `AirQualityIngestor` generated `air_quality_observation_id`.

Each incremental script reads the update, applies validation, compares business keys with the target, and uses Delta `MERGE`. Matched records are ignored, and unmatched valid records are inserted. No historical corrections or matched-row updates were simulated.

The incremental scripts use Delta merge operations rather than the existing snapshot-overwrite write path. Weather and Air Quality use supported schema evolution during the merge.

## 7. Correctness and Validation

The completed audit verified the following count arithmetic:

- Taxi: `9,394,330 + 659,476 = 10,053,806`
- Weather: `8,784 + 168 = 8,952`
- Air Quality: `51,855 + 1,008 = 52,863`

Inserted Taxi trip IDs were absent from the baseline, and historical Taxi records were unchanged. The generated Taxi duplicate population was larger than the deduplicated Silver baseline; this explains why the source-level duplicate count is 95,548 while target-matched duplicate rows are 93,650.

Weather and Air Quality historical records were preserved. New observations had populated values for their evolved columns, while historical values remained `NULL`. Existing columns remained present and compatible.

Delta history verified an initial `WRITE`, followed by an incremental `MERGE` for each dataset. No snapshot overwrite was detected, no unexpected Delta tables were modified, and the raw update files remained unchanged. No Task 2 work was performed.

## 8. Performance and Optimization

The update generator used a fixed seed and Spark-based generation with representative source statistics, avoiding repeated full-source scans for each generated record. The measured incremental ingestion runtimes were 51.573 seconds for Taxi, 14.298 seconds for Weather, and 32.651 seconds for Air Quality.

The main trade-off was validation of large update files against existing business keys. Air Quality had 363,195 physical input rows but only 1,008 unique observation keys after duplicate-key handling. This preserved key integrity but caused 362,187 rows to be rejected.

## 9. Discussion

The update datasets were generated with controlled release windows, duplicate content for Taxi, and new columns for Weather and Air Quality. Incremental ingestion used business-key matching and inserted only valid, unmatched observations. Existing Week 1 transformations and validation rules were reused, while Delta `MERGE` provided the Week 3 incremental behavior.

The Taxi result shows an important implementation observation: source-level duplicates do not all match the Silver target because the baseline Silver table is already deduplicated and validation may reject additional candidates. Therefore, generated duplicates, target-matched duplicates, and rejected rows are reported separately.

The Air Quality update also contains repeated observation keys within the update file. There were 0 target duplicate matches, 362,187 repeated-key rows rejected, and 1,008 observations inserted. This is a limitation of the observation-key granularity, not a rebuild or overwrite of the target.

Overall, the task preserved unchanged records, added the required schema fields, and avoided rebuilding the complete Silver tables.

## 10. Files Created / Modified

| File | Task 1 role |
|---|---|
| `scripts/generate_week3_task1.py` | Generates and validates the three update datasets |
| `scripts/run_week3_taxi_incremental.py` | Taxi incremental Delta merge |
| `scripts/run_week3_weather_incremental.py` | Weather incremental Delta merge with `humidity` |
| `scripts/run_week3_air_quality_incremental.py` | Air Quality incremental Delta merge with `aqi` |
| `storage/metrics/week3/task1_incremental_updates.json` | Generated statistics, ingestion results, and correctness evidence |
| `reports/week3/task1.md` | This Task 1 report |
