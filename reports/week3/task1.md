# Week 3 Task 1: Generating Incremental Update Datasets

## Objective

Task 1 simulates a second data release containing new records and controlled schema evolution. These update files will later test incremental processing without rebuilding the Week 1 platform.

## Update Design

Taxi uses 7% new trips and 1% exact duplicates, calculated from the source row count. Weather and Air Quality continue hourly observations for a common seven-day simulated release period beginning immediately after the original maximum timestamp. Time-based continuation is used for environmental data because their observations are naturally defined by time coverage rather than a target percentage. The seven-day duration is an implementation choice because the assignment specifies an immediately following period but does not prescribe an exact duration. Weather adds numeric `humidity`; Air Quality adds numeric `aqi`.

## Results

| Metric | Taxi Trips | Weather | Air Quality |
|---|---:|---:|---:|
| Original records | 9554778 | 8784 | 8139551 |
| Update rows | 764382 | 168 | 363195 |
| New rows | 668834 | 168 | 363195 |
| Duplicate rows | 95548 | 0 | 0 |
| Rejected rows | 0 | 0 | 0 |
| Earliest update timestamp | 2025-01-01T01:00:01 | 2025-01-01T01:00:00 | 2025-01-01T01:00:00 |
| Latest update timestamp | 2025-01-07T23:59:55 | 2025-01-08T00:00:00 | 2025-01-08T00:00:00 |
| Schema version before | 2.0.0 | 2.0.0 | 2.0.0 |
| Schema version after | 2.0.0 | 2.1.0 | 2.1.0 |
| Schema evolution | None | `+ humidity` | `+ aqi` |
| New column detected | N/A | True | True |
| New-column minimum | N/A | 39.0 | 4.0 |
| New-column maximum | N/A | 66.0 | 500.0 |

For Taxi, update rows equal new rows plus duplicate rows. Duplicate rows are physically present but should later be ignored by incremental ingestion.

## Dataset Details

### Taxi Trips

The update contains 668834 new trips and 95548 exact duplicates. New trip timestamps range from 2025-01-01T01:00:01 to 2025-01-07T23:59:55. The original schema is preserved. New trips use deterministic source samples for locations, distances, fares, passenger counts and other attributes, with timestamps shifted into the common release period. The generator uses fixed seed 20250925.

### Weather

The update contains 168 hourly observations from 2025-01-01T01:00:00 through 2025-01-08T00:00:00. Existing raw columns and types are preserved, and `humidity` is added. Observed humidity ranges from 39.0 to 66.0. The schema evolves from 2.0.0 to 2.1.0.

### Air Quality

The update contains 363195 observations from 2025-01-01T01:00:00 through 2025-01-08T00:00:00, preserving the source's observed hourly multiplicity. Existing raw columns and types are preserved, and `aqi` is added. Observed AQI ranges from 4.0 to 500.0. The schema evolves from 2.0.0 to 2.1.0.

## Discussion

The Taxi update deliberately combines genuinely new records and exact duplicates to test duplicate detection. Weather and Air Quality use continuous hourly timestamps immediately after the original period. The seven-day window is a deliberate simulation choice. The new numeric `humidity` and `aqi` columns demonstrate schema evolution while existing columns and types remain preserved. Values are derived from original observations rather than arbitrary unrelated records. These files will be inputs to the later incremental ingestion implementation.

## Files Created

| File | Description |
|---|---|
| `data/updates/yellow_tripdata_2024_update01.parquet` | Taxi incremental update |
| `data/updates/weather_update.csv` | Weather incremental update with `humidity` |
| `data/updates/air_quality_update.csv` | Air Quality incremental update with `aqi` |

Generator: `scripts/generate_week3_task1.py`.

## Evidence

Measured statistics are stored in `storage/metrics/week3/task1_incremental_updates.json`.
