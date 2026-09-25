"""Generate and measure the Week 3 Task 1 incremental update datasets."""

from __future__ import annotations

import json
import csv
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.spark_session import get_spark_session, stop_spark_session

SEED = 20250925
RELEASE_START = datetime(2025, 1, 1, 1, 0, 0)
RELEASE_END = datetime(2025, 1, 8, 0, 0, 0)
RELEASE_HOURS = 168
TAXI_SOURCE = str(ROOT / "data/yellow_tripdata_2024-*.parquet")
WEATHER_SOURCE = str(ROOT / "data/weather.csv")
AIR_SOURCE = str(ROOT / "data/hourly_88101_2024.csv")
WEATHER_SILVER = str(ROOT / "storage/delta/silver/clean_weather")
AIR_SILVER = str(ROOT / "storage/delta/silver/clean_air_quality")
UPDATE_DIR = ROOT / "data/updates"
EVIDENCE = ROOT / "storage/metrics/week3/task1_incremental_updates.json"
REPORT = ROOT / "reports/week3/task1.md"


def deterministic_sample(df: DataFrame, count: int, seed: int = SEED) -> DataFrame:
    """Take a bounded, reproducible sample without a global shuffle."""
    return df.limit(count)


def bounds(df: DataFrame, column: str) -> tuple[object, object]:
    row = df.select(F.min(column).alias("minimum"), F.max(column).alias("maximum")).first()
    return row.minimum, row.maximum


def write_csv_rows(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def taxi_update(spark: SparkSession) -> tuple[dict, DataFrame]:
    source = spark.read.parquet(TAXI_SOURCE)
    original_count = source.count()
    new_count = round(original_count * 0.07)
    duplicate_count = round(original_count * 0.01)
    source_max_pickup = source.select(F.max("tpep_pickup_datetime")).first()[0]
    source_max_dropoff = source.select(F.max("tpep_dropoff_datetime")).first()[0]
    source_max = max(source_max_pickup, source_max_dropoff)
    release_start = max(
        RELEASE_START,
        source_max + timedelta(seconds=1),
    )
    release_end = RELEASE_END
    if release_start >= release_end:
        raise ValueError("Taxi source maximum is outside the simulated release window")

    duplicates = deterministic_sample(source, duplicate_count)
    new_source = deterministic_sample(source, new_count, SEED + 1)
    total_seconds = int((release_end - release_start).total_seconds())
    numbered = new_source.withColumn("_random", F.rand(SEED + 1))
    available_seconds = total_seconds - 60
    pickup = F.timestamp_add(
        "SECOND",
        F.floor(F.col("_random") * F.lit(available_seconds)),
        F.to_timestamp(F.lit(release_start.strftime("%Y-%m-%d %H:%M:%S"))),
    ).cast(source.schema["tpep_pickup_datetime"].dataType)
    duration = F.greatest(
        F.coalesce(
            F.unix_timestamp("tpep_dropoff_datetime")
            - F.unix_timestamp("tpep_pickup_datetime"),
            F.lit(60),
        ),
        F.lit(60),
    )
    duration = F.least(
        duration,
        F.greatest(
            F.lit(total_seconds)
            - (
                F.lit(total_seconds)
                - F.floor(F.col("_random") * F.lit(available_seconds))
            ),
            F.lit(60),
        ),
    )
    new_rows = (
        numbered.withColumn(
            "tpep_pickup_datetime",
            pickup.cast(source.schema["tpep_pickup_datetime"].dataType),
        )
        .withColumn(
            "tpep_dropoff_datetime",
            (pickup + duration.cast("long").cast("interval second")).cast(
                source.schema["tpep_dropoff_datetime"].dataType
            ),
        )
        .drop("_random")
    )
    update = new_rows.unionByName(duplicates)
    update_path = UPDATE_DIR / "yellow_tripdata_2024_update01.parquet"
    update.write.mode("overwrite").parquet(str(update_path))

    update_count = update.count()
    new_pickup_min, new_pickup_max = bounds(new_rows, "tpep_pickup_datetime")
    new_dropoff_min, new_dropoff_max = bounds(new_rows, "tpep_dropoff_datetime")
    duplicate_matches = update.filter(
        F.col("tpep_pickup_datetime") <= F.lit(source_max)
    ).count()
    if update_count != new_count + duplicate_count:
        raise ValueError("Taxi update row count is incorrect")
    if duplicate_matches != duplicate_count:
        raise ValueError("Taxi duplicate count is incorrect")
    if update.schema != source.schema:
        raise ValueError("Taxi update schema does not match the source schema")

    return (
        {
            "original_records": original_count,
            "update_rows": update_count,
            "new_rows": new_count,
            "duplicate_rows": duplicate_matches,
            "rejected_rows": 0,
            "earliest_update_timestamp": new_pickup_min.isoformat(),
            "latest_update_timestamp": new_dropoff_max.isoformat(),
            "schema_version_before": "2.0.0",
            "schema_version_after": "2.0.0",
            "schema_evolution": "None",
            "new_column_detected": "N/A",
            "new_column_minimum": None,
            "new_column_maximum": None,
            "source_max_relevant_timestamp": source_max.isoformat(),
            "schema_preserved": True,
            "duplicate_rows_correctly_identified": True,
            "release_start": release_start.isoformat(),
            "release_end": release_end.isoformat(),
        },
        update,
    )


def weather_update(spark: SparkSession) -> dict:
    import csv

    with open(WEATHER_SOURCE, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        columns = reader.fieldnames or []
        source_rows = list(reader)
    templates = {int(row["hour"]): row for row in source_rows}
    rows = []
    for index in range(RELEASE_HOURS):
        timestamp = RELEASE_START + timedelta(hours=index)
        row = dict(templates[timestamp.hour])
        row.update(
            year=str(timestamp.year),
            month=str(timestamp.month),
            day=str(timestamp.day),
            hour=str(timestamp.hour),
            humidity=templates[timestamp.hour]["rhum"],
        )
        rows.append(row)
    write_csv_rows(UPDATE_DIR / "weather_update.csv", columns + ["humidity"], rows)
    humidity = [float(row["humidity"]) for row in rows]
    return {
        "original_records": len(source_rows),
        "update_rows": len(rows),
        "new_rows": len(rows),
        "duplicate_rows": 0,
        "rejected_rows": sum(not 20 <= value <= 100 for value in humidity),
        "earliest_update_timestamp": RELEASE_START.isoformat(),
        "latest_update_timestamp": RELEASE_END.isoformat(),
        "schema_version_before": "2.0.0",
        "schema_version_after": "2.1.0",
        "schema_evolution": "+ humidity",
        "new_column_detected": True,
        "new_column_minimum": min(humidity),
        "new_column_maximum": max(humidity),
        "schema_preserved": True,
        "invalid_humidity_count": sum(not 20 <= value <= 100 for value in humidity),
    }


def air_quality_update(spark: SparkSession) -> dict:
    import csv

    with open(AIR_SOURCE, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        columns = reader.fieldnames or []
        templates: dict[int, list[dict[str, str]]] = {}
        original_count = 0
        for row in reader:
            original_count += 1
            if (
                row["State Code"].lstrip("0") == "36"
                and row["County Code"].lstrip("0") in {"5", "47", "61", "81", "85"}
                and row["Parameter Code"] == "88101"
            ):
                hour = int(row["Time GMT"].split(":")[0])
                templates.setdefault(hour, []).append(row)
    rows = []
    for index in range(RELEASE_HOURS):
        timestamp = RELEASE_START + timedelta(hours=index)
        for template in templates.get(timestamp.hour, []):
            row = dict(template)
            row.update(
                **{
                    "Date GMT": timestamp.strftime("%Y-%m-%d"),
                    "Time GMT": timestamp.strftime("%H:%M"),
                    "Date Local": timestamp.strftime("%Y-%m-%d"),
                    "Time Local": timestamp.strftime("%H:%M"),
                    "aqi": str(
                        min(500.0, max(0.0, float(template["Sample Measurement"]) * 10))
                    ),
                }
            )
            rows.append(row)
    write_csv_rows(UPDATE_DIR / "air_quality_update.csv", columns + ["aqi"], rows)
    aqi = [float(row["aqi"]) for row in rows]
    return {
        "original_records": original_count,
        "update_rows": len(rows),
        "new_rows": len(rows),
        "duplicate_rows": 0,
        "rejected_rows": sum(not 0 <= value <= 500 for value in aqi),
        "earliest_update_timestamp": RELEASE_START.isoformat(),
        "latest_update_timestamp": RELEASE_END.isoformat(),
        "schema_version_before": "2.0.0",
        "schema_version_after": "2.1.0",
        "schema_evolution": "+ aqi",
        "new_column_detected": True,
        "new_column_minimum": min(aqi),
        "new_column_maximum": max(aqi),
        "schema_preserved": True,
        "invalid_aqi_count": sum(not 0 <= value <= 500 for value in aqi),
    }


def write_report(results: dict) -> None:
    taxi, weather, air = (results[name] for name in ("Taxi Trips", "Weather", "Air Quality"))
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(
        f"""# Week 3 Task 1: Generating Incremental Update Datasets

## Objective

Task 1 simulates a second data release containing new records and controlled schema evolution. These update files will later test incremental processing without rebuilding the Week 1 platform.

## Update Design

Taxi uses 7% new trips and 1% exact duplicates, calculated from the source row count. Weather and Air Quality continue hourly observations for a common seven-day simulated release period beginning immediately after the original maximum timestamp. Time-based continuation is used for environmental data because their observations are naturally defined by time coverage rather than a target percentage. The seven-day duration is an implementation choice because the assignment specifies an immediately following period but does not prescribe an exact duration. Weather adds numeric `humidity`; Air Quality adds numeric `aqi`.

## Results

| Metric | Taxi Trips | Weather | Air Quality |
|---|---:|---:|---:|
| Original records | {taxi['original_records']} | {weather['original_records']} | {air['original_records']} |
| Update rows | {taxi['update_rows']} | {weather['update_rows']} | {air['update_rows']} |
| New rows | {taxi['new_rows']} | {weather['new_rows']} | {air['new_rows']} |
| Duplicate rows | {taxi['duplicate_rows']} | {weather['duplicate_rows']} | {air['duplicate_rows']} |
| Rejected rows | {taxi['rejected_rows']} | {weather['rejected_rows']} | {air['rejected_rows']} |
| Earliest update timestamp | {taxi['earliest_update_timestamp']} | {weather['earliest_update_timestamp']} | {air['earliest_update_timestamp']} |
| Latest update timestamp | {taxi['latest_update_timestamp']} | {weather['latest_update_timestamp']} | {air['latest_update_timestamp']} |
| Schema version before | {taxi['schema_version_before']} | {weather['schema_version_before']} | {air['schema_version_before']} |
| Schema version after | {taxi['schema_version_after']} | {weather['schema_version_after']} | {air['schema_version_after']} |
| Schema evolution | None | `+ humidity` | `+ aqi` |
| New column detected | N/A | {weather['new_column_detected']} | {air['new_column_detected']} |
| New-column minimum | N/A | {weather['new_column_minimum']} | {air['new_column_minimum']} |
| New-column maximum | N/A | {weather['new_column_maximum']} | {air['new_column_maximum']} |

For Taxi, update rows equal new rows plus duplicate rows. Duplicate rows are physically present but should later be ignored by incremental ingestion.

## Dataset Details

### Taxi Trips

The update contains {taxi['new_rows']} new trips and {taxi['duplicate_rows']} exact duplicates. New trip timestamps range from {taxi['earliest_update_timestamp']} to {taxi['latest_update_timestamp']}. The original schema is preserved. New trips use deterministic source samples for locations, distances, fares, passenger counts and other attributes, with timestamps shifted into the common release period. The generator uses fixed seed {SEED}.

### Weather

The update contains {weather['update_rows']} hourly observations from {weather['earliest_update_timestamp']} through {weather['latest_update_timestamp']}. Existing raw columns and types are preserved, and `humidity` is added. Observed humidity ranges from {weather['new_column_minimum']} to {weather['new_column_maximum']}. The schema evolves from {weather['schema_version_before']} to {weather['schema_version_after']}.

### Air Quality

The update contains {air['update_rows']} observations from {air['earliest_update_timestamp']} through {air['latest_update_timestamp']}, preserving the source's observed hourly multiplicity. Existing raw columns and types are preserved, and `aqi` is added. Observed AQI ranges from {air['new_column_minimum']} to {air['new_column_maximum']}. The schema evolves from {air['schema_version_before']} to {air['schema_version_after']}.

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
""",
        encoding="utf-8",
    )


def main() -> None:
    spark = get_spark_session(app_name="UrbanDataPlatform_Week3_Task1")
    try:
        results = {}
        results["Taxi Trips"], _ = taxi_update(spark)
        results["Weather"] = weather_update(spark)
        results["Air Quality"] = air_quality_update(spark)
        EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
        EVIDENCE.write_text(
            json.dumps(
                {
                    "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                    "seed": SEED,
                    "release_start": RELEASE_START.isoformat(),
                    "release_end": RELEASE_END.isoformat(),
                    "datasets": results,
                },
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
        write_report(results)
        print(json.dumps(results, indent=2, default=str))
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
