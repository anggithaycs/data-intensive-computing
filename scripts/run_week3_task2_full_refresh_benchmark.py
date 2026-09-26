"""Benchmark full Week 2 Gold refreshes against the Step 5 incremental outputs."""

from __future__ import annotations

import json
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
PRODUCT_EVIDENCE = ROOT / "storage/metrics/week3/product_comparison.json"
INTEGRATION_EVIDENCE = ROOT / "storage/metrics/week3/task2_integration_refresh.json"
OUTPUT_EVIDENCE = ROOT / "storage/metrics/week3/analytical_refresh.json"
INTEGRATED_PATH = ROOT / "storage/delta/silver/integrated_taxi_trips"
GOLD_ROOT = ROOT / "storage/delta/gold"
STAGING_ROOT = ROOT / "storage/tmp/week3"
PRODUCT_NAMES = (
    "daily_mobility_summary",
    "taxi_zone_statistics",
    "weather_impact_summary",
    "air_quality_impact_summary",
)
KEYS = {
    "daily_mobility_summary": ("pickup_date", "pickup_borough"),
    "taxi_zone_statistics": ("month", "pickup_location_id"),
    "weather_impact_summary": (
        "pickup_location_id",
        "pickup_zone",
        "weather",
    ),
    "air_quality_impact_summary": ("hour_utc",),
}
FLOAT_TOLERANCE = 1e-8


def week2_sql(relative_path: str) -> str:
    return subprocess.check_output(
        ["git", "show", f"week2:src/week2/sql/{relative_path}"],
        cwd=ROOT,
        text=True,
    )


def current_version(spark: SparkSession, path: str | Path) -> int:
    return int(
        DeltaTable.forPath(spark, str(path))
        .history(1)
        .select("version")
        .first()["version"]
    )


def schema_description(frame: DataFrame) -> list[dict[str, str]]:
    return [
        {"name": field.name, "type": field.dataType.typeName()}
        for field in frame.schema.fields
    ]


def compare_outputs(
    incremental: DataFrame, full: DataFrame, name: str
) -> dict[str, int]:
    keys = KEYS[name]
    incremental_schema = schema_description(incremental)
    full_schema = schema_description(full)
    if incremental_schema != full_schema:
        raise ValueError(
            f"{name}: schema mismatch: {incremental_schema} != {full_schema}"
        )

    for label, frame in (("incremental", incremental), ("full", full)):
        duplicates = frame.groupBy(*keys).count().filter(F.col("count") != 1).count()
        if duplicates:
            raise ValueError(f"{name}: {label} output has {duplicates} duplicate keys")

    missing = full.select(*keys).join(incremental.select(*keys), list(keys), "left_anti").count()
    unexpected = incremental.select(*keys).join(full.select(*keys), list(keys), "left_anti").count()
    joined = incremental.alias("incremental").join(full.alias("full"), list(keys), "full")

    differences = []
    for field in incremental.schema.fields:
        if field.name in keys:
            continue
        left = F.col(f"incremental.`{field.name}`")
        right = F.col(f"full.`{field.name}`")
        if field.dataType.simpleString() in {"double", "float"}:
            differs = (left.isNull() != right.isNull()) | (
                left.isNotNull()
                & right.isNotNull()
                & (
                    F.abs(left - right)
                    > F.lit(FLOAT_TOLERANCE)
                    + F.lit(FLOAT_TOLERANCE) * F.greatest(F.abs(left), F.abs(right))
                )
            )
        else:
            differs = ~left.eqNullSafe(right)
        differences.append(differs)

    value_mismatches = 0
    if differences:
        mismatch = differences[0]
        for difference in differences[1:]:
            mismatch = mismatch | difference
        value_mismatches = joined.filter(mismatch).count()

    results = {
        "missing_keys": int(missing),
        "unexpected_keys": int(unexpected),
        "value_mismatches": int(value_mismatches),
    }
    if any(results.values()):
        raise ValueError(f"{name}: outputs differ: {results}")
    return results


def validate_gold_unchanged(
    spark: SparkSession, snapshots: dict[str, dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    actual_snapshots = {}
    for name, before in snapshots.items():
        path = GOLD_ROOT / name
        version = current_version(spark, path)
        product = spark.read.format("delta").load(str(path))
        count = int(product.count())
        schema = schema_description(product)
        actual_snapshots[name] = {
            "delta_version": version,
            "row_count": count,
            "schema": schema,
        }
        if (
            version != before["delta_version"]
            or count != before["row_count"]
            or schema != before["schema"]
        ):
            raise RuntimeError(
                f"{name}: live Gold changed during benchmark "
                f"(version {before['delta_version']}->{version}, "
                f"rows {before['row_count']}->{count})"
            )
    return actual_snapshots


def run(spark: SparkSession) -> dict[str, Any]:
    comparison = json.loads(PRODUCT_EVIDENCE.read_text(encoding="utf-8"))
    integration = json.loads(INTEGRATION_EVIDENCE.read_text(encoding="utf-8"))
    integrated_version = current_version(spark, INTEGRATED_PATH)
    expected_integrated_version = int(comparison["integrated_delta_version"])
    if integrated_version != expected_integrated_version:
        raise ValueError(
            f"Expected integrated Delta version {expected_integrated_version}, "
            f"found {integrated_version}"
        )

    integrated = spark.read.format("delta").load(str(INTEGRATED_PATH))
    integrated_rows = int(integrated.count())
    if integrated_rows != int(integration["execution"]["final_rows"]):
        raise ValueError("Integrated row count does not match Step 3 evidence")
    integrated.createOrReplaceTempView("integrated_trips")
    spark.sql(week2_sql("views/trips.sql")).createOrReplaceTempView("trips")
    coverage = spark.sql(week2_sql("views/coverage.sql")).first().asDict()
    calendar_sql = week2_sql("views/calendar_hours.sql").format(
        start=coverage["first_date"],
        end=coverage["last_date"],
    )
    spark.sql(calendar_sql).createOrReplaceTempView("calendar_hours")
    for view_name in ("hourly_demand", "zone_weather"):
        spark.sql(week2_sql(f"views/{view_name}.sql")).createOrReplaceTempView(
            view_name
        )

    product_sql = {
        "daily_mobility_summary": week2_sql(
            "products/daily_mobility_summary.sql"
        ),
        "taxi_zone_statistics": week2_sql("products/taxi_zone_statistics.sql"),
        "weather_impact_summary": "SELECT * FROM zone_weather",
        "air_quality_impact_summary": "SELECT * FROM hourly_demand",
    }

    gold_snapshots: dict[str, dict[str, Any]] = {}
    for name in PRODUCT_NAMES:
        gold_path = GOLD_ROOT / name
        live = spark.read.format("delta").load(str(gold_path))
        expected = comparison["products"][name]
        version = current_version(spark, gold_path)
        row_count = int(live.count())
        if (
            version != int(expected["updated_delta_version"])
            or row_count != int(expected["updated_row_count"])
        ):
            raise ValueError(
                f"{name}: current Gold state differs from Step 5 evidence"
            )
        gold_snapshots[name] = {
            "delta_version": version,
            "row_count": row_count,
            "schema": schema_description(live),
        }

    STAGING_ROOT.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    run_root = (STAGING_ROOT / f"task2_full_refresh_{run_id}").resolve()
    allowed_staging_root = STAGING_ROOT.resolve()
    if allowed_staging_root not in run_root.parents or run_root.exists():
        raise RuntimeError(f"Unsafe or pre-existing staging path: {run_root}")

    product_results: dict[str, Any] = {}
    try:
        for name in PRODUCT_NAMES:
            staging_path = run_root / name
            started = time.perf_counter()
            product = spark.sql(product_sql[name]).coalesce(1)
            (
                product.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(str(staging_path))
            )
            full_refresh_seconds = time.perf_counter() - started

            full_output = spark.read.format("delta").load(str(staging_path))
            full_rows = int(full_output.count())
            incremental_output = spark.read.format("delta").load(
                str(GOLD_ROOT / name)
            )
            incremental_rows = int(incremental_output.count())
            correctness = compare_outputs(incremental_output, full_output, name)
            if full_rows != incremental_rows:
                raise ValueError(
                    f"{name}: row count differs: {incremental_rows} != {full_rows}"
                )

            incremental_seconds = float(
                comparison["products"][name]["incremental_refresh_time_seconds"]
            )
            product_results[name] = {
                "incremental_seconds": incremental_seconds,
                "full_refresh_seconds": round(full_refresh_seconds, 6),
                "refresh_savings": round(
                    1 - incremental_seconds / full_refresh_seconds, 6
                ),
                "speedup": round(full_refresh_seconds / incremental_seconds, 6),
                "equivalent": True,
                "incremental_scope": comparison["products"][name][
                    "affected_scope"
                ],
                "full_scope": {
                    "source": "integrated_taxi_trips",
                    "integrated_delta_version": integrated_version,
                    "source_rows": integrated_rows,
                    "pickup_date_range": {
                        "start": str(coverage["first_date"]),
                        "end": str(coverage["last_date"]),
                    },
                    "grouping_keys": list(KEYS[name]),
                },
                "row_count": full_rows,
                "schema": schema_description(full_output),
                "correctness": correctness,
                "temporary_output_path": str(staging_path),
            }

        unchanged_gold = validate_gold_unchanged(spark, gold_snapshots)
        evidence = {
            "benchmark_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "integrated_delta_version": integrated_version,
            "integrated_row_count": integrated_rows,
            "methodology": (
                "For each product, execute the unchanged Week 2 SQL/views over the "
                "current integrated Delta snapshot; apply the Week 2 builder's "
                "coalesce(1), Delta overwriteSchema write to an isolated staging "
                "path. Full-refresh timing covers SQL plan construction and the "
                "materializing Delta write, matching the Week 2 builder's timer. "
                "Staged output row counts, schemas, business keys, and aggregate "
                "values are validated against the live Step 5 Gold Delta output. "
                "The post-write row-count and equivalence validation are excluded "
                "from full-refresh timing."
            ),
            "incremental_baseline_source": str(PRODUCT_EVIDENCE.relative_to(ROOT)),
            "staging_root": str(run_root),
            "live_gold_before": gold_snapshots,
            "live_gold_after_unchanged": unchanged_gold,
            "products": product_results,
            "all_products_equivalent": all(
                value["equivalent"] for value in product_results.values()
            ),
            "temporary_outputs_cleaned": False,
        }
    except Exception:
        # Preserve staged results on failure so the mismatch can be investigated.
        raise
    else:
        shutil.rmtree(run_root)
        evidence["temporary_outputs_cleaned"] = True
        evidence["staging_root"] = str(run_root)
        evidence["live_gold_after_unchanged"] = validate_gold_unchanged(
            spark, gold_snapshots
        )
        OUTPUT_EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_EVIDENCE.write_text(
            json.dumps(evidence, indent=2) + "\n", encoding="utf-8"
        )
        return evidence


def main() -> None:
    from src.common.config import load_config
    from src.common.spark_session import get_spark_session, stop_spark_session

    config = load_config(ROOT / "config/platform_config.yaml")
    spark = get_spark_session(**config["spark"])
    try:
        print(json.dumps(run(spark), indent=2, default=str))
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
