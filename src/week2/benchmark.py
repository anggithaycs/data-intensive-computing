"""Time complete query results and change one optimization at a time."""

import json
import math
import statistics
import time
from pathlib import Path

from src.week2.analysis import QUERIES, QUERY_KEYS
from src.week2.products import GOLD_QUERIES
from src.week2.sql_files import read_sql

# Each file is the executed plan for one side of a focused comparison.
PLAN_FILES = {
    "weather_trip_distance_silver": "caching_before.txt",
    "weather_trip_distance_cached": "caching_after.txt",
    "month_date_filter": "partition_pruning_before.txt",
    "month_partition_filter": "partition_pruning_after.txt",
    "zone_join_no_broadcast": "broadcast_join_before.txt",
    "zone_join_broadcast": "broadcast_join_after.txt",
    "weather_demand_variation_silver": "aqe_before.txt",
    "weather_demand_variation_aqe": "aqe_after.txt",
}


def check_equal(expected, actual, keys):
    """Match rows by business key; counts are exact, float aggregates tolerate rounding."""
    left = {tuple(row[k] for k in keys): row.asDict() for row in expected}
    right = {tuple(row[k] for k in keys): row.asDict() for row in actual}

    if len(left) != len(expected) or len(right) != len(actual):
        raise AssertionError("Duplicate result keys")

    if left.keys() != right.keys():
        raise AssertionError("Result keys differ")

    for key, row in left.items():
        if row.keys() != right[key].keys():
            raise AssertionError("Result columns differ")

        for name, value in row.items():
            other = right[key][name]
            if isinstance(value, float) and isinstance(other, float):
                equal = (
                    math.isfinite(value)
                    and math.isfinite(other)
                    and math.isclose(value, other, rel_tol=1e-9, abs_tol=1e-8)
                )
            else:
                equal = value == other
            if not equal:
                raise AssertionError(f"Mismatch at {key}, {name}: {value} != {other}")


def measure(spark, sql, keys, repeats=5, expected=None):
    """One warmup, then repeated full collect actions. Checks are outside the timer."""
    if repeats < 1:
        raise ValueError("repeats must be positive")

    warmup = spark.sql(sql).collect()
    reference = warmup if expected is None else expected
    check_equal(reference, warmup, keys)
    samples = []

    for _ in range(repeats):
        start = time.perf_counter()
        frame = spark.sql(sql)
        rows = frame.collect()  # count() could skip the calculation of result columns.
        samples.append((time.perf_counter() - start) * 1000)
        check_equal(reference, rows, keys)

    final_plan = spark._jvm.PythonSQLUtils.explainString(
        frame._jdf.queryExecution(), "formatted"
    )

    return {
        "samples_ms": samples,
        "median_ms": statistics.median(samples),
        "min_ms": min(samples),
        "max_ms": max(samples),
        "stdev_ms": statistics.stdev(samples) if repeats > 1 else 0,
        "rows": len(rows),
        "equivalent": True,
        "sql": sql,
        "final_plan": final_plan,
    }, reference


def run_benchmarks(
    spark, output, product_metrics, coverage, repeats=5, month="2024-02"
):
    """Compare all six Silver/Gold queries, plus four focused experiments.

    Do not change Silver tables during the run. Gold must have just been rebuilt.
    The runner guarantees this by calling build_products before this function.
    """
    from datetime import date

    month_start = date.fromisoformat(month + "-01")

    if repeats < 1:
        raise ValueError("repeats must be positive")

    output = Path(output)
    (output / "plans").mkdir(parents=True, exist_ok=True)
    settings = {
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.adaptive.coalescePartitions.enabled": "false",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.adaptive.autoBroadcastJoinThreshold": "-1",
    }
    previous = {key: spark.conf.get(key, None) for key in settings}
    report = {
        "status": "running",
        "repeats": repeats,
        "month": month,
        "coverage": coverage,
        "products": product_metrics,
        "cases": {},
        "spark_version": spark.version,
        "master": spark.sparkContext.master,
        "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
        "baseline_settings": settings,
    }

    def record(name, sql, keys, expected=None):
        print(f"Benchmark: {name}", flush=True)
        stats, rows = measure(spark, sql, keys, repeats, expected)
        report["cases"][name] = stats

        if name in PLAN_FILES:
            (output / "plans" / PLAN_FILES[name]).write_text(
                stats["final_plan"], encoding="utf-8"
            )

        return rows

    try:
        for key, value in settings.items():
            spark.conf.set(key, value)

        spark.catalog.clearCache()
        answers = {}

        for name, sql in QUERIES.items():
            answers[name] = record(name + "_silver", sql, QUERY_KEYS[name])

        for name, sql in GOLD_QUERIES.items():
            record(name + "_gold", sql, QUERY_KEYS[name], answers[name])

        # 1. Caching: cache the same input, keeping SQL and all other settings fixed.
        start = time.perf_counter()
        spark.sql("CACHE TABLE trips OPTIONS ('storageLevel' 'MEMORY_AND_DISK')")
        spark.table("trips").count()
        report["cache_population_seconds"] = time.perf_counter() - start
        name = "weather_trip_distance"
        record(name + "_cached", QUERIES[name], QUERY_KEYS[name], answers[name])
        cached_plan = report["cases"][name + "_cached"]["final_plan"]

        if (
            "Scan In-memory table" not in cached_plan
            and "InMemoryTableScan" not in cached_plan
        ):
            raise AssertionError("Caching experiment did not use a cached scan")

        spark.catalog.clearCache()

        # 2. AQE: enable adaptive execution and shuffle coalescing only.
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        name = "weather_demand_variation"
        record(name + "_aqe", QUERIES[name], QUERY_KEYS[name], answers[name])
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")

        # 3. Pruning: identical monthly answer, with explicit partition predicates.
        month_check_sql = read_sql("benchmarks/month_has_trips.sql").format(
            month_start=month_start
        )
        if not spark.sql(month_check_sql).count():
            raise ValueError("Choose a benchmark month that contains trips")

        monthly_sql = read_sql("benchmarks/monthly_pickups.sql")
        expected = record(
            "month_date_filter",
            monthly_sql.format(month_start=month_start, partition_filter=""),
            ["pickup_location_id"],
        )

        record(
            "month_partition_filter",
            monthly_sql.format(
                month_start=month_start,
                partition_filter=(
                    f"AND pickup_year = {month_start.year} AND pickup_month = {month_start.month}"
                ),
            ),
            ["pickup_location_id"],
            expected,
        )

        # 4. Broadcast: use underlying Silver taxi + zone tables, not enriched trips.
        join_sql = read_sql("benchmarks/zone_join.sql")
        keys = QUERY_KEYS["monthly_zone_demand"]
        expected = record(
            "zone_join_no_broadcast",
            join_sql.format(hint=""),
            keys,
            answers["monthly_zone_demand"],
        )
        record(
            "zone_join_broadcast",
            join_sql.format(hint="/*+ BROADCAST(z) */"),
            keys,
            expected,
        )

        if (
            "BroadcastHashJoin"
            in report["cases"]["zone_join_no_broadcast"]["final_plan"]
        ):
            raise AssertionError("Baseline join unexpectedly used broadcast")

        if (
            "BroadcastHashJoin"
            not in report["cases"]["zone_join_broadcast"]["final_plan"]
        ):
            raise AssertionError("Broadcast hint did not produce the expected join")

        report["status"] = "success"

    except Exception as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        # Even a failed experiment leaves its completed measurements for inspection.
        (output / "metrics.json").write_text(
            json.dumps(report, indent=2, default=str, allow_nan=False), encoding="utf-8"
        )
        spark.catalog.clearCache()
        for key, value in previous.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)

    return report
