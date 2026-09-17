import json
import math
import statistics
import time
from pathlib import Path

from src.week2.analysis import QUERIES, QUERY_KEYS, prepare_derived_views
from src.week2.products import GOLD_QUERIES
from src.week2.sql_files import read_sql


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
    """Compare Silver/Gold and test each optimization on all six queries.

    Silver must stay unchanged and Gold must have just been rebuilt. Timed
    queries include full result collection; setup and equality checks do not.
    """
    from datetime import date, timedelta

    month_start = date.fromisoformat(month + "-01")
    next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
    prior_month = (month_start - timedelta(days=1)).replace(day=1)
    if repeats < 1:
        raise ValueError("repeats must be positive")

    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    original = spark.table("trips")
    original.createOrReplaceTempView("benchmark_original_trips")
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
        "comparisons": {},
        "spark_version": spark.version,
        "master": spark.sparkContext.master,
        "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
        "baseline_settings": settings,
    }

    def record(name, sql, keys, expected=None, input_sql=None):
        print(f"Benchmark: {name}", flush=True)
        stats, rows = measure(spark, sql, keys, repeats, expected)
        if input_sql is not None:
            stats["trips_view_sql"] = input_sql
        report["cases"][name] = stats
        return rows

    def compare(technique, name, before, after, scope):
        baseline = report["cases"][before]
        optimized = report["cases"][after]
        comparison = {
            "before_case": before,
            "after_case": after,
            "before_ms": baseline["median_ms"],
            "after_ms": optimized["median_ms"],
            "speedup": baseline["median_ms"] / optimized["median_ms"],
            "equivalent": True,
            "scope": scope,
        }
        if technique != "silver_gold":
            directory = output / "plans" / technique
            directory.mkdir(parents=True, exist_ok=True)
            for side, stats in (("before", baseline), ("after", optimized)):
                path = directory / f"{name}_{side}.txt"
                path.write_text(stats["final_plan"], encoding="utf-8")
                comparison[f"{side}_plan"] = path.relative_to(output).as_posix()
        report["comparisons"].setdefault(technique, {})[name] = comparison
        return comparison

    def use_input(sql, date_coverage):
        spark.sql(sql).createOrReplaceTempView("trips")
        prepare_derived_views(spark, date_coverage)

    try:
        for key, value in settings.items():
            spark.conf.set(key, value)
        spark.catalog.clearCache()
        answers = {}
        for name, sql in QUERIES.items():
            answers[name] = record(name + "_silver", sql, QUERY_KEYS[name])
        for name, sql in GOLD_QUERIES.items():
            record(name + "_gold", sql, QUERY_KEYS[name], answers[name])
            compare(
                "silver_gold", name, name + "_silver", name + "_gold", "Full period"
            )

        # Populate once; all six queries reuse the same cached trip projection.
        start = time.perf_counter()
        spark.sql("CACHE TABLE trips OPTIONS ('storageLevel' 'MEMORY_AND_DISK')")
        spark.table("trips").count()
        report["cache_population_seconds"] = time.perf_counter() - start
        for name, sql in QUERIES.items():
            record(name + "_cached", sql, QUERY_KEYS[name], answers[name])
            compare("caching", name, name + "_silver", name + "_cached", "Full period")
            plan = report["cases"][name + "_cached"]["final_plan"]
            if "Scan In-memory table" not in plan and "InMemoryTableScan" not in plan:
                raise AssertionError(f"{name}: caching did not use a cached scan")
        spark.catalog.clearCache()

        # AQE may coalesce shuffles; automatic broadcasting stays disabled.
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        for name, sql in QUERIES.items():
            record(name + "_aqe", sql, QUERY_KEYS[name], answers[name])
            compare("aqe", name, name + "_silver", name + "_aqe", "Full period")
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")

        # Both sides read the same dates. Trends also need the previous month.
        if (
            not original.where(
                f"pickup_date >= DATE '{month_start}' AND pickup_date < DATE '{next_month}'"
            )
            .limit(1)
            .count()
        ):
            raise ValueError("Choose a benchmark month that contains trips")
        scoped_sql = read_sql("benchmarks/scoped_trips.sql")
        for name, sql in QUERIES.items():
            start_date = prior_month if name == "monthly_demand_trends" else month_start
            months = (
                [prior_month, month_start]
                if start_date == prior_month
                else [month_start]
            )
            predicate = " OR ".join(
                f"(pickup_year = {d.year} AND pickup_month = {d.month})" for d in months
            )
            before_sql = scoped_sql.format(
                start=start_date, end=next_month, partition_filter=""
            )
            spark.sql(before_sql).createOrReplaceTempView("trips")
            scoped_coverage = spark.sql(read_sql("views/coverage.sql")).first().asDict()
            prepare_derived_views(spark, scoped_coverage)
            before = name + "_date_filter"
            expected = record(before, sql, QUERY_KEYS[name], input_sql=before_sql)
            after_sql = scoped_sql.format(
                start=start_date, end=next_month, partition_filter=f"AND ({predicate})"
            )
            use_input(after_sql, scoped_coverage)
            after = name + "_partition_filter"
            record(after, sql, QUERY_KEYS[name], expected, after_sql)
            comparison = compare(
                "partition_pruning",
                name,
                before,
                after,
                f"{start_date} inclusive to {next_month} exclusive; "
                "calendar uses observed dates within this window",
            )
            # A plan can apply pruning without producing a speedup (e.g. file skipping).
            comparison["partition_filters_present"] = any(
                "PartitionFilters:" in line
                and "pickup_year" in line
                and "pickup_month" in line
                for line in report["cases"][after]["final_plan"].splitlines()
            )
            if not comparison["partition_filters_present"]:
                raise AssertionError(
                    f"{name}: year/month partition filters are missing"
                )

        # Reconstruct the integrated input with small zone and hourly lookups.
        # The shared lookup preparation cost is separate from both join timings.
        original.createOrReplaceTempView("trips")
        prepare_derived_views(spark, coverage)
        context_sql = read_sql("benchmarks/hourly_context.sql")
        spark.sql(context_sql).createOrReplaceTempView("benchmark_hourly_context")
        start = time.perf_counter()
        spark.sql("CACHE TABLE benchmark_hourly_context")
        spark.table("benchmark_hourly_context").count()
        report["broadcast_lookup_population_seconds"] = time.perf_counter() - start
        report["broadcast_context_sql"] = context_sql
        report["broadcast_scope"] = (
            "Full period, clean trips joined to zones and an hourly weather/PM2.5 "
            "lookup derived from integrated trips. Both sides reuse the materialized "
            "lookup. Each answer is also checked against integrated Silver."
        )
        join_sql = read_sql("benchmarks/trips_from_sources.sql")
        for suffix, hint in (
            ("no_broadcast", ""),
            ("broadcast", "/*+ BROADCAST(z, c) */"),
        ):
            input_sql = join_sql.format(hint=hint)
            use_input(input_sql, coverage)
            for name, sql in QUERIES.items():
                case = name + "_" + suffix
                record(case, sql, QUERY_KEYS[name], answers[name], input_sql)
                plan = report["cases"][case]["final_plan"]
                if ("BroadcastHashJoin" in plan) != bool(hint):
                    raise AssertionError(f"{case}: unexpected broadcast join strategy")
                if hint:
                    compare(
                        "broadcast_join",
                        name,
                        name + "_no_broadcast",
                        case,
                        report["broadcast_scope"],
                    )
        report["status"] = "success"
    except Exception as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        # Keep completed evidence on failure and restore the analytical session.
        try:
            (output / "metrics.json").write_text(
                json.dumps(report, indent=2, default=str, allow_nan=False),
                encoding="utf-8",
            )
        finally:
            spark.catalog.clearCache()
            original.createOrReplaceTempView("trips")
            prepare_derived_views(spark, coverage)
            for view in ("benchmark_original_trips", "benchmark_hourly_context"):
                spark.catalog.dropTempView(view)
            for key, value in previous.items():
                if value is None:
                    spark.conf.unset(key)
                else:
                    spark.conf.set(key, value)
    return report
