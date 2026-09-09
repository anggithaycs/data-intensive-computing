"""Storage Strategy Benchmarking Engine for Taxi Trips.

Compares 3 distinct storage architectures:
- Strategy A: Unpartitioned Delta Table (Flat single directory)
- Strategy B: Date Partitioned Delta Table (partitionBy pickup_date)
- Strategy C: Monthly Partitioned Delta Table (partitionBy pickup_year, pickup_month)
Measures write runtime, on-disk storage size, file counts, and analytical query latencies.
"""

import math
import os
import statistics
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

from pyspark import StorageLevel
from pyspark.sql import SparkSession

from src.common.logger import get_logger
from src.ingestion.taxi_ingestion import TaxiTripsIngestor

logger = get_logger("StorageBenchmark")


class StorageBenchmarkRunner:
    """Executes empirical benchmarking across 3 distinct storage architectures."""

    def __init__(
        self,
        spark: SparkSession,
        taxi_ingestor: TaxiTripsIngestor,
        zones_delta_path: str = "storage/delta/silver/clean_taxi_zones",
        writer_partitions: int = 8,
        strategy_a_path: str = "storage/delta/benchmark/strategy_a_unpartitioned",
        strategy_b_path: str = "storage/delta/benchmark/strategy_b_partitioned",
        strategy_c_path: str = "storage/delta/benchmark/strategy_c_monthly_partitioned",
    ) -> None:
        self.spark = spark
        self.taxi_ingestor = taxi_ingestor
        self.zones_delta_path = zones_delta_path
        self.writer_partitions = writer_partitions
        if writer_partitions < 1:
            raise ValueError("writer_partitions must be positive")
        # Fresh run directories keep sizes free of obsolete Delta files and
        # preserve previous measurements without deleting configured paths.
        self.run_id = uuid4().hex
        self.strategy_a_path = os.path.join(strategy_a_path, self.run_id)
        self.strategy_b_path = os.path.join(strategy_b_path, self.run_id)
        self.strategy_c_path = os.path.join(strategy_c_path, self.run_id)
        self.logger = logger
        self.benchmark_results: dict[str, Any] = {}
        self.strategies = [
            ("Strategy A (Unpartitioned)", self.strategy_a_path, []),
            ("Strategy B (Date Partitioned)", self.strategy_b_path, ["pickup_date"]),
            ("Strategy C (Monthly Partitioned)", self.strategy_c_path, ["pickup_year", "pickup_month"]),
        ]

    def prepare_storage_strategies(self) -> dict[str, dict[str, Any]]:
        """Independent raw-to-clean ingestion for every fresh taxi layout.

        Shared Bronze, quarantine and audit writes are excluded. No integrated
        input or materialization is reused between layout timers.
        """
        metrics = {}
        for name, path, partitions in self.strategies:
            self.spark.catalog.clearCache()
            start = time.perf_counter()
            ingestor = self.taxi_ingestor
            raw = ingestor.load_raw_data()
            normalized = ingestor.prepare(raw).persist(StorageLevel.DISK_ONLY)
            try:
                source_count = normalized.count()
                accepted, _ = ingestor.apply_data_quality_rules(normalized)
                writer = (
                    accepted.repartition(self.writer_partitions)
                    .write.format("delta")
                    .mode("errorifexists")
                )
                if partitions:
                    writer = writer.partitionBy(*partitions)
                writer.save(path)
                elapsed = time.perf_counter() - start
            finally:
                normalized.unpersist(blocking=True)

            accepted_count = self.spark.read.format("delta").load(path).count()
            files = [f for f in Path(path).rglob("*") if f.is_file()]
            size = sum(f.stat().st_size for f in files)
            metrics[name] = {
                "ingestion_time_sec": round(elapsed, 3),
                "source_records": source_count,
                "valid_records": accepted_count,
                "rejected_records": source_count - accepted_count,
                "storage_size_bytes": size,
                "storage_size_mb": round(size / 1024**2, 2),
                "data_size_bytes": sum(
                    f.stat().st_size for f in files if f.suffix == ".parquet"
                ),
                "num_parquet_files": sum(f.suffix == ".parquet" for f in files),
                "num_generated_files": len(files),
                "path": path,
            }
        if len({m["valid_records"] for m in metrics.values()}) != 1:
            raise ValueError("Layouts contain different accepted row counts")
        return metrics

    def measure_query(
        self, table_path: str, query_sql: str, iterations: int = 3
    ) -> tuple[dict[str, Any], list[Any]]:
        """Executes a SQL query against a Delta table and returns execution metrics and results."""
        df = self.spark.read.format("delta").load(table_path)
        view_name = f"benchmark_tbl_{self.run_id}"
        df.createOrReplaceTempView(view_name)
        sql = query_sql.replace("{{TABLE}}", view_name)

        try:
            # Warmup run
            result = self.spark.sql(sql).collect()

            latencies = []
            for _ in range(iterations):
                self.spark.catalog.clearCache()
                start = time.perf_counter()
                self.spark.sql(sql).collect()
                latencies.append((time.perf_counter() - start) * 1000.0)

            plan = self.spark.sql(f"EXPLAIN FORMATTED {sql}").first()[0]
            return {
                "mean_ms": round(statistics.mean(latencies), 2),
                "median_ms": round(statistics.median(latencies), 2),
                "stdev_ms": round(statistics.stdev(latencies), 2)
                if len(latencies) > 1
                else 0.0,
                "samples_ms": latencies,
                "physical_plan": plan,
            }, result
        finally:
            self.spark.catalog.dropTempView(view_name)

    @staticmethod
    def assert_results_equal(expected: list[Any], actual: list[Any]) -> None:
        """Verifies that two query result sets are equivalent regardless of row order."""
        left = sorted([tuple(row) for row in expected], key=lambda row: str(row[0]))
        right = sorted([tuple(row) for row in actual], key=lambda row: str(row[0]))
        if len(left) != len(right):
            raise ValueError("Benchmark query row counts differ")
        for a, b in zip(left, right, strict=True):
            if len(a) != len(b):
                raise ValueError("Benchmark query schemas differ")
            for x, y in zip(a, b, strict=True):
                equal = (
                    math.isclose(x, y, rel_tol=1e-9, abs_tol=1e-8)
                    if isinstance(x, float) and isinstance(y, float)
                    else x == y
                )
                if not equal:
                    raise ValueError(f"Benchmark results differ: {a} != {b}")

    def run_benchmark(self, iterations: int = 3) -> dict[str, Any]:
        """Runs the complete benchmark suite on all 3 storage architectures."""
        self.logger.info(
            "=== Running Week 1 Storage Architecture Benchmark (3 Strategies) ==="
        )
        if iterations < 1:
            raise ValueError("iterations must be positive")

        zones = self.spark.read.format("delta").load(self.zones_delta_path)
        if (
            zones.filter("location_id IS NULL").count()
            or zones.select("location_id").distinct().count() != zones.count()
        ):
            raise ValueError("Benchmark zones must have unique non-null location IDs")
        zones_view = f"benchmark_zones_{self.run_id}"
        zones.createOrReplaceTempView(zones_view)
        try:
            storage_metrics = self.prepare_storage_strategies()
            total_records = next(iter(storage_metrics.values()))["valid_records"]

            # Benchmark queries
            queries = {
                "Q1_Trips_Per_Borough": (
                    "SELECT coalesce(z.borough, 'Unknown') AS pickup_borough, COUNT(*) AS trip_count "
                    "FROM {{TABLE}} t LEFT JOIN benchmark_zones z ON t.pickup_location_id = z.location_id "
                    "GROUP BY coalesce(z.borough, 'Unknown')"
                ),
                "Q2_Avg_Duration_Per_Day": (
                    "SELECT pickup_date, AVG(trip_duration_minutes) AS avg_duration "
                    "FROM {{TABLE}} GROUP BY pickup_date ORDER BY pickup_date"
                ),
                "Q3_Avg_Fare_Per_Borough": (
                    "SELECT coalesce(z.borough, 'Unknown') AS pickup_borough, AVG(t.fare_amount) AS avg_fare "
                    "FROM {{TABLE}} t LEFT JOIN benchmark_zones z ON t.pickup_location_id = z.location_id "
                    "GROUP BY coalesce(z.borough, 'Unknown')"
                ),
                "Q4_Single_Day_Pruned_Analysis": (
                    "SELECT coalesce(z.borough, 'Unknown') AS pickup_borough, COUNT(*), AVG(t.fare_amount) "
                    "FROM {{TABLE}} t LEFT JOIN benchmark_zones z ON t.pickup_location_id = z.location_id "
                    "WHERE pickup_date = '2024-02-14' GROUP BY coalesce(z.borough, 'Unknown')"
                ),
                "Q5_Month_Pruned_Analysis": (
                    "SELECT coalesce(z.borough, 'Unknown') AS pickup_borough, COUNT(*), AVG(t.fare_amount) "
                    "FROM {{TABLE}} t LEFT JOIN benchmark_zones z ON t.pickup_location_id = z.location_id "
                    "WHERE pickup_year = 2024 AND pickup_month = 2 GROUP BY coalesce(z.borough, 'Unknown')"
                ),
            }

            paths = {name: path for name, path, _ in self.strategies}

            query_statistics = {name: {} for name in paths}
            for index, (q_name, sql) in enumerate(queries.items()):
                self.logger.info(f"Measuring latency for {q_name} across strategies...")
                order = list(paths)
                offset = index % len(order)
                order = order[offset:] + order[:offset]
                expected = None
                for strat_name in order:
                    stats, result = self.measure_query(
                        paths[strat_name], sql.replace("benchmark_zones", zones_view), iterations=iterations
                    )
                    if expected is None:
                        expected = result
                    else:
                        self.assert_results_equal(expected, result)
                    query_statistics[strat_name][q_name] = stats

            self.benchmark_results = {
                "total_records": total_records,
                "storage_metrics": storage_metrics,
                "query_statistics": query_statistics,
                "results_equivalent": True,
                "run_id": self.run_id,
                "input_kind": "raw_taxi_parquet",
                "source_path": self.taxi_ingestor.input_path,
                "writer_partitions": self.writer_partitions,
                "methodology": (
                    "Raw Parquet loading, schema checks, casts, transformations, disk materialization, "
                    "validation, deduplication, equal writer redistribution and Delta write timed separately "
                    "for each layout. Shared Bronze, quarantine and metadata writes excluded. One warmup; "
                    "Spark cache cleared before measured queries, OS cache remains warm. Strategy order rotated "
                    "per query; ingestion order fixed with one trial per layout."
                ),
            }
            return self.benchmark_results
        finally:
            self.spark.catalog.dropTempView(zones_view)
