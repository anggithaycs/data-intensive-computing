"""Small examples with known answers; all writes stay in .test-output."""

import sys
import unittest
from datetime import date
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pyspark.sql import Row

from src.common.spark_session import get_spark_session, stop_spark_session
from src.week2.analysis import QUERIES, QUERY_KEYS, prepare_views
from src.week2.benchmark import check_equal, measure, run_benchmarks
from src.week2.products import GOLD_QUERIES, build_products


class ComparisonTests(unittest.TestCase):
    def test_keys_nulls_counts_and_float_tolerance(self):
        expected = [Row(id=1, count=2, mean=1.0), Row(id=2, count=0, mean=None)]
        actual = [Row(id=2, count=0, mean=None), Row(id=1, count=2, mean=1.0000000001)]
        check_equal(expected, actual, ["id"])
        for wrong in (
            actual[:1],
            actual + actual[:1],
            [Row(id=1, count=3, mean=1.0)],
            [Row(id=1, count=2, mean=float("nan")), actual[0]],
        ):
            with self.assertRaises(AssertionError):
                check_equal(expected, wrong, ["id"])


class SparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark_session(
            app_name="Week2SimpleTests", master="local[2]", shuffle_partitions=2
        )
        cls.spark.conf.set("spark.databricks.delta.snapshotPartitions", "2")
        cls.output = ROOT / ".test-output" / f"week2-simple-{uuid4().hex}"
        cls.trips = cls.spark.sql("""
            WITH raw AS (
                SELECT * FROM VALUES
                (TIMESTAMP '2024-01-31 23:10:00', 1, 2.0D, 10.0D, 0.0D),
                (TIMESTAMP '2024-01-31 23:20:00', 1, 4.0D, 10.0D, 0.0D),
                (TIMESTAMP '2024-01-31 23:30:00', 2, 6.0D, 10.0D, 0.0D),
                (TIMESTAMP '2024-02-01 05:10:00', 1, 8.0D, 20.0D, 1.0D),
                (TIMESTAMP '2024-02-01 06:10:00', 2, 10.0D, CAST(NULL AS DOUBLE), 1.0D)
                AS t(pickup_datetime, pickup_location_id, trip_distance,
                     pickup_pm25_citywide, pickup_precipitation_mm)
            ), dated AS (
                SELECT *, CAST(from_utc_timestamp(pickup_datetime, 'America/New_York') AS DATE) AS pickup_date
                FROM raw
            )
            SELECT *, year(pickup_date) AS pickup_year, month(pickup_date) AS pickup_month,
                   concat('Zone ', pickup_location_id) AS pickup_zone,
                   'Manhattan' AS pickup_borough, 10.0D AS trip_duration_minutes,
                   12.0D AS fare_amount, true AS pickup_weather_matched
            FROM dated
        """)

    @classmethod
    def tearDownClass(cls):
        stop_spark_session(cls.spark)

    def setUp(self):
        prepare_views(self.spark, self.trips)

    def test_all_six_known_answers(self):
        zones = self.spark.sql(QUERIES["monthly_zone_demand"]).collect()
        self.assertEqual(
            [(r.month, r.pickup_location_id, r.trip_count) for r in zones],
            [
                (date(2024, 1, 1), 1, 2),
                (date(2024, 1, 1), 2, 1),
                (date(2024, 2, 1), 1, 1),
                (date(2024, 2, 1), 2, 1),
            ],
        )
        weather = self.spark.sql(QUERIES["weather_trip_distance"]).collect()
        self.assertEqual(
            [(r.weather, r.avg_distance) for r in weather], [("dry", 4.0), ("wet", 9.0)]
        )
        air = self.spark.sql(QUERIES["air_quality_demand"]).first()
        self.assertEqual((air.total_hours, air.matched_hours), (48, 2))
        self.assertAlmostEqual(air.correlation, -1.0)
        variation = self.spark.sql(QUERIES["weather_demand_variation"]).collect()
        self.assertEqual(
            [(r.pickup_location_id, r.demand_range) for r in variation],
            [(1, 1.5), (2, 0.5)],
        )
        peaks = (
            self.spark.sql(QUERIES["weekday_peak_hours"])
            .where("demand_rank = 1")
            .collect()
        )
        self.assertEqual(
            {(r.weekday, r.local_hour) for r in peaks}, {(3, 18), (4, 0), (4, 1)}
        )
        trends = self.spark.sql(QUERIES["monthly_demand_trends"]).collect()
        self.assertIsNone(trends[0].change_pct)
        self.assertAlmostEqual(trends[1].change_pct, -100 / 3)

    def test_gold_equality_and_repeatable_refresh(self):
        first = build_products(self.spark, self.output, "fixture")
        created = (
            self.spark.read.format("delta")
            .load(str(self.output / "_metadata"))
            .select("product", "created_at")
            .collect()
        )
        for name in QUERIES:
            check_equal(
                self.spark.sql(QUERIES[name]).collect(),
                self.spark.sql(GOLD_QUERIES[name]).collect(),
                QUERY_KEYS[name],
            )
        # In particular, an unknown hour with zero trips must not invent a Q2 row.
        second = build_products(self.spark, self.output, "fixture")
        self.assertEqual(
            {p: s["rows"] for p, s in first.items()},
            {p: s["rows"] for p, s in second.items()},
        )
        refreshed = (
            self.spark.read.format("delta")
            .load(str(self.output / "_metadata"))
            .select("product", "created_at")
            .collect()
        )
        check_equal(created, refreshed, ["product"])
        self.assertEqual(len(refreshed), 4)

    def test_dst_calendar_and_constant_correlation(self):
        for day, hours in (("2024-03-10", 23), ("2024-11-03", 25)):
            shifted = self.trips.selectExpr(
                "* EXCEPT (pickup_datetime, pickup_date, pickup_year, pickup_month)",
                f"TIMESTAMP '{day} 12:00:00' AS pickup_datetime",
                f"DATE '{day}' AS pickup_date",
                "2024 AS pickup_year",
                f"month(DATE '{day}') AS pickup_month",
            )
            prepare_views(self.spark, shifted)
            self.assertEqual(self.spark.table("calendar_hours").count(), hours)
            self.assertIsNone(
                self.spark.sql(QUERIES["air_quality_demand"]).first().correlation
            )

    def test_measure_consumes_results_and_rejects_wrong_answer(self):
        sql = QUERIES["monthly_zone_demand"]
        stats, result = measure(
            self.spark, sql, QUERY_KEYS["monthly_zone_demand"], repeats=1
        )
        self.assertEqual(len(stats["samples_ms"]), 1)
        self.assertIn("Physical Plan", stats["final_plan"])
        with self.assertRaises(AssertionError):
            measure(
                self.spark,
                sql,
                QUERY_KEYS["monthly_zone_demand"],
                repeats=1,
                expected=result[:1],
            )

    def test_full_benchmark_matrix_preserves_answers(self):
        path = self.output / "partitioned_fixture"
        self.trips.write.format("delta").partitionBy(
            "pickup_year", "pickup_month"
        ).save(str(path))
        integrated = self.spark.read.format("delta").load(str(path))
        coverage = prepare_views(self.spark, integrated)
        self.trips.createOrReplaceTempView("clean_trips")
        self.trips.selectExpr(
            "pickup_location_id AS location_id",
            "pickup_zone AS zone",
            "pickup_borough AS borough",
        ).distinct().createOrReplaceTempView("zones")
        products = build_products(self.spark, self.output / "matrix_gold", "fixture")
        previous_aqe = self.spark.conf.get("spark.sql.adaptive.enabled")
        output = self.output / "matrix"
        report = run_benchmarks(
            self.spark, output, products, coverage, repeats=1, month="2024-02"
        )
        self.assertEqual(report["status"], "success")
        self.assertEqual(len(report["cases"]), 48)
        self.assertEqual(len(list((output / "plans").rglob("*.txt"))), 48)
        for technique in (
            "silver_gold",
            "caching",
            "aqe",
            "partition_pruning",
            "broadcast_join",
        ):
            self.assertEqual(set(report["comparisons"][technique]), set(QUERIES))
            self.assertTrue(
                all(c["equivalent"] for c in report["comparisons"][technique].values())
            )
        self.assertTrue(
            all(
                c["partition_filters_present"]
                for c in report["comparisons"]["partition_pruning"].values()
            )
        )
        self.assertEqual(self.spark.table("trips").count(), 5)
        self.assertEqual(
            self.spark.conf.get("spark.sql.adaptive.enabled"), previous_aqe
        )
        self.assertEqual(self.spark.table("calendar_hours").count(), 48)
        trends = self.spark.sql(QUERIES["monthly_demand_trends"]).collect()
        self.assertAlmostEqual(trends[1].change_pct, -100 / 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
