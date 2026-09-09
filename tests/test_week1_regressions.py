"""Small deterministic fixtures; never reads or overwrites submission tables."""

import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from scripts.render_benchmark_report import render
from src.common.config import build_ingestors, load_config
from src.common.spark_session import get_spark_session, stop_spark_session
from src.ingestion.air_quality_ingestion import AirQualityIngestor
from src.ingestion.taxi_ingestion import TaxiTripsIngestor
from src.ingestion.weather_ingestion import WeatherIngestor
from src.ingestion.zone_ingestion import TaxiZoneIngestor
from src.integration.pipeline import UrbanDataIntegrationPipeline
from src.storage.benchmark import StorageBenchmarkRunner


class ConfigTests(unittest.TestCase):
    def test_config_controls_ingestors(self) -> None:
        config = load_config(ROOT / "config/platform_config.yaml")
        config = copy.deepcopy(config)
        config["datasets"]["taxi_trips"]["quality_rules"]["max_fare_amount"] = 42
        config["datasets"]["weather"]["source_path"] = "custom.csv"
        config["datasets"]["taxi_trips"]["partition_cols"] = []
        config["datasets"]["weather"]["timezone"] = "America/New_York"
        ingestors = build_ingestors(None, config)
        self.assertEqual(ingestors["taxi_trips"].quality_rules["max_fare_amount"], 42)
        self.assertEqual(ingestors["taxi_trips"].partition_cols, [])
        self.assertEqual(ingestors["weather"].input_path, "custom.csv")
        self.assertEqual(ingestors["weather"].source_timezone, "America/New_York")

    def test_benchmark_equality(self) -> None:
        StorageBenchmarkRunner.assert_results_equal(
            [("A", 1.0), ("B", 2)], [("B", 2), ("A", 1.0000000001)]
        )
        with self.assertRaises(ValueError):
            StorageBenchmarkRunner.assert_results_equal([("A", 1)], [("A", 2)])

    def test_report_rejects_partial_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            metrics = Path(directory) / "metrics.json"
            metrics.write_text(json.dumps({"status": "failed"}), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "successful run"):
                render(metrics, Path(directory) / "report.md")


class SparkRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark_session(
            app_name="Week1Regression", master="local[2]", shuffle_partitions=2
        )
        cls.root = ROOT / ".test-output" / uuid4().hex
        cls.root.mkdir(parents=True)
        cls.base_config = load_config(ROOT / "config/platform_config.yaml")

    @classmethod
    def tearDownClass(cls):
        stop_spark_session(cls.spark)

    def taxi(self, rows: list[tuple[str, str, str, str]]) -> DataFrame:
        values = ",".join(
            f"('{pk}', '{pickup}', '{dropoff}', {fare})"
            for pk, pickup, dropoff, fare in rows
        )
        return self.spark.sql(f"""SELECT id, pickup_datetime, dropoff_datetime, fare_amount,
            1 AS vendor_id, 1 AS passenger_count, 1 AS pickup_location_id,
            2 AS dropoff_location_id, 2.0 AS trip_distance, 15.0 AS total_amount,
            1.0 AS tip_amount, 0.0 AS tolls_amount
            FROM VALUES {values} AS t(id, pickup_datetime, dropoff_datetime, fare_amount)""")

    def test_null_duplicate_invalid_timestamp_and_threshold(self) -> None:
        cfg = copy.deepcopy(self.base_config["datasets"]["taxi_trips"])
        cfg["quality_rules"]["max_fare_amount"] = 20
        ingestor = TaxiTripsIngestor(
            self.spark,
            dataset_spec=cfg,
            schema_version="2.0.0",
            metadata_path=str(self.root / "audit"),
        )
        raw = self.taxi(
            [
                ("a", "2024-01-01 12:00:00", "2024-01-01 12:10:00", "10.0"),
                ("a", "2024-01-01 12:00:00", "2024-01-01 12:10:00", "10.0"),
                ("b", "2024-01-01 13:00:00", "2024-01-01 13:10:00", "NULL"),
                ("c", "bad", "2024-01-01 14:10:00", "10.0"),
                ("d", "2024-01-01 15:00:00", "2024-01-01 15:10:00", "25.0"),
            ]
        )
        valid, rejected = ingestor.apply_data_quality_rules(
            ingestor.normalize_and_transform(raw)
        )
        self.assertEqual(valid.count(), 1)
        self.assertEqual(rejected.count(), 4)
        self.assertEqual(
            rejected.filter("_rejection_reason = 'duplicate_key'").count(), 1
        )

    def test_taxi_utc_conversion_local_date_and_dst(self) -> None:
        cfg = copy.deepcopy(self.base_config["datasets"]["taxi_trips"])
        ingestor = TaxiTripsIngestor(
            self.spark,
            dataset_spec=cfg,
            schema_version="2.0.0",
            metadata_path=str(self.root / "audit"),
        )
        raw = self.taxi(
            [
                ("winter", "2024-01-01 23:30:00", "2024-01-01 23:40:00", "10.0"),
                ("spring", "2024-03-10 01:55:00", "2024-03-10 03:05:00", "10.0"),
                ("invalid", "2024-03-10 02:30:00", "2024-03-10 03:40:00", "10.0"),
            ]
        )
        normal = ingestor.normalize_and_transform(raw)
        rows = {
            r.id: r
            for r in normal.selectExpr(
                "id",
                "CAST(pickup_datetime AS STRING) AS utc",
                "CAST(pickup_date AS STRING) AS date",
                "trip_duration_minutes",
            ).collect()
        }
        self.assertEqual(rows["winter"].utc, "2024-01-02 04:30:00")
        self.assertEqual(rows["winter"].date, "2024-01-01")
        self.assertEqual(rows["spring"].trip_duration_minutes, 10.0)
        valid, rejected = ingestor.apply_data_quality_rules(normal)
        self.assertEqual(valid.count(), 2)
        self.assertEqual(rejected.count(), 1)

    def test_epa_uses_gmt_and_scope_is_separate(self) -> None:
        cfg = copy.deepcopy(self.base_config["datasets"]["air_quality"])
        ingestor = AirQualityIngestor(
            self.spark,
            dataset_spec=cfg,
            schema_version="2.0.0",
            metadata_path=str(self.root / "audit"),
        )
        raw = self.spark.sql("""SELECT 36 AS state_code, 5 AS county_code, 1 AS site_number,
            1 AS poc, 'Bronx' AS county_name, '2024-03-10' AS date_local,
            '03:00' AS time_local, '2024-03-10' AS date_gmt, '08:00' AS time_gmt,
            12.0 AS sample_measurement_pm25, 'ug/m3' AS units_of_measure,
            40.0 AS latitude, -74.0 AS longitude""")
        scoped = ingestor.filter_scope(
            raw.unionByName(raw.withColumn("state_code", F.lit(1)))
        )
        self.assertEqual(scoped.count(), 1)
        result = (
            ingestor.normalize_and_transform(scoped)
            .selectExpr("CAST(observation_datetime AS STRING) AS utc")
            .first()
        )
        self.assertEqual(result.utc, "2024-03-10 08:00:00")

    def test_schema_validation_and_quarantine_persistence(self) -> None:
        raw = self.root / "zones.csv"
        # Reordered headers plus malformed numeric key.
        raw.write_text(
            "Borough,LocationID,Zone,service_zone\nBronx,1,A,B\nBronx,1,A,B\nBronx,bad,A,B\n",
            encoding="utf-8",
        )
        cfg = copy.deepcopy(self.base_config["datasets"]["taxi_zones"])
        cfg["source_path"] = str(raw)
        cfg["silver_path"] = str(self.root / "zones")
        cfg["bronze_path"] = str(self.root / "bronze")
        cfg["quarantine_path"] = str(self.root / "quarantine")

        ingestor = TaxiZoneIngestor(
            self.spark,
            dataset_spec=cfg,
            schema_version="2.0.0",
            metadata_path=str(self.root / "audit"),
        )
        metrics = ingestor.run()
        self.assertEqual(metrics["initial_records"], 3)
        self.assertEqual(metrics["valid_records"], 1)
        self.assertEqual(metrics["rejected_records"], 2)
        self.assertEqual(metrics["duplicate_records"], 1)
        rejected = self.spark.read.format("delta").load(str(self.root / "quarantine"))
        self.assertEqual(rejected.count(), 2)
        self.assertEqual(rejected.select("_run_id").first()[0], metrics["run_id"])
        bad = self.root / "missing.csv"
        bad.write_text("LocationID\n1\n", encoding="utf-8")
        bad_cfg = copy.deepcopy(cfg)
        bad_cfg["source_path"] = str(bad)
        with self.assertRaisesRegex(ValueError, "missing required columns"):
            TaxiZoneIngestor(
                self.spark,
                dataset_spec=bad_cfg,
                schema_version="2.0.0",
                metadata_path=str(self.root / "audit"),
            ).load_raw_data()

    def context(self) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
        taxi = self.spark.sql("""SELECT * FROM VALUES
            ('a', timestamp'2024-01-01 12:30:00', timestamp'2024-01-01 12:45:00', 1, 2),
            ('b', timestamp'2024-01-02 12:30:00', timestamp'2024-01-02 12:45:00', 2, 1)
            AS t(trip_id, pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id)""")
        zones = self.spark.sql(
            "SELECT * FROM VALUES (1, 'Bronx', 'A', 'B'), (2, 'Queens', 'C', 'D') "
            "AS t(location_id, borough, zone, service_zone)"
        )
        weather = self.spark.sql("""SELECT timestamp'2024-01-01 12:00:00' AS observation_datetime,
            10.0 AS temperature_c, 50.0 AS relative_humidity_pct, CAST(NULL AS DOUBLE) AS precipitation_mm,
            1.0 AS wind_speed_kmh, 1000.0 AS air_pressure_hpa, 3 AS cloud_cover_oktas, 1 AS weather_condition_code""")
        air = self.spark.sql(
            "SELECT timestamp'2024-01-01 12:00:00' AS observation_datetime, "
            "'Bronx' AS borough, 36 AS state_code, 5 AS county_code, 1 AS site_number, 12.0 AS sample_measurement_pm25"
        )
        return taxi, weather, air, zones

    def test_missing_context_and_identity(self) -> None:
        taxi, weather, air, zones = self.context()
        pipeline = UrbanDataIntegrationPipeline(
            self.spark,
            taxi_delta_path=str(self.root / "taxi"),
            weather_delta_path=str(self.root / "weather"),
            air_quality_delta_path=str(self.root / "air"),
            zones_delta_path=str(self.root / "zones"),
            output_delta_path=str(self.root / "out"),
        )
        integrated = pipeline.build_integrated_dataset(
            taxi, weather, air, zones
        ).cache()
        try:
            rows = {r.trip_id: r for r in integrated.collect()}
            self.assertTrue(rows["a"].pickup_weather_matched)
            self.assertTrue(rows["a"].pickup_precipitation_missing)
            self.assertIsNone(rows["a"].pickup_precipitation_mm)
            self.assertEqual(rows["a"].pickup_pm25_source, "borough")
            self.assertEqual(rows["a"].dropoff_pm25_source, "citywide")
            self.assertEqual(rows["a"].pickup_pm25_borough, 12.0)
            self.assertEqual(rows["a"].pickup_pm25_citywide, 12.0)
            self.assertIsNone(rows["a"].dropoff_pm25_borough)
            self.assertEqual(rows["a"].dropoff_pm25_citywide, 12.0)
            self.assertIsNone(rows["b"].pickup_pm25)
            self.assertIsNone(rows["b"].pickup_pm25_borough)
            self.assertIsNone(rows["b"].pickup_pm25_citywide)
            self.assertEqual(rows["b"].pickup_pm25_source, "missing")
            self.assertFalse(rows["b"].pickup_weather_matched)
            self.assertEqual(pipeline.assert_trip_identity(taxi, integrated), 2)
        finally:
            integrated.unpersist()

    def test_separate_air_estimates_across_boroughs(self) -> None:
        _, weather, _, zones = self.context()
        taxi = self.spark.sql("""SELECT trip_id, timestamp'2024-01-01 12:30:00' pickup_datetime,
            timestamp'2024-01-01 12:45:00' dropoff_datetime, location_id pickup_location_id,
            location_id dropoff_location_id FROM VALUES ('a', 1), ('b', 2), ('c', 3)
            AS t(trip_id, location_id)""")
        zones = zones.unionByName(
            self.spark.sql(
                "SELECT 3 location_id, 'Manhattan' borough, 'E' zone, 'F' service_zone"
            )
        )
        air = self.spark.sql("""SELECT timestamp'2024-01-01 12:00:00' observation_datetime,
            * FROM VALUES ('Bronx', 36, 5, 1, 1, 10.0), ('Bronx', 36, 5, 1, 2, 10.0),
            ('Queens', 36, 81, 1, 1, 30.0)
            AS t(borough, state_code, county_code, site_number, poc, sample_measurement_pm25)""")
        pipeline = UrbanDataIntegrationPipeline(
            self.spark,
            taxi_delta_path=str(self.root / "taxi"),
            weather_delta_path=str(self.root / "weather"),
            air_quality_delta_path=str(self.root / "air"),
            zones_delta_path=str(self.root / "zones"),
            output_delta_path=str(self.root / "out"),
        )
        rows = {
            r.trip_id: r
            for r in pipeline.build_integrated_dataset(
                taxi, weather, air, zones
            ).collect()
        }
        self.assertEqual(set(rows), {"a", "b", "c"})
        for endpoint in ("pickup", "dropoff"):
            self.assertEqual(rows["a"][endpoint + "_pm25_borough"], 10.0)
            self.assertEqual(rows["b"][endpoint + "_pm25_borough"], 30.0)
            self.assertIsNone(rows["c"][endpoint + "_pm25_borough"])
            self.assertEqual(
                {r[endpoint + "_pm25_citywide"] for r in rows.values()}, {20.0}
            )
            self.assertEqual(rows["a"][endpoint + "_pm25"], 10.0)
            self.assertEqual(rows["c"][endpoint + "_pm25"], 20.0)
            self.assertEqual(rows["c"][endpoint + "_pm25_source"], "citywide")

    def test_air_quality_site_equal_weighting(self) -> None:
        pipeline = UrbanDataIntegrationPipeline(
            self.spark,
            taxi_delta_path=str(self.root / "taxi"),
            weather_delta_path=str(self.root / "weather"),
            air_quality_delta_path=str(self.root / "air"),
            zones_delta_path=str(self.root / "zones"),
            output_delta_path=str(self.root / "out"),
        )
        air = self.spark.sql("""SELECT timestamp'2024-01-01 12:00:00' AS observation_datetime, * FROM VALUES
            ('Bronx', 36, 5, 1, 1, 10.0),
            ('Bronx', 36, 5, 1, 2, 10.0),
            ('Bronx', 36, 5, 2, 1, 20.0),
            ('Manhattan', 36, 61, 1, 1, 30.0)
            AS t(borough, state_code, county_code, site_number, poc, sample_measurement_pm25)""")
        borough_aq, citywide_aq = pipeline.prepare_air_quality_aggregates(air)
        b_rows = {r.borough: r.pm25_borough_level for r in borough_aq.collect()}
        c_row = citywide_aq.first()
        self.assertEqual(b_rows["Bronx"], 15.0)
        self.assertEqual(b_rows["Manhattan"], 30.0)
        self.assertEqual(c_row.pm25_citywide_level, 20.0)

    def test_identity_rejects_loss_fanout_and_replacement(self) -> None:
        taxi, *_ = self.context()
        pipeline = UrbanDataIntegrationPipeline(
            self.spark,
            taxi_delta_path=str(self.root / "taxi"),
            weather_delta_path=str(self.root / "weather"),
            air_quality_delta_path=str(self.root / "air"),
            zones_delta_path=str(self.root / "zones"),
            output_delta_path=str(self.root / "out"),
        )
        for broken in (
            taxi.limit(1),
            taxi.unionByName(taxi),
            taxi.withColumn("trip_id", F.concat(F.lit("new"), F.col("trip_id"))),
        ):
            with self.assertRaises(ValueError):
                pipeline.assert_trip_identity(taxi, broken)

    def test_weather_preserves_unknown_precipitation(self) -> None:
        raw = self.spark.sql("""SELECT 2024 AS year, 1 AS month, 1 AS day, 12 AS hour,
            10.0 AS temperature_c, 50.0 AS relative_humidity_pct, CAST(NULL AS DOUBLE) AS precipitation_mm,
            1.0 AS wind_speed_kmh, 1000.0 AS air_pressure_hpa, 3 AS cloud_cover_oktas, 1 AS weather_condition_code""")
        cfg = copy.deepcopy(self.base_config["datasets"]["weather"])
        cfg["timezone"] = "America/New_York"
        row = (
            WeatherIngestor(
                self.spark,
                dataset_spec=cfg,
                schema_version="2.0.0",
                metadata_path=str(self.root / "audit"),
            )
            .normalize_and_transform(raw)
            .selectExpr(
                "CAST(observation_datetime AS STRING) AS utc", "precipitation_mm"
            )
            .first()
        )
        self.assertEqual(row.utc, "2024-01-01 17:00:00")
        self.assertIsNone(row.precipitation_mm)

    def test_configured_rules_casts_composite_keys_and_delta_audit(self) -> None:
        config = copy.deepcopy(load_config(ROOT / "config/platform_config.yaml"))
        spec = config["datasets"]["taxi_zones"]
        spec["key"] = ["location_id", "zone"]
        spec["casts"]["location_id"] = "bigint"
        spec["validation"]["allowed_service"] = "service_zone = 'B'"
        raw = self.root / "configured_zones.csv"
        raw.write_text(
            "LocationID,Borough,Zone,service_zone\n"
            "1,Bronx,A,B\n1,Bronx,C,B\n1,Bronx,A,B\n2,Bronx,D,X\nbad,Bronx,E,B\n3,Bronx,F,\n",
            encoding="utf-8",
        )
        spec["source_path"] = str(raw)
        spec["silver_path"] = str(self.root / "configured_silver")
        spec["bronze_path"] = None
        spec["quarantine_path"] = str(self.root / "configured_quarantine")
        ingestor = TaxiZoneIngestor(
            self.spark,
            dataset_spec=spec,
            schema_version="2.0.0",
            metadata_path=str(self.root / "configured_audit"),
        )
        for _ in range(2):
            metrics = ingestor.run()
            self.assertEqual(
                (metrics["valid_records"], metrics["rejected_records"]), (2, 4)
            )
        audit = self.spark.read.format("delta").load(
            str(self.root / "configured_audit")
        )
        self.assertEqual(audit.count(), 2)
        self.assertEqual(audit.select("run_id").distinct().count(), 2)
        silver = self.spark.read.format("delta").load(ingestor.delta_output_path)
        self.assertEqual(silver.schema["location_id"].dataType.simpleString(), "bigint")
        rejected = self.spark.read.format("delta").load(ingestor.quarantine_path)
        self.assertGreater(
            rejected.filter(
                "_rejection_reason LIKE '%invalid_type:location_id%'"
            ).count(),
            0,
        )
        self.assertGreater(
            rejected.filter("_rejection_reason LIKE '%allowed_service%'").count(), 0
        )

    def test_raw_taxi_benchmark_with_zone_join(self) -> None:
        root = self.root / "benchmark"
        raw = self.taxi(
            [
                ("a", "2024-02-14 12:00:00", "2024-02-14 12:10:00", "10.0"),
                ("a", "2024-02-14 12:00:00", "2024-02-14 12:10:00", "10.0"),
                ("bad", "2024-02-14 13:00:00", "2024-02-14 13:10:00", "-1.0"),
                ("b", "2024-03-10 01:55:00", "2024-03-10 03:05:00", "20.0"),
            ]
        )
        mapping = load_config(ROOT / "config/platform_config.yaml")["datasets"][
            "taxi_trips"
        ]["rename"]
        reverse = {v: k for k, v in mapping.items()}
        raw.toDF(*[reverse.get(c, c) for c in raw.columns]).write.parquet(
            str(root / "raw")
        )
        self.spark.sql("SELECT 1 location_id, 'Bronx' borough").write.format(
            "delta"
        ).save(str(root / "zones"))
        taxi_cfg = copy.deepcopy(self.base_config["datasets"]["taxi_trips"])
        taxi_cfg["source_path"] = str(root / "raw")
        ingestor = TaxiTripsIngestor(
            self.spark,
            dataset_spec=taxi_cfg,
            schema_version="2.0.0",
            metadata_path=str(self.root / "audit"),
        )
        runner = StorageBenchmarkRunner(
            self.spark,
            taxi_ingestor=ingestor,
            zones_delta_path=str(root / "zones"),
            writer_partitions=2,
            strategy_a_path=str(root / "a"),
            strategy_b_path=str(root / "b"),
            strategy_c_path=str(root / "c"),
        )
        result = runner.run_benchmark(iterations=1)
        self.assertTrue(result["results_equivalent"])
        self.assertEqual(result["total_records"], 2)
        for layout in result["storage_metrics"].values():
            self.assertEqual(layout["source_records"], 4)
            self.assertEqual(layout["rejected_records"], 2)
            df = self.spark.read.format("delta").load(layout["path"])
            self.assertNotIn("pickup_borough", df.columns)
            self.assertEqual(df.filter("trip_duration_minutes = 10").count(), 2)
        for stats in result["query_statistics"].values():
            self.assertIn("Join", stats["Q1_Trips_Per_Borough"]["physical_plan"])

    def test_cast_failures_survive_defaults_and_duplicates_are_deterministic(self):
        cfg = copy.deepcopy(self.base_config["datasets"]["taxi_trips"])
        ingestor = TaxiTripsIngestor(self.spark, cfg, "2.0.0", str(self.root / "audit"))
        raw = self.taxi([("a", "2024-01-01 12:00:00", "2024-01-01 12:10:00", "10.0")])
        malformed = raw.withColumn("passenger_count", F.lit("bad"))
        valid, rejected = ingestor.apply_data_quality_rules(ingestor.prepare(malformed))
        self.assertEqual(valid.count(), 0)
        row = rejected.first()
        self.assertEqual(row.passenger_count, 1)
        self.assertIn("invalid_type:passenger_count", row._rejection_reason)
        conflicting = raw.unionByName(raw.withColumn("tip_amount", F.lit(9.0)))
        first, rejected = ingestor.apply_data_quality_rules(
            ingestor.prepare(conflicting)
        )
        second, _ = ingestor.apply_data_quality_rules(
            ingestor.prepare(conflicting.orderBy(F.desc("tip_amount")))
        )
        self.assertEqual(
            first.select("trip_id", "tip_amount").collect(),
            second.select("trip_id", "tip_amount").collect(),
        )
        self.assertEqual(
            rejected.filter("_rejection_reason = 'duplicate_key'").count(), 1
        )
        for field in ("tip_amount", "tolls_amount"):
            for value in (-1.0, float("inf"), float("nan")):
                valid, rejected = ingestor.apply_data_quality_rules(
                    ingestor.prepare(raw.withColumn(field, F.lit(value)))
                )
                self.assertEqual(rejected.count(), 1)

    def test_weather_numeric_domains_and_missing_values(self):
        cfg = copy.deepcopy(self.base_config["datasets"]["weather"])
        ingestor = WeatherIngestor(self.spark, cfg, "2.0.0", str(self.root / "audit"))
        _, raw, _, _ = self.context()
        raw = (
            raw.withColumn("year", F.lit(2024))
            .withColumn("month", F.lit(1))
            .withColumn("day", F.lit(1))
            .withColumn("hour", F.lit(12))
            .withColumn("wind_direction_deg", F.lit(0))
        )
        valid, rejected = ingestor.apply_data_quality_rules(ingestor.prepare(raw))
        self.assertEqual(valid.count(), 1)
        self.assertEqual(rejected.count(), 0)
        cases = [
            ("precipitation_mm", -1.0),
            ("precipitation_mm", float("nan")),
            ("air_pressure_hpa", float("inf")),
            ("cloud_cover_oktas", 9),
            ("weather_condition_code", 28),
        ]
        invalid = None
        for field, value in cases:
            row = raw.withColumn(field, F.lit(value))
            invalid = row if invalid is None else invalid.unionByName(row)
        _, rejected = ingestor.apply_data_quality_rules(ingestor.prepare(invalid))
        self.assertEqual(rejected.count(), len(cases))

    def test_output_and_site_schema_fail_explicitly(self):
        cfg = copy.deepcopy(self.base_config["datasets"]["taxi_zones"])
        cfg["selected_columns"].append("missing_column")
        ingestor = TaxiZoneIngestor(self.spark, cfg, "2.0.0", str(self.root / "audit"))
        _, _, air, zones = self.context()
        with self.assertRaises(AnalysisException):
            ingestor.apply_data_quality_rules(ingestor.prepare(zones))
        pipeline = UrbanDataIntegrationPipeline.from_config(
            self.spark, self.base_config
        )
        with self.assertRaisesRegex(ValueError, "complete site identifiers"):
            pipeline.prepare_air_quality_aggregates(air.drop("site_number"))
        cfg["selected_columns"].remove("missing_column")
        ingestor = TaxiZoneIngestor(self.spark, cfg, "2.0.0", str(self.root / "audit"))
        _, rejected = ingestor.apply_data_quality_rules(
            ingestor.prepare(zones.withColumn("borough", F.lit("  ")))
        )
        self.assertEqual(rejected.count(), zones.count())


if __name__ == "__main__":
    unittest.main(verbosity=2)
