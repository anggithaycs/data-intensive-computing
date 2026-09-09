"""Post-run acceptance checks against configured Week 1 Delta outputs.

Validates Bronze & Silver Medallion layers, schemas, standardization,
data quality filters, and contextual enrichment for both pickup and dropoff.
"""

import json
import sys
import unittest
from pathlib import Path

from delta.tables import DeltaTable

# Ensure root is in sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.config import load_config
from src.common.spark_session import get_spark_session, stop_spark_session


class TestWeek1Platform(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = load_config(ROOT / "config/platform_config.yaml")
        cls.metrics = json.loads(
            (ROOT / cls.config["paths"]["metrics_path"]).read_text()
        )
        if cls.metrics["status"] != "success":
            raise ValueError("Acceptance checks require a successful platform run")
        cls.spark = get_spark_session(
            app_name="TestWeek1",
            master="local[2]",
            driver_memory="2g",
        )

    @classmethod
    def tearDownClass(cls):
        stop_spark_session(cls.spark)

    def test_bronze_layers_exist(self):
        """Verify Bronze Delta tables exist with raw fidelity and ingestion metadata."""
        bronze_tables = [
            str(ROOT / spec["bronze_path"]) for spec in self.config["datasets"].values()
        ]
        for path in bronze_tables:
            self.assertTrue(
                DeltaTable.isDeltaTable(self.spark, path),
                f"{path} is not a valid Delta table",
            )
            df = self.spark.read.format("delta").load(path)
            self.assertIn(
                "_ingestion_timestamp",
                df.columns,
                f"Missing _ingestion_timestamp in {path}",
            )
            self.assertGreater(df.count(), 0)

    def test_taxi_zones_silver_exists(self):
        """Verify clean taxi zones Delta table exists and has expected columns."""
        path = str(ROOT / self.config["datasets"]["taxi_zones"]["silver_path"])
        self.assertTrue(
            DeltaTable.isDeltaTable(self.spark, path),
            f"{path} is not a valid Delta table",
        )
        df = self.spark.read.format("delta").load(path)
        self.assertEqual(
            df.count(), self.metrics["ingestion"]["taxi_zones"]["valid_records"]
        )
        self.assertIn("location_id", df.columns)
        self.assertIn("borough", df.columns)

    def test_weather_silver_exists(self):
        """Verify clean weather Delta table exists and has canonical timestamps."""
        path = str(ROOT / self.config["datasets"]["weather"]["silver_path"])
        self.assertTrue(DeltaTable.isDeltaTable(self.spark, path))
        df = self.spark.read.format("delta").load(path)
        self.assertEqual(
            df.count(), self.metrics["ingestion"]["weather"]["valid_records"]
        )
        self.assertIn("observation_datetime", df.columns)
        self.assertIn("temperature_c", df.columns)

    def test_air_quality_silver_exists(self):
        """Verify clean air quality Delta table contains NYC records."""
        path = str(ROOT / self.config["datasets"]["air_quality"]["silver_path"])
        self.assertTrue(DeltaTable.isDeltaTable(self.spark, path))
        df = self.spark.read.format("delta").load(path)
        self.assertEqual(
            df.count(), self.metrics["ingestion"]["air_quality"]["valid_records"]
        )
        self.assertIn("sample_measurement_pm25", df.columns)
        self.assertIn("borough", df.columns)

    def test_integrated_taxi_trips_pickup_and_dropoff_schema(self):
        """Verify integrated taxi trips has both pickup AND dropoff contextual enrichment columns."""
        path = str(ROOT / self.config["integration"]["output_path"])
        self.assertTrue(DeltaTable.isDeltaTable(self.spark, path))
        df = self.spark.read.format("delta").load(path)

        expected_columns = [
            "trip_id",
            "pickup_datetime",
            "dropoff_datetime",
            "pickup_date",
            # Spatial zones
            "pickup_borough",
            "pickup_zone",
            "dropoff_borough",
            "dropoff_zone",
            # Pickup weather & air quality
            "pickup_temperature_c",
            "pickup_precipitation_mm",
            "pickup_pm25",
            "pickup_pm25_borough",
            "pickup_pm25_citywide",
            "pickup_pm25_source",
            # Dropoff weather & air quality
            "dropoff_temperature_c",
            "dropoff_precipitation_mm",
            "dropoff_pm25",
            "dropoff_pm25_borough",
            "dropoff_pm25_citywide",
            "dropoff_pm25_source",
            # Trip metrics
            "fare_amount",
            "trip_distance",
            "trip_duration_minutes",
        ]
        for col_name in expected_columns:
            self.assertIn(
                col_name, df.columns, f"Missing required integrated column: {col_name}"
            )
        self.assertEqual(
            df.count(), self.metrics["ingestion"]["taxi_trips"]["valid_records"]
        )
        self.assertTrue(self.metrics["integration"]["trip_identity_preserved"])


if __name__ == "__main__":
    unittest.main()
