"""Taxi Trips Ingestion Implementation.

Processes NYC Yellow Taxi Parquet data, standardizes timestamps,
computes trip duration, generates unique trip hashes, and enforces quality filters.
"""

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.ingestion.base_ingestion import BaseDatasetIngestor


class TaxiTripsIngestor(BaseDatasetIngestor):
    """Specialized Ingestor for NYC Yellow Taxi Trips."""

    def __init__(
        self,
        spark: SparkSession,
        dataset_spec: dict[str, Any],
        schema_version: str,
        metadata_path: str,
    ) -> None:
        super().__init__(
            spark=spark,
            dataset_name="TaxiTrips",
            dataset_spec=dataset_spec,
            schema_version=schema_version,
            metadata_path=metadata_path,
        )

    def normalize_and_transform(self, df: DataFrame) -> DataFrame:
        """Standardizes timestamps and derives analytical helper columns."""
        self.logger.info("Normalizing taxi trip timestamps and deriving attributes...")

        # Source wall-clock values are retained for local calendar analytics.
        df = df.withColumn(
            "pickup_local_datetime", F.try_to_timestamp("pickup_datetime")
        ).withColumn("dropoff_local_datetime", F.try_to_timestamp("dropoff_datetime"))
        # Convert naive New York times to instants in the UTC session.
        df = df.withColumn(
            "pickup_datetime",
            F.to_utc_timestamp("pickup_local_datetime", self.source_timezone),
        ).withColumn(
            "dropoff_datetime",
            F.to_utc_timestamp("dropoff_local_datetime", self.source_timezone),
        )

        # Derive date, hour, and duration
        df = (
            df.withColumn("pickup_date", F.to_date("pickup_local_datetime"))
            .withColumn("pickup_year", F.year("pickup_local_datetime"))
            .withColumn("pickup_month", F.month("pickup_local_datetime"))
            .withColumn("pickup_day", F.dayofmonth("pickup_local_datetime"))
            .withColumn("pickup_hour", F.hour("pickup_local_datetime"))
            .withColumn(
                "trip_duration_minutes",
                F.round(
                    (
                        F.unix_timestamp("dropoff_datetime")
                        - F.unix_timestamp("pickup_datetime")
                    )
                    / 60.0,
                    2,
                ),
            )
        )

        # Apply column defaults from dataset configuration
        defaults = self.dataset_spec["column_defaults"]
        passenger_default = defaults["passenger_count"]
        tip_default = defaults["tip_amount"]
        tolls_default = defaults["tolls_amount"]

        df = (
            df.withColumn(
                "passenger_count",
                F.coalesce(F.col("passenger_count"), F.lit(passenger_default)),
            )
            .withColumn(
                "tip_amount", F.coalesce(F.col("tip_amount"), F.lit(tip_default))
            )
            .withColumn(
                "tolls_amount", F.coalesce(F.col("tolls_amount"), F.lit(tolls_default))
            )
        )

        # Generate unique surrogate trip hash key
        df = df.withColumn(
            "trip_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("vendor_id").cast("string"), F.lit("")),
                    F.col("pickup_datetime").cast("string"),
                    F.col("dropoff_datetime").cast("string"),
                    F.coalesce(F.col("pickup_location_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("dropoff_location_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("fare_amount").cast("string"), F.lit("")),
                    F.coalesce(F.col("trip_distance").cast("string"), F.lit("")),
                ),
                256,
            ),
        )
        return df
