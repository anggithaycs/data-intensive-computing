"""Taxi Zone Lookup Dataset Ingestion Implementation.

Processes reference lookup table mapping LocationID to Zone, Borough, and Service Zone.
"""

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.ingestion.base_ingestion import BaseDatasetIngestor


class TaxiZoneIngestor(BaseDatasetIngestor):
    """Specialized Ingestor for Taxi Zone Reference Lookup."""

    def __init__(
        self,
        spark: SparkSession,
        dataset_spec: dict[str, Any],
        schema_version: str,
        metadata_path: str,
    ) -> None:
        super().__init__(
            spark=spark,
            dataset_name="TaxiZones",
            dataset_spec=dataset_spec,
            schema_version=schema_version,
            metadata_path=metadata_path,
        )

    def normalize_and_transform(self, df: DataFrame) -> DataFrame:
        """Standardizes location id and trims whitespace."""
        self.logger.info("Standardizing taxi zone lookup attributes...")

        df = (
            df.withColumn("borough", F.trim(F.col("borough")))
            .withColumn("zone", F.trim(F.col("zone")))
            .withColumn("service_zone", F.trim(F.col("service_zone")))
        )

        return df
