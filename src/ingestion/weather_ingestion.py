"""Weather Dataset Ingestion Implementation.

Parses hourly NYC weather observations, builds canonical ISO timestamps,
and applies meteorological bounds validation.
"""

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.ingestion.base_ingestion import BaseDatasetIngestor


class WeatherIngestor(BaseDatasetIngestor):
    """Specialized Ingestor for NYC Hourly Weather Data."""

    def __init__(
        self,
        spark: SparkSession,
        dataset_spec: dict[str, Any],
        schema_version: str,
        metadata_path: str,
    ) -> None:
        super().__init__(
            spark=spark,
            dataset_name="Weather",
            dataset_spec=dataset_spec,
            schema_version=schema_version,
            metadata_path=metadata_path,
        )

    def normalize_and_transform(self, df: DataFrame) -> DataFrame:
        """Constructs standardized UTC timestamps and derives composite keys."""
        self.logger.info("Building observation timestamps from year/month/day/hour...")

        # Construct ISO timestamp string
        ts_str = F.concat_ws(
            " ",
            F.concat_ws(
                "-",
                F.col("year").cast("string"),
                F.lpad(F.col("month").cast("string"), 2, "0"),
                F.lpad(F.col("day").cast("string"), 2, "0"),
            ),
            F.concat(F.lpad(F.col("hour").cast("string"), 2, "0"), F.lit(":00:00")),
        )

        df = df.withColumn(
            "observation_datetime",
            F.to_utc_timestamp(F.try_to_timestamp(ts_str), self.source_timezone),
        ).withColumn("observation_date", F.to_date(F.col("observation_datetime")))

        # Unique observation key
        df = df.withColumn(
            "weather_observation_id",
            F.concat_ws(
                "_", F.col("year"), F.col("month"), F.col("day"), F.col("hour")
            ),
        )

        # Output columns configured in dataset_spec
        return df
