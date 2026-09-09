"""Air Quality Dataset Ingestion Implementation.

Filters EPA nationwide PM2.5 observations to NYC metropolitan area,
normalizes timestamps, maps counties to boroughs, and enforces air quality bounds.
"""

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.ingestion.base_ingestion import BaseDatasetIngestor


class AirQualityIngestor(BaseDatasetIngestor):
    """Specialized Ingestor for EPA Hourly Air Quality (PM2.5) Data."""

    def __init__(
        self,
        spark: SparkSession,
        dataset_spec: dict[str, Any],
        schema_version: str,
        metadata_path: str,
    ) -> None:
        super().__init__(
            spark=spark,
            dataset_name="AirQuality",
            dataset_spec=dataset_spec,
            schema_version=schema_version,
            metadata_path=metadata_path,
        )

    def filter_scope(self, df: DataFrame) -> DataFrame:
        """Filters national observations to targeted state and county codes from configuration."""
        return df.filter(
            F.coalesce(
                (F.col("state_code") == self.quality_rules["ny_state_code"])
                & F.col("county_code").isin(self.quality_rules["nyc_county_codes"]),
                F.lit(True),
            )
        )

    def normalize_and_transform(self, df: DataFrame) -> DataFrame:
        """Use EPA GMT fields; EPA local time is standard time, not DST."""
        df_ny = df
        # Construct ISO timestamp from Date GMT and Time GMT.
        ts_str = F.concat_ws(
            " ", F.col("date_gmt"), F.concat(F.col("time_gmt"), F.lit(":00"))
        )
        df_ny = (
            df_ny.withColumn("observation_datetime", F.try_to_timestamp(ts_str))
            .withColumn("observation_date", F.to_date(F.col("observation_datetime")))
            .withColumn("observation_hour", F.hour(F.col("observation_datetime")))
        )

        # Map county code to standard Borough name using configuration
        borough_mapping = F.create_map(
            *[
                value
                for code, name in self.dataset_spec["county_borough_mapping"].items()
                for value in (F.lit(int(code)), F.lit(name))
            ]
        )
        df_ny = df_ny.withColumn(
            "borough", F.element_at(borough_mapping, F.col("county_code"))
        )

        # Generate unique sensor measurement key
        df_ny = df_ny.withColumn(
            "air_quality_observation_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("state_code").cast("string"),
                    F.col("county_code").cast("string"),
                    F.col("site_number").cast("string"),
                    F.col("poc").cast("string"),
                    F.col("observation_datetime").cast("string"),
                ),
                256,
            ),
        )

        return df_ny
