"""Spatial-Temporal Contextual Integration Pipeline.

Enriches NYC Taxi Trips with:
1. Hourly weather observations matched to the containing UTC pickup hour.
2. PM2.5 air quality observations matched by pickup timestamp and pickup borough (with citywide fallback).
3. Pickup and dropoff zones, boroughs, and service zones from reference lookups via broadcast joins.
"""

import time
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.logger import get_logger

logger = get_logger("IntegrationPipeline")


class UrbanDataIntegrationPipeline:
    """Orchestrates multi-source contextual data integration."""

    def __init__(
        self,
        spark: SparkSession,
        taxi_delta_path: str,
        weather_delta_path: str,
        air_quality_delta_path: str,
        zones_delta_path: str,
        output_delta_path: str,
        partition_cols: list[str] | None = None,
    ) -> None:
        self.spark = spark
        self.taxi_delta_path = str(Path(taxi_delta_path))
        self.weather_delta_path = str(Path(weather_delta_path))
        self.air_quality_delta_path = str(Path(air_quality_delta_path))
        self.zones_delta_path = str(Path(zones_delta_path))
        self.output_delta_path = str(Path(output_delta_path))
        self.partition_cols = (
            ["pickup_year", "pickup_month"]
            if partition_cols is None
            else partition_cols
        )
        self.logger = logger

    @classmethod
    def from_config(
        cls, spark: SparkSession, config: dict[str, Any]
    ) -> "UrbanDataIntegrationPipeline":
        """Instantiates integration pipeline from unified platform configuration."""
        datasets = config["datasets"]
        integration = config["integration"]
        return cls(
            spark=spark,
            taxi_delta_path=datasets["taxi_trips"]["silver_path"],
            weather_delta_path=datasets["weather"]["silver_path"],
            air_quality_delta_path=datasets["air_quality"]["silver_path"],
            zones_delta_path=datasets["taxi_zones"]["silver_path"],
            output_delta_path=integration["output_path"],
            partition_cols=integration["partition_cols"],
        )

    def load_sources(self) -> dict[str, DataFrame]:
        """Loads clean silver Delta tables for integration."""
        self.logger.info("Loading silver Delta tables...")
        taxi_df = self.spark.read.format("delta").load(self.taxi_delta_path)
        weather_df = self.spark.read.format("delta").load(self.weather_delta_path)
        air_df = self.spark.read.format("delta").load(self.air_quality_delta_path)
        zones_df = self.spark.read.format("delta").load(self.zones_delta_path)
        return {
            "taxi": taxi_df,
            "weather": weather_df,
            "air": air_df,
            "zones": zones_df,
        }

    def prepare_air_quality_aggregates(
        self, air_df: DataFrame
    ) -> tuple[DataFrame, DataFrame]:
        """Aggregates sensor measurements using a two-stage hierarchical weighting:

        1. Site-level hourly average: Averages collocated instruments within each physical
           monitoring station (state_code, county_code, site_number) for each hour.
        2. Borough-level hourly average: Averages across physical stations within each borough,
           giving equal weight to each physical station.
        3. Citywide hourly average: Averages across all active physical stations in NYC,
           giving equal weight to each physical station (fallback for boroughs with no active monitors).
        """
        self.logger.info(
            "Computing site-level, borough-level, and citywide air quality aggregates..."
        )

        site_keys = ["state_code", "county_code", "site_number"]
        missing = sorted(set(site_keys) - set(air_df.columns))
        if missing:
            raise ValueError(
                f"Air quality requires complete site identifiers: {missing}"
            )
        site_hourly = air_df.groupBy("observation_datetime", "borough", *site_keys).agg(
            F.avg("sample_measurement_pm25").alias("site_pm25")
        )
        measure_col = "site_pm25"

        # Step 2: Borough-level average PM2.5 per hour (equal weight per physical station)
        borough_aq = site_hourly.groupBy("observation_datetime", "borough").agg(
            F.round(F.avg(measure_col), 2).alias("pm25_borough_level")
        )

        # Step 3: Citywide average PM2.5 per hour across all active NYC stations (equal weight per physical station)
        citywide_aq = site_hourly.groupBy("observation_datetime").agg(
            F.round(F.avg(measure_col), 2).alias("pm25_citywide_level")
        )
        return borough_aq, citywide_aq

    @staticmethod
    def join_zones(trips: DataFrame, zones: DataFrame, endpoint: str) -> DataFrame:
        lookup = zones.select(
            F.col("location_id").alias("_zone_id"),
            *[
                F.col(c).alias(f"{endpoint}_{c}")
                for c in ("borough", "zone", "service_zone")
            ],
        )
        return (
            trips.join(
                F.broadcast(lookup),
                F.col(f"{endpoint}_location_id") == F.col("_zone_id"),
                "left",
            )
            .drop("_zone_id")
            .fillna({f"{endpoint}_borough": "Unknown", f"{endpoint}_zone": "Unknown"})
        )

    @staticmethod
    def join_weather(trips: DataFrame, weather: DataFrame, endpoint: str) -> DataFrame:
        fields = {
            "temperature_c": "temperature_c",
            "relative_humidity_pct": "humidity_pct",
            "precipitation_mm": "precipitation_mm",
            "weather_condition_code": "weather_code",
        }
        if endpoint == "pickup":
            fields.update(
                wind_speed_kmh="wind_speed_kmh",
                air_pressure_hpa="pressure_hpa",
                cloud_cover_oktas="cloud_cover",
            )
        lookup = weather.select(
            F.col("observation_datetime").alias("_weather_hour"),
            *[F.col(c).alias(f"{endpoint}_{name}") for c, name in fields.items()],
        )
        return (
            trips.join(
                F.broadcast(lookup),
                F.col("_context_hour") == F.col("_weather_hour"),
                "left",
            )
            .withColumn(
                f"{endpoint}_weather_matched", F.col("_weather_hour").isNotNull()
            )
            .withColumn(
                f"{endpoint}_precipitation_missing",
                F.col(f"{endpoint}_precipitation_mm").isNull(),
            )
            .drop("_weather_hour")
        )

    @staticmethod
    def join_air_quality(
        trips: DataFrame, borough: DataFrame, citywide: DataFrame, endpoint: str
    ) -> DataFrame:
        borough_lookup = borough.select(
            F.col("observation_datetime").alias("_air_hour"),
            F.col("borough").alias("_air_borough"),
            F.col("pm25_borough_level").alias(f"{endpoint}_pm25_borough"),
        )
        trips = trips.join(
            F.broadcast(borough_lookup),
            (F.col("_context_hour") == F.col("_air_hour"))
            & (F.col(f"{endpoint}_borough") == F.col("_air_borough")),
            "left",
        ).drop("_air_hour", "_air_borough")
        city_lookup = citywide.select(
            F.col("observation_datetime").alias("_air_hour"),
            F.col("pm25_citywide_level").alias(f"{endpoint}_pm25_citywide"),
        )
        return (
            trips.join(
                F.broadcast(city_lookup),
                F.col("_context_hour") == F.col("_air_hour"),
                "left",
            )
            .drop("_air_hour")
            .withColumn(
                f"{endpoint}_pm25",
                F.coalesce(
                    F.col(f"{endpoint}_pm25_borough"),
                    F.col(f"{endpoint}_pm25_citywide"),
                ),
            )
            .withColumn(
                f"{endpoint}_pm25_source",
                F.when(F.col(f"{endpoint}_pm25_borough").isNotNull(), "borough")
                .when(F.col(f"{endpoint}_pm25_citywide").isNotNull(), "citywide")
                .otherwise("missing"),
            )
        )

    def build_integrated_dataset(
        self,
        taxi_df: DataFrame,
        weather_df: DataFrame,
        air_df: DataFrame,
        zones_df: DataFrame,
    ) -> DataFrame:
        """Match each endpoint to zones and its containing UTC observation hour."""
        borough, citywide = self.prepare_air_quality_aggregates(air_df)
        enriched = taxi_df
        for endpoint in ("pickup", "dropoff"):
            enriched = enriched.withColumn(
                "_context_hour", F.date_trunc("hour", F.col(f"{endpoint}_datetime"))
            )
            enriched = self.join_zones(enriched, zones_df, endpoint)
            enriched = self.join_weather(enriched, weather_df, endpoint)
            enriched = self.join_air_quality(enriched, borough, citywide, endpoint)
        return enriched.drop("_context_hour")

    def assert_trip_identity(self, taxi_df: DataFrame, integrated_df: DataFrame) -> int:
        """Fail before publishing if a join drops, duplicates, or changes a trip ID."""
        stats = None
        for label, frame in (("source", taxi_df), ("integrated", integrated_df)):
            stats = frame.agg(
                F.count("*").alias("rows"), F.countDistinct("trip_id").alias("ids")
            ).first()
            if stats.rows != stats.ids:
                raise ValueError(f"{label}: trip_id must be unique and non-null")
        source_ids = taxi_df.select("trip_id")
        output_ids = integrated_df.select("trip_id")
        if (
            source_ids.join(output_ids, "trip_id", "left_anti").limit(1).count()
            or output_ids.join(source_ids, "trip_id", "left_anti").limit(1).count()
        ):
            raise ValueError("Integration changed the set of trip IDs")
        return stats.rows

    def run(self) -> dict[str, Any]:
        """Executes end-to-end integration and saves output to Delta Lake."""
        start_time = time.perf_counter()
        self.logger.info("=== Starting Contextual Integration Pipeline ===")

        sources = self.load_sources()
        integrated_df = self.build_integrated_dataset(
            taxi_df=sources["taxi"],
            weather_df=sources["weather"],
            air_df=sources["air"],
            zones_df=sources["zones"],
        )

        integrated_df = integrated_df.persist()
        try:
            total_records = self.assert_trip_identity(sources["taxi"], integrated_df)
            coverage = (
                integrated_df.agg(
                    *[
                        F.sum(
                            F.when(
                                F.col(f"{endpoint}_pm25_source") == source, 1
                            ).otherwise(0)
                        ).alias(f"{endpoint}_pm25_{source}_rows")
                        for endpoint in ("pickup", "dropoff")
                        for source in ("borough", "citywide", "missing")
                    ],
                    *[
                        F.sum(F.col(f"{endpoint}_weather_matched").cast("long")).alias(
                            f"{endpoint}_weather_matched_rows"
                        )
                        for endpoint in ("pickup", "dropoff")
                    ],
                )
                .first()
                .asDict()
            )
            writer = (
                integrated_df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
            )
            if self.partition_cols:
                writer = (
                    integrated_df.repartition(
                        int(self.spark.conf.get("spark.sql.shuffle.partitions"))
                    )
                    .write.format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .partitionBy(*self.partition_cols)
                )
            writer.save(self.output_delta_path)
        finally:
            integrated_df.unpersist()

        duration = round(time.perf_counter() - start_time, 3)
        return {
            "pipeline_name": "UrbanDataIntegrationPipeline",
            "output_path": self.output_delta_path,
            "total_records": total_records,
            "duration_seconds": duration,
            "partition_columns": self.partition_cols,
            "trip_identity_preserved": True,
            "context_coverage": coverage,
        }
