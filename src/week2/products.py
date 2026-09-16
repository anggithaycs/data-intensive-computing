"""Build the four Gold products and load their analytical SQL queries."""

import time
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.week2.analysis import QUERY_KEYS
from src.week2.sql_files import read_sql

PRODUCTS = (
    "daily_mobility_summary",
    "taxi_zone_statistics",
    "weather_impact_summary",
    "air_quality_impact_summary",
)

GOLD_QUERIES = {name: read_sql(f"gold/{name}.sql") for name in QUERY_KEYS}


def load_products(spark, gold_root):
    for name in PRODUCTS:
        spark.read.format("delta").load(str(gold_root / name)).createOrReplaceTempView(
            f"gold_{name}"
        )


def build_products(spark, gold_root, source_path):
    """Replace four small snapshots and a four-row metadata table.

    Use full refreshes in Week 2. Rerun this function after any Silver update.
    Writes are atomic per table, not a transaction across all four products.
    """
    gold_root = Path(gold_root)
    metadata_path = str(gold_root / "_metadata")
    created = {}

    if DeltaTable.isDeltaTable(spark, metadata_path):
        # Read as strings in Spark's UTC timezone, avoiding host timezone conversion.
        created = {
            r.product: r.created_at
            for r in spark.read.format("delta")
            .load(metadata_path)
            .selectExpr("product", "CAST(created_at AS STRING) AS created_at")
            .collect()
        }

    metadata = []
    metrics = {}

    for name in PRODUCTS:
        sql = read_sql(f"products/{name}.sql")
        path = str(gold_root / name)

        start = time.perf_counter()
        product = spark.sql(sql).coalesce(1)
        (
            product.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(path)
        )
        elapsed = time.perf_counter() - start

        count = spark.read.format("delta").load(path).count()
        detail = DeltaTable.forPath(spark, path).detail().first()
        metrics[name] = {
            "rows": count,
            "build_seconds": elapsed,
            "active_bytes": detail.sizeInBytes,
            "active_files": detail.numFiles,
        }

        # Literal expressions stay in the JVM; no Python worker is needed.
        metadata.append(
            spark.range(1).select(
                F.lit(name).alias("product"),
                F.lit(source_path).alias("data_source"),
                F.coalesce(
                    F.lit(created.get(name)).cast("timestamp"), F.current_timestamp()
                ).alias("created_at"),
                F.current_timestamp().alias("refreshed_at"),
                F.lit("1.0").alias("schema_version"),
                F.lit(count).cast("long").alias("row_count"),
            )
        )
        print(f"Gold {name}: {count:,} rows", flush=True)

    rows = metadata[0]
    for other in metadata[1:]:
        rows = rows.unionByName(other)

    rows.coalesce(1).write.format("delta").mode("overwrite").save(metadata_path)
    load_products(spark, gold_root)
    return metrics
