"""SparkSession Factory Module.

Configures Apache Spark with Delta Lake extensions, optimized memory,
and proper Windows/Hadoop integration.
"""

import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark_session(
    app_name: str = "UrbanDataPlatform",
    master: str = "local[*]",
    driver_memory: str = "8g",
    shuffle_partitions: int = 8,
    enable_aqe: bool = True,
) -> SparkSession:
    """Creates or retrieves a singleton SparkSession configured for Delta Lake.

    Args:
        app_name: Name of the Spark application.
        master: Spark master URL (defaults to local[*]).
        driver_memory: Memory allocated to the driver.
        shuffle_partitions: Default number of shuffle partitions.
        enable_aqe: Whether to enable Adaptive Query Execution (AQE).

    Returns:
        Configured SparkSession instance.
    """
    # Ensure HADOOP_HOME is set for Windows compatibility
    if "HADOOP_HOME" not in os.environ:
        if Path(r"C:\hadoop").exists():
            os.environ["HADOOP_HOME"] = r"C:\hadoop"
        elif Path("./hadoop").resolve().exists():
            os.environ["HADOOP_HOME"] = str(Path("./hadoop").resolve())

    # Add bin to path if needed
    if "HADOOP_HOME" in os.environ:
        hadoop_bin = os.path.join(os.environ["HADOOP_HOME"], "bin")
        if hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true" if enable_aqe else "false")
        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true" if enable_aqe else "false",
        )
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def stop_spark_session(spark: SparkSession | None) -> None:
    """Safely stops the active SparkSession."""
    if spark is not None:
        try:
            spark.stop()
        except Exception as error:  # noqa: BLE001
            from src.common.logger import get_logger

            get_logger("SparkSession").debug(f"Error stopping Spark session: {error}")
