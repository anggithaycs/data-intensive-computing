"""Run Week 1 ingestion, integration, and optional storage benchmarks."""

import argparse
import json
import os
import platform
import sys
import time
from importlib.metadata import version
from pathlib import Path
from typing import Any
from uuid import uuid4

# Ensure project root is available on sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.config import build_ingestors, load_config
from src.common.logger import get_logger
from src.common.spark_session import get_spark_session, stop_spark_session
from src.integration.pipeline import UrbanDataIntegrationPipeline
from src.storage.benchmark import StorageBenchmarkRunner
from src.storage.report import render

logger = get_logger("RunWeek1")


def save_summary_metrics(
    summary: dict[str, Any], metrics_file_path: str | Path
) -> Path:
    """Safely persist run summary to timestamped and latest metrics files."""
    output = Path(metrics_file_path).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    run_output = output.with_name(f"{output.stem}_{time.time_ns()}.json")
    payload = json.dumps(summary, indent=2, default=str)

    run_output.write_text(payload, encoding="utf-8")
    temporary = output.with_name(f".{output.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(payload, encoding="utf-8")
        temporary.replace(output)
    finally:
        temporary.unlink(missing_ok=True)
    return run_output


def run(config: dict[str, Any], skip_benchmark: bool = False) -> dict[str, Any]:
    """Execute a validated snapshot and publish its required execution artifacts."""
    from src.common.config import validate_config

    validate_config(config)
    start = time.perf_counter()
    spark = None

    summary: dict[str, Any] = {
        "run_id": uuid4().hex,
        "schema_version": config["platform"]["version"],
        "status": "running",
        "runtime": {
            "python": platform.python_version(),
            "spark": None,
            "delta": version("delta-spark"),
            "platform": platform.platform(),
        },
        "configuration": config,
        "ingestion": {},
    }

    try:
        logger.info("Initializing Spark session...")
        spark = get_spark_session(**config["spark"])
        summary["runtime"]["spark"] = spark.version

        logger.info("Starting ingestion stage...")
        ingestors = build_ingestors(spark, config)
        logger.info(
            "Resolving all input, validation, and integration schemas before writes..."
        )
        prepared = {name: ingestor.preflight() for name, ingestor in ingestors.items()}
        pipeline = UrbanDataIntegrationPipeline.from_config(spark=spark, config=config)
        _ = (
            pipeline.build_integrated_dataset(
                prepared["taxi_trips"],
                prepared["weather"],
                prepared["air_quality"],
                prepared["taxi_zones"],
            )
            .select(*pipeline.partition_cols)
            .schema
        )
        for name, ingestor in ingestors.items():
            summary["ingestion"][name] = ingestor.run(run_id=summary["run_id"])

        logger.info("Starting integration pipeline...")
        summary["integration"] = pipeline.run()

        if not skip_benchmark:
            logger.info("Starting storage benchmarking stage...")
            benchmark_cfg = config["benchmark"]
            storage_paths = benchmark_cfg["storage_paths"]
            summary["benchmark"] = StorageBenchmarkRunner(
                spark=spark,
                taxi_ingestor=ingestors["taxi_trips"],
                zones_delta_path=config["datasets"]["taxi_zones"]["silver_path"],
                writer_partitions=config["spark"]["shuffle_partitions"],
                strategy_a_path=storage_paths["strategy_a_unpartitioned"],
                strategy_b_path=storage_paths["strategy_b_partitioned"],
                strategy_c_path=storage_paths["strategy_c_monthly_partitioned"],
            ).run_benchmark(iterations=benchmark_cfg["iterations"])

        summary["status"] = "success"

    except Exception as error:
        logger.exception("Pipeline failed")
        summary.update(status="failed", error=str(error))
        raise

    finally:
        summary["total_execution_seconds"] = round(time.perf_counter() - start, 3)
        try:
            metrics_path = config["paths"]["metrics_path"]
            run_output = save_summary_metrics(summary, metrics_path)
            if summary["status"] == "success" and "benchmark" in summary:
                report_path = config["paths"]["benchmark_report"]
                render(run_output, report_path)
        except Exception:
            logger.exception("Failed writing execution summary or report")
            if summary["status"] == "success":
                raise
        finally:
            if spark is not None:
                stop_spark_session(spark)

    logger.info("Pipeline and execution artifacts completed successfully.")
    return summary


def main() -> None:
    """Resolve CLI configuration paths against the project root."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(ROOT / "config/platform_config.yaml"))
    parser.add_argument("--skip-benchmark", action="store_true")
    args = parser.parse_args()
    config_path = Path(args.config).resolve()
    os.chdir(ROOT)
    run(load_config(config_path), skip_benchmark=args.skip_benchmark)


if __name__ == "__main__":
    main()
