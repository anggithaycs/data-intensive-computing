"""Run with: python -m src.week2.run [queries|products|benchmark|all]."""

import argparse
import json
from datetime import datetime
from pathlib import Path

from src.common.config import load_config
from src.common.spark_session import get_spark_session, stop_spark_session
from src.week2.analysis import QUERIES, prepare_views
from src.week2.benchmark import run_benchmarks
from src.week2.products import GOLD_QUERIES, build_products, load_products

ROOT = Path(__file__).resolve().parents[2]
GOLD_ROOT = ROOT / "storage/delta/gold/week2_simple"
RESULTS_ROOT = ROOT / "reports/week2_simple/runs"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["queries", "products", "benchmark", "all"])
    parser.add_argument("--query", choices=list(QUERIES))
    parser.add_argument(
        "--gold",
        action="store_true",
        help="Read saved Gold tables for the queries command",
    )
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument(
        "--month", default="2024-02", help="Month for the pruning experiment"
    )
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("--repeats must be positive")
    if args.gold and args.command != "queries":
        parser.error("--gold applies to the queries command")
    if args.query and args.command not in {"queries", "all"}:
        parser.error("--query applies to the queries stage")

    config = load_config(ROOT / "config/platform_config.yaml")
    source = str(ROOT / config["integration"]["output_path"])
    spark = get_spark_session(**{**config["spark"], "app_name": "Week2Simple"})
    output = RESULTS_ROOT / datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    output.mkdir(parents=True, exist_ok=True)
    try:
        # These are the only three input tables needed by this implementation.
        integrated = spark.read.format("delta").load(source)
        coverage = prepare_views(spark, integrated)
        for dataset, view in (("taxi_trips", "clean_trips"), ("taxi_zones", "zones")):
            path = str(ROOT / config["datasets"][dataset]["silver_path"])
            spark.read.format("delta").load(path).createOrReplaceTempView(view)
        print(f"Loaded {coverage['trip_count']:,} trips", flush=True)

        if args.command in {"products", "benchmark", "all"}:
            product_metrics = build_products(spark, GOLD_ROOT, source)
            (output / "products.json").write_text(
                json.dumps(product_metrics, indent=2), encoding="utf-8"
            )
        if args.command in {"queries", "all"}:
            if args.gold:
                load_products(spark, GOLD_ROOT)
            queries = GOLD_QUERIES if args.gold else QUERIES
            for name in [args.query] if args.query else queries:
                rows = spark.sql(queries[name]).collect()
                (output / f"{name}.json").write_text(
                    json.dumps(
                        [row.asDict() for row in rows],
                        indent=2,
                        default=str,
                        allow_nan=False,
                    ),
                    encoding="utf-8",
                )
                print(f"{name}: {len(rows)} rows; first five:", rows[:5], flush=True)
        if args.command in {"benchmark", "all"}:
            run_benchmarks(
                spark, output, product_metrics, coverage, args.repeats, args.month
            )
        print(f"Results saved in {output}", flush=True)
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
