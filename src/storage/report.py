"""Render storage benchmark tables from the current metrics contract."""

import argparse
import json
from pathlib import Path


def render(metrics_path: str | Path, output_path: str | Path) -> None:
    """Renders benchmark metrics JSON into Markdown report."""
    metrics_file = Path(metrics_path)
    metrics = json.loads(metrics_file.read_text(encoding="utf-8"))
    if metrics.get("status") != "success" or "benchmark" not in metrics:
        raise ValueError("A successful run with benchmark results is required")

    benchmark = metrics["benchmark"]
    if benchmark.get("input_kind") != "raw_taxi_parquet":
        raise ValueError("Benchmark input_kind must be raw_taxi_parquet")
    if benchmark.get("results_equivalent") is not True:
        raise ValueError("Benchmark query results must be equivalent")
    layouts = [
        "Strategy A (Unpartitioned)",
        "Strategy B (Date Partitioned)",
        "Strategy C (Monthly Partitioned)",
    ]
    if set(benchmark["storage_metrics"]) != set(layouts) or set(
        benchmark["query_statistics"]
    ) != set(layouts):
        raise ValueError("Benchmark must contain all three current storage layouts")
    storage = benchmark["storage_metrics"]
    runtime = metrics["runtime"]

    lines = [
        "# Week 1 Storage Benchmark Report",
        "",
        f"Generated from `{metrics_file.name}` for schema version {metrics['schema_version']}.",
        f"Run ID: `{benchmark['run_id']}`. Accepted taxi rows: {benchmark['total_records']:,}.",
        f"Runtime: Python {runtime['python']}, Spark {runtime['spark']}, Delta {runtime['delta']}.",
        "",
        "## Storage measurements",
        "",
        "| Metric | Unpartitioned | Daily | Monthly |",
        "|---|---:|---:|---:|",
    ]

    for label, key in [
        ("Taxi ingestion time (s)", "ingestion_time_sec"),
        ("Directory size (MiB)", "storage_size_mb"),
        ("Parquet files", "num_parquet_files"),
    ]:
        lines.append(
            f"| {label} | "
            + " | ".join(str(storage[name][key]) for name in layouts)
            + " |"
        )

    lines += [
        "",
        "## Warm query measurements",
        "",
        "Cells show mean ± sample standard deviation in milliseconds.",
        "",
        "| Query | Unpartitioned | Daily | Monthly |",
        "|---|---:|---:|---:|",
    ]

    queries = benchmark["query_statistics"][layouts[0]]
    if not queries or any(
        set(benchmark["query_statistics"][name]) != set(queries) for name in layouts
    ):
        raise ValueError("Benchmark query sets must be nonempty and identical")
    for query in queries:
        values = [benchmark["query_statistics"][name][query] for name in layouts]
        lines.append(
            f"| {query} | "
            + " | ".join(f"{s['mean_ms']:.2f} ± {s['stdev_ms']:.2f}" for s in values)
            + " |"
        )

    fastest_write = min(layouts, key=lambda name: storage[name]["ingestion_time_sec"])
    smallest = min(layouts, key=lambda name: storage[name]["storage_size_bytes"])

    lines += [
        "",
        "## Interpretation and limits",
        "",
        f"{fastest_write} had the shortest measured taxi ingestion; {smallest} used the least directory space.",
        "Query means must be considered alongside their variability. These repetitions are not independent cold-cache trials,",
        "and a small difference does not establish a general performance advantage.",
        "",
        "The three required queries compute trips per pickup borough, average duration per local day, and average fare per pickup borough.",
        "Two additional queries filter February 14 and February respectively. All layouts use the same raw taxi input and business queries.",
        f"Cross-layout query equivalence checked: {benchmark['results_equivalent']}.",
        "",
        benchmark["methodology"],
        "Raw timing samples, medians, layout paths, configuration and runtime metadata are retained in the JSON.",
        "Directory size includes Delta metadata; the field `storage_size_mb` uses MiB (1024² bytes).",
        "Benchmark runs use fresh subdirectories and preserve older runs. Write order remains fixed, with one measurement per layout.",
        "Physical plans are saved with query samples. Scan bytes are not measured; timings alone do not isolate compression or pruning effects.",
        "",
        "At 20× scale, distinguish higher trips per day from a longer date range. Re-evaluate partition sizes, writer parallelism,",
        "and selective-query performance before choosing daily or monthly partitions. Fewer files are not inherently better.",
    ]

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--metrics", default="storage/metrics/week1_benchmark_metrics.json"
    )
    parser.add_argument("--output", default="reports/week1/benchmark_report.md")
    args = parser.parse_args()
    render(args.metrics, args.output)
