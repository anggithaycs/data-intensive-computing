# Week 1 Storage Benchmark Report

Generated from `week1_benchmark_metrics_1789577222617599500.json` for schema version 2.0.0.
Run ID: `ae7a85e36c6b4e20944a8932a02a9bf0`. Accepted taxi rows: 9,394,330.
Runtime: Python 3.12.14, Spark 4.0.1, Delta 4.0.1.

## Storage measurements

| Metric | Unpartitioned | Daily | Monthly |
|---|---:|---:|---:|
| Taxi ingestion time (s) | 346.656 | 197.888 | 186.55 |
| Directory size (MiB) | 1038.98 | 921.28 | 1024.42 |
| Parquet files | 4 | 91 | 4 |

## Warm query measurements

Cells show mean ± sample standard deviation in milliseconds.

| Query | Unpartitioned | Daily | Monthly |
|---|---:|---:|---:|
| Q1_Trips_Per_Borough | 697.98 ± 107.61 | 804.59 ± 133.22 | 616.18 ± 56.88 |
| Q2_Avg_Duration_Per_Day | 459.76 ± 43.00 | 577.94 ± 63.31 | 465.42 ± 33.92 |
| Q3_Avg_Fare_Per_Borough | 761.94 ± 92.28 | 885.31 ± 64.37 | 767.59 ± 53.32 |
| Q4_Single_Day_Pruned_Analysis | 669.14 ± 57.73 | 446.03 ± 56.82 | 520.61 ± 33.94 |
| Q5_Month_Pruned_Analysis | 811.81 ± 80.99 | 668.49 ± 26.94 | 616.95 ± 85.42 |

## Interpretation and limits

Strategy C (Monthly Partitioned) had the shortest measured taxi ingestion; Strategy B (Date Partitioned) used the least directory space.
Query means must be considered alongside their variability. These repetitions are not independent cold-cache trials,
and a small difference does not establish a general performance advantage.

The three required queries compute trips per pickup borough, average duration per local day, and average fare per pickup borough.
Two additional queries filter February 14 and February respectively. All layouts use the same raw taxi input and business queries.
Cross-layout query equivalence checked: True.

Raw Parquet loading, schema checks, casts, transformations, disk materialization, validation, deduplication, equal writer redistribution and Delta write timed separately for each layout. Shared Bronze, quarantine and metadata writes excluded. One warmup; Spark cache cleared before measured queries, OS cache remains warm. Strategy order rotated per query; ingestion order fixed with one trial per layout.
Raw timing samples, medians, layout paths, configuration and runtime metadata are retained in the JSON.
Directory size includes Delta metadata; the field `storage_size_mb` uses MiB (1024² bytes).
Benchmark runs use fresh subdirectories and preserve older runs. Write order remains fixed, with one measurement per layout.
Physical plans are saved with query samples. Scan bytes are not measured; timings alone do not isolate compression or pruning effects.

At 20× scale, distinguish higher trips per day from a longer date range. Re-evaluate partition sizes, writer parallelism,
and selective-query performance before choosing daily or monthly partitions. Fewer files are not inherently better.
