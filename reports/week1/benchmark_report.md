# Week 1 Storage Benchmark Report

Generated from `week1_benchmark_metrics_1788945334850314700.json` for schema version 2.0.0.
Run ID: `7378aaa59cad4b35ac40eb9a75ca9daf`. Accepted taxi rows: 9,394,330.
Runtime: Python 3.12.14, Spark 4.0.1, Delta 4.0.1.

## Storage measurements

| Metric | Unpartitioned | Daily | Monthly |
|---|---:|---:|---:|
| Taxi ingestion time (s) | 191.05 | 192.817 | 184.841 |
| Directory size (MiB) | 1038.98 | 921.28 | 1024.42 |
| Parquet files | 4 | 91 | 4 |

## Warm query measurements

Cells show mean ± sample standard deviation in milliseconds.

| Query | Unpartitioned | Daily | Monthly |
|---|---:|---:|---:|
| Q1_Trips_Per_Borough | 951.48 ± 57.76 | 1036.23 ± 130.87 | 845.77 ± 89.71 |
| Q2_Avg_Duration_Per_Day | 458.13 ± 55.45 | 595.96 ± 76.12 | 431.28 ± 12.17 |
| Q3_Avg_Fare_Per_Borough | 822.38 ± 88.80 | 888.20 ± 42.61 | 823.64 ± 47.92 |
| Q4_Single_Day_Pruned_Analysis | 642.70 ± 56.12 | 356.65 ± 19.33 | 452.44 ± 72.10 |
| Q5_Month_Pruned_Analysis | 659.08 ± 88.83 | 603.33 ± 40.66 | 477.72 ± 40.92 |

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
