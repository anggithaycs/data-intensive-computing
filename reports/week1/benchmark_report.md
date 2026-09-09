# Week 1 Storage Benchmark Report

This report compares three Delta storage layouts for the same 9,394,330 accepted taxi rows: an unpartitioned table, daily partitions and monthly partitions. The measurements come from `week1_benchmark_metrics_1788945334850314700.json`, using schema version 2.0.0 and run ID `7378aaa59cad4b35ac40eb9a75ca9daf`. The runtime was Python 3.12.14, Spark 4.0.1 and Delta 4.0.1.

## Storage measurements

| Metric | Unpartitioned | Daily | Monthly |
|---|---:|---:|---:|
| Taxi ingestion time (s) | 191.05 | 192.817 | 184.841 |
| Directory size (MiB) | 1038.98 | 921.28 | 1024.42 |
| Parquet files | 4 | 91 | 4 |

## Warm query measurements

The first three queries calculate trips per pickup borough, average trip duration per local day and average fare per pickup borough. Two additional queries restrict the analysis to February 14 and to February, respectively. Every layout uses the same raw taxi input and the same business queries. The benchmark confirmed that all three layouts produced equivalent results.

Each cell shows the mean execution time in milliseconds, followed by the sample standard deviation. These are warm-system measurements, so the variation between repetitions should be considered alongside the mean.

| Query | Unpartitioned | Daily | Monthly |
|---|---:|---:|---:|
| Q1_Trips_Per_Borough | 951.48 ± 57.76 | 1036.23 ± 130.87 | 845.77 ± 89.71 |
| Q2_Avg_Duration_Per_Day | 458.13 ± 55.45 | 595.96 ± 76.12 | 431.28 ± 12.17 |
| Q3_Avg_Fare_Per_Borough | 822.38 ± 88.80 | 888.20 ± 42.61 | 823.64 ± 47.92 |
| Q4_Single_Day_Pruned_Analysis | 642.70 ± 56.12 | 356.65 ± 19.33 | 452.44 ± 72.10 |
| Q5_Month_Pruned_Analysis | 659.08 ± 88.83 | 603.33 ± 40.66 | 477.72 ± 40.92 |

## Interpretation and limits

Monthly partitioning (Strategy C) had the shortest measured taxi ingestion time, while daily partitioning (Strategy B) used the least directory space. Daily partitions also had the lowest mean time for the single-day query, and monthly partitions had the lowest mean time for the February query. These results describe this workload and run. Small differences between query means do not establish a general performance advantage.

The ingestion timer covers raw Parquet loading, schema checks, casts, transformations, disk materialization, validation, deduplication, redistribution to an equal number of writers, and the Delta write. Each layout performs this work independently. Shared Bronze, quarantine and metadata writes are excluded, so the ingestion times do not represent a complete platform run.

Each query receives one warmup before its measured repetitions. Spark’s cache is cleared before measured queries, but the operating system cache remains warm. Strategy order rotates between queries to reduce ordering effects. Ingestion uses a fixed order and only one trial per layout, which limits how confidently those timings can be compared. The query repetitions are not independent cold-cache trials.

Each benchmark execution uses fresh subdirectories and preserves older runs. Directory sizes include Delta metadata; although the JSON field is named `storage_size_mb`, its unit is MiB (1024² bytes). The JSON retains raw timing samples, medians, layout paths, configuration, runtime metadata and physical plans. Scan bytes are not measured, so timings alone cannot separate the effects of compression and partition pruning.

Before applying these results to a dataset twenty times larger, establish whether that growth means more trips per day or a longer date range. Those cases can place different demands on the layout. Re-evaluate partition sizes, writer parallelism and selective-query performance before choosing daily or monthly partitions. File count is only one part of that decision; fewer files do not automatically mean better performance.
