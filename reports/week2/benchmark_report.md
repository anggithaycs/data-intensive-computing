# Week 2: benchmark report

## Methodology and evidence

This report evaluates optimization techniques using benchmark run `20260917_161256_727995` from 17 September 2026 on **9,394,330 trips** spanning 1 January to 31 March 2024. The recorded execution environment is Apache Spark 4.0.1 running on `local[*]` with eight shuffle partitions. Because execution occurred on a local development workstation, these timings illustrate architectural and execution plan trade-offs rather than portable production targets.

As required by the assignment, **every analytical query is compared before and after optimization** across all five optimization strategies: Gold preaggregation, in-memory table caching, Adaptive Query Execution (AQE) with shuffle coalescing, explicit partition pruning, and broadcast hash joins.

The benchmark evaluates 48 distinct cases (6 analytical queries across 8 experimental configurations). Each case runs one warmup execution followed by five timed iterations, yielding 240 measured query executions. Timings measure the entire latency including SQL planning, stage execution, and driver result collection (`collect()`). Result correctness validation, physical plan extraction, Gold product materialization, and cache population are excluded from query timing. Every optimized run was verified for semantic equivalence against its baseline answer, asserting exact key matching, identical row counts and null distributions, and floating-point agreement within tolerance (`rel_tol=1e-9`, `abs_tol=1e-8`).

### Baseline design across the five optimizations

Each technique uses a baseline appropriate to its input and date scope:

- **Initial Silver baseline (`_silver`)**: Used directly for **Gold preaggregation**, **In-memory caching**, and **AQE**. All three run against `integrated_taxi_trips` over the entire 3-month dataset.
- **Scoped baseline for Partition Pruning (`_date_filter`)**: Evaluated on February 2024 using date range predicates alone (`pickup_date >= '2024-02-01' AND pickup_date < '2024-03-01'`). Comparing a 1-month partition-pruned query against the 3-month Silver baseline would confound data volume reduction with partition-skipping effectiveness. Monthly trends reads January and February to preserve the preceding-month comparison. Holding each pair's date window identical isolates whether Catalyst explicit partition filters offer incremental gain over Delta date skipping.
- **Join baseline for Broadcast Join (`_no_broadcast`)**: Evaluated by joining clean taxi trips with zone and hourly weather/PM2.5 lookup tables using unhinted sort-merge joins (`SortMergeJoin`). Because `integrated_taxi_trips` is already denormalized and pre-joined during ingestion, those enrichment joins are absent from its scan (analytical queries may still contain other joins). Reconstructing the join on clean tables and comparing unhinted joins against broadcast-hinted joins (`/*+ BROADCAST */`) isolates the true impact of broadcast hash joins.

Reported latencies represent median wall-clock milliseconds. **Speedup = before / after**; a value above 1.0 represents a performance gain, while a value below 1.0 indicates a performance regression. In the baseline configuration, AQE and automatic broadcasting are explicitly disabled (`spark.sql.adaptive.enabled=false`, `spark.sql.autoBroadcastJoinThreshold=-1`). The benchmark run artifacts, including `comparison.json`, `metrics.json`, and physical execution plans under `plans/`, preserve the measured evidence in `storage/metrics/week2/runs/20260917_161256_727995/`.

## Execution times before and after optimization

### Optimization summary across all queries

The table below summarizes the speedup factor achieved by each optimization technique across all six analytical queries:

| Analysis Query | Silver vs Gold | Caching | AQE Coalescing | Partition Pruning | Broadcast Join |
|---|---:|---:|---:|---:|---:|
| Monthly zone demand | **3.19x** | 0.63x | 0.96x | 1.03x | **3.09x** |
| Weather and distance | **2.06x** | 1.10x | 1.04x | 0.95x | **1.83x** |
| Air quality and demand | **5.21x** | **2.36x** | 1.06x | 0.95x | **2.23x** |
| Weather demand variation | **11.07x** | 0.76x | **1.25x** | 0.94x | **4.64x** |
| Weekday peak hours | **4.27x** | 1.09x | 0.50x | 0.96x | **2.19x** |
| Monthly demand trends | **4.06x** | **1.37x** | 0.58x | 1.04x | **2.15x** |

### 1. Silver versus Gold preaggregation

Silver calculates from detailed trip data in `integrated_taxi_trips`; Gold reuses saved aggregations (`daily_mobility_summary`, `taxi_zone_statistics`, `weather_impact_summary`, and `air_quality_impact_summary`) to produce the same answers.

| Analysis Query | Silver (ms) | Gold (ms) | Speedup | Scope |
|---|---:|---:|---:|---|
| Monthly zone demand | 934.9 | 293.0 | **3.19x** | Full period (9.39M trips) |
| Weather and distance | 564.5 | 274.3 | **2.06x** | Full period (9.39M trips) |
| Air quality and demand | 1188.9 | 228.0 | **5.21x** | Full period (9.39M trips) |
| Weather demand variation | 4149.5 | 374.9 | **11.07x** | Full period (9.39M trips) |
| Weekday peak hours | 1468.1 | 343.9 | **4.27x** | Full period (9.39M trips) |
| Monthly demand trends | 1626.8 | 400.2 | **4.06x** | Full period (9.39M trips) |

### 2. In-memory table caching

The projected `trips` view was cached in memory and disk (`CACHE TABLE trips OPTIONS ('storageLevel' 'MEMORY_AND_DISK')`) and forced into cache via an initial count action (taking 10.85 seconds). All six queries were executed against the cached relation.

| Analysis Query | Baseline Silver (ms) | Cached (ms) | Speedup | Scope |
|---|---:|---:|---:|---|
| Monthly zone demand | 934.9 | 1483.6 | 0.63x | Full period, cached `trips` |
| Weather and distance | 564.5 | 514.2 | **1.10x** | Full period, cached `trips` |
| Air quality and demand | 1188.9 | 504.3 | **2.36x** | Full period, cached `trips` |
| Weather demand variation | 4149.5 | 5426.2 | 0.76x | Full period, cached `trips` |
| Weekday peak hours | 1468.1 | 1343.3 | **1.09x** | Full period, cached `trips` |
| Monthly demand trends | 1626.8 | 1190.4 | **1.37x** | Full period, cached `trips` |

### 3. Adaptive Query Execution (AQE) and shuffle coalescing

Adaptive Query Execution and dynamic partition coalescing were enabled (`spark.sql.adaptive.enabled=true`, `spark.sql.adaptive.coalescePartitions.enabled=true`), with automatic broadcasting disabled to isolate the impact of runtime shuffle partition coalescing across all six queries.

| Analysis Query | Baseline (ms) | AQE Enabled (ms) | Speedup | Scope |
|---|---:|---:|---:|---|
| Monthly zone demand | 934.9 | 970.5 | 0.96x | Full period, AQE coalescing |
| Weather and distance | 564.5 | 542.7 | **1.04x** | Full period, AQE coalescing |
| Air quality and demand | 1188.9 | 1116.5 | **1.06x** | Full period, AQE coalescing |
| Weather demand variation | 4149.5 | 3329.9 | **1.25x** | Full period, AQE coalescing |
| Weekday peak hours | 1468.1 | 2916.3 | 0.50x | Full period, AQE coalescing |
| Monthly demand trends | 1626.8 | 2826.6 | 0.58x | Full period, AQE coalescing |

### 4. Explicit partition pruning

Queries bounded by date range predicates alone were compared against queries with added explicit year/month partition column predicates over February 2024 (and January 2024 for `monthly_demand_trends` to supply the preceding-month lag window).

| Analysis Query | Date Filter Alone (ms) | Explicit Partition Filter (ms) | Speedup | Scope |
|---|---:|---:|---:|---|
| Monthly zone demand | 1839.2 | 1781.8 | **1.03x** | February 2024 window |
| Weather and distance | 1257.1 | 1327.9 | 0.95x | February 2024 window |
| Air quality and demand | 729.6 | 770.2 | 0.95x | February 2024 window |
| Weather demand variation | 2232.3 | 2370.5 | 0.94x | February 2024 window |
| Weekday peak hours | 769.7 | 798.4 | 0.96x | February 2024 window |
| Monthly demand trends | 844.7 | 811.9 | **1.04x** | Jan–Feb 2024 window |

### 5. Broadcast hash join

Clean taxi trips were joined to the zone lookup and a cached table containing weather and PM2.5 values for each UTC hour. The hourly lookup was extracted from integrated trips and took 2.47 seconds to prepare. Unhinted sort-merge joins were compared against broadcast-hinted joins (`/*+ BROADCAST(z, c) */`) across all six queries.

| Analysis Query | No Broadcast (ms) | Broadcast Hint (ms) | Speedup | Scope |
|---|---:|---:|---:|---|
| Monthly zone demand | 3857.3 | 1250.2 | **3.09x** | Full period, clean trips join |
| Weather and distance | 5959.2 | 3253.6 | **1.83x** | Full period, clean trips join |
| Air quality and demand | 5834.5 | 2613.8 | **2.23x** | Full period, clean trips join |
| Weather demand variation | 29955.3 | 6462.3 | **4.64x** | Full period, clean trips join |
| Weekday peak hours | 3003.1 | 1370.9 | **2.19x** | Full period, clean trips join |
| Monthly demand trends | 2666.9 | 1238.3 | **2.15x** | Full period, clean trips join |

<!-- pagebreak -->

## Analysis of EXPLAIN FORMATTED plans

Physical execution plans captured before and after optimization across all queries clarify the operational mechanisms driving these results:

**Gold preaggregation.** Across all six queries, Silver execution plans scan the entire 9.39M-row `integrated_taxi_trips` Delta table, executing columnar parquet scans, expression projections, and multi-stage distributed hash aggregations. Gold plans replace this scan with direct reads from compact summary tables: `daily_mobility_summary` (714 rows), `taxi_zone_statistics` (773 rows), `weather_impact_summary` (786 rows), and `air_quality_impact_summary` (2,183 rows). In `weather_demand_variation`, Silver scans 9.39M trips across multiple branches to derive wet/dry exposure hours and zone rates, whereas Gold queries the 786-row `weather_impact_summary` and calculates rate differences directly, yielding an **11.07x speedup** (4,149.5 ms down to 374.9 ms).

**In-memory table caching.** The plans contain `Scan In-memory table trips` and `InMemoryRelation`, confirming cache use. Air-quality demand improves by **2.36x**, but monthly zone demand and weather-demand variation regress to **0.63x** and **0.76x**. Caching reuses the input projection; aggregation and join work remains. Spark SQL supports columnar caching, so the storage-level label alone does not establish an uncompressed row representation. Cache access costs, memory pressure and warm Parquet reads are possible explanations, not measured causes. No GC, spill or cache-residency metrics were recorded. Cached weather-demand variation samples span 3.15–6.34 seconds, showing substantial variability. [Spark caching documentation](https://spark.apache.org/docs/3.5.5/sql-performance-tuning.html#caching-data-in-memory).

**Adaptive Query Execution (AQE).** The executed plans contain `AdaptiveSparkPlan` with `isFinalPlan=true` and coalesced `AQEShuffleRead` operators. Weather-demand variation improves by **1.25x**, while weekday peaks and monthly trends regress to **0.50x** and **0.58x**. Adaptive planning overhead or reduced useful parallelism after coalescing could outweigh savings in this local, eight-partition setup. These are hypotheses: the plans confirm adaptation, but do not attribute time to coordination or quantify the cause of the regressions.

**Explicit partition pruning.** Baseline plans contain pushed-down date filters; optimized plans additionally contain year/month `PartitionFilters`. Pruning was therefore applied. Speedups of **0.94x–1.04x**, with overlapping sample ranges, show no clear additional latency benefit in this run. The table was already partitioned on both sides. Delta can skip files using available column statistics, and Parquet filtering can further limit reads. However, `PushedFilters` does not prove how many files were skipped. We did not record scan bytes or file counts, so equal physical I/O remains an explanation to test, not a measured result. [Delta data-skipping documentation](https://docs.delta.io/optimizations-oss/).

**Broadcast hash join.** The unhinted baseline executed `SortMergeJoin LeftOuter`, requiring both sides to perform `Exchange hashpartitioning` and sorting on join keys. Shuffling and sorting 9.39M trip records across shuffle partitions dominated join latency. The hinted plans substituted `BroadcastHashJoin LeftOuter BuildRight`, broadcasting the zone and hourly context lookups via `BroadcastExchange`. This removes the large-side shuffle and sort for the joins converted to broadcast; other joins and aggregations still require exchanges. In `weather_demand_variation`, eliminating multiple join shuffles reduced latency from 29,955.3 ms to 6,462.3 ms (**4.64x speedup**). Across all six analytical queries, broadcasting achieved consistent **1.83x to 4.64x speedups**.

## Performance discussion and trade-offs

Gold reduced latency by **2.06x–11.07x**, and broadcast joins improved the reconstructed-input comparisons by **1.83x–4.64x**. Broadcast timings must be compared within their own pairs: they include rebuilding enriched input, whereas integrated Silver already stores that enrichment.

Construction of all four Gold products took **23.40 seconds**, producing **114,319 bytes of active data files**, excluding transaction logs and retained versions. Summing the six query medians gives approximately 9.93 seconds for Silver versus 1.91 seconds for Gold. At that workload mix, construction is recovered after roughly three complete six-query rounds; this is an estimate from separate medians, not a timed batch or a universal per-query break-even point.

Caching cost **10.85 seconds** to populate and must earn that cost back through repeated savings. The broadcast context lookup cost **2.47 seconds** and was shared by both join variants. Its preparation belongs in total workload cost; the hint comparison alone does not establish an end-to-end break-even point.

### Which optimization had little or no effect? Why?

**Explicit partition pruning had the least effect**, with speedups between 0.94x and 1.04x. Both variants already requested the same dates from a partitioned Delta table. Existing date-based file skipping and Parquet filtering may leave little extra work for explicit partition predicates to remove. Small differences are consistent with run variability, rather than clear evidence of improvement or harm.

Caching had only modest gains for weather-distance and weekday peaks, and regressed for two other queries. AQE barely changed monthly zone, weather-distance and air-quality latency, helped weather-demand variation, and substantially slowed weekday peaks and monthly trends. Optimizations change execution costs; they do not guarantee acceleration. The exact causes of these regressions require stage, memory and spill metrics.

### What characteristics of the data explain these results?

The input has **9.39 million trips but only three months of history**. Many trips share zones, hours and weather categories, so aggregation reduces millions of rows to hundreds or a few thousand. This supports the strong Gold results. Small zone and hourly context lookups relative to the trip table make broadcasting suitable. Repeated analytical branches make avoiding enrichment joins especially valuable for weather-demand variation.

The short history contains only three monthly partitions: February queries select one, and trends selects two. Monthly organization also makes date ranges useful for file skipping when date statistics are available. These characteristics help explain why adding explicit partition predicates may contribute little. Local execution, eight shuffle partitions, fixed run order and warm caches further limit how these timings generalize.

### What changes would we recommend for ten cities?

Use city-qualified keys, city-specific environmental joins and local calendars while retaining UTC timestamps. Standardize measurement units and track coverage per city. Evaluate city/year/month partitioning against common filters and actual partition sizes, avoiding tiny files.

Use distributed execution when workload size or concurrency requires it, and retune shuffle counts, AQE and broadcast thresholds from observed stage metrics. Refresh only affected city/date slices and dependent summaries, handle late corrections, and publish consistent refresh versions with freshness monitoring. Materialize repeated aggregations; cache selected reused inputs rather than assuming the entire trip history fits in memory. The [design report](design_report.md#5-engineering-decisions-and-trade-offs) explains these choices and their trade-offs.
