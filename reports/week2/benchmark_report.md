# Week 2: benchmark report

## Methodology and evidence

This report evaluates optimization techniques using benchmark run `20260917_161256_727995` from 17 September 2026 on **9,394,330 trips** spanning 1 January to 31 March 2024. The recorded execution environment is Apache Spark 4.0.1 running on `local[*]` with eight shuffle partitions. Because execution occurred on a local development workstation, these timings illustrate architectural and execution plan trade-offs rather than portable production targets.

As required by the assignment, **every analytical query is compared before and after optimization** across all five optimization strategies: Gold preaggregation, in-memory table caching, Adaptive Query Execution (AQE) with shuffle coalescing, explicit partition pruning, and broadcast hash joins.

The benchmark evaluates 48 distinct cases (6 analytical queries across 8 experimental configurations). Each case runs one warmup execution followed by five timed iterations, yielding 240 measured query executions. Timings measure the entire latency including SQL planning, stage execution, and driver result collection (`collect()`). Result correctness validation, physical plan extraction, Gold product materialization, and cache population are excluded from query timing. Every optimized run was verified for semantic equivalence against its baseline answer, asserting exact key matching, identical row counts and null distributions, and strict floating-point equality (`rel_tol=1e-9`, `abs_tol=1e-8`).

### Baseline design across the five optimizations

To maintain scientific rigor, not every technique compares against the initial full-period Silver table:
- **Initial Silver baseline (`_silver`)**: Used directly for **Gold preaggregation**, **In-memory caching**, and **AQE**. All three run against `integrated_taxi_trips` over the entire 3-month dataset.
- **Scoped baseline for Partition Pruning (`_date_filter`)**: Evaluated on February 2024 using date range predicates alone (`pickup_date >= '2024-02-01' AND pickup_date < '2024-03-01'`). Comparing a 1-month partition-pruned query against the 3-month Silver baseline would confound data volume reduction with partition-skipping effectiveness. Holding the date window identical isolates whether Catalyst explicit partition filters offer incremental gain over Delta date skipping.
- **Join baseline for Broadcast Join (`_no_broadcast`)**: Evaluated by joining clean taxi trips with zone and hourly weather/PM2.5 lookup tables using unhinted sort-merge joins (`SortMergeJoin`). Because `integrated_taxi_trips` is already denormalized and pre-joined during ingestion, it contains no joins to broadcast. Reconstructing the join on clean tables and comparing unhinted joins against broadcast-hinted joins (`/*+ BROADCAST */`) isolates the true impact of broadcast hash joins.

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

Clean taxi trips (9.39M rows) were joined to the taxi zone lookup (265 rows) and a materialized hourly weather/PM2.5 context lookup (2,184 rows, population cost 2.47 seconds). Unhinted sort-merge joins were compared against broadcast-hinted joins (`/*+ BROADCAST(z, c) */`) across all six queries.

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

**In-memory table caching.** The cached plans confirm that `Scan In-memory table trips` backed by `InMemoryRelation` replaced the Parquet scans across all queries. This eliminates Parquet column decoding and date truncations on subsequent queries. For queries with selective column reads and timestamp parsing, such as `air_quality_demand` (**2.36x speedup**, 1,188.9 ms to 504.3 ms) and `monthly_demand_trends` (**1.37x speedup**, 1,626.8 ms to 1,190.4 ms), caching yields substantial performance gains. However, for queries requiring wide projection scans or large aggregation buffers—such as `monthly_zone_demand` (0.63x) and `weather_demand_variation` (0.76x)—scanning deserialized in-memory rows introduced higher GC overhead and memory bus pressure than Spark's native vectorized Parquet reader. Furthermore, the 10.85-second cache population cost must be amortized across queries.

**Adaptive Query Execution (AQE).** The executed plans reveal `AdaptiveSparkPlan` with `isFinalPlan=true` and dynamic `AQEShuffleRead (Arguments: coalesced)` inserted before downstream stages. In `weather_demand_variation`, runtime statistics allowed Spark to coalesce empty and small shuffle partitions following complex joins, producing a **1.25x speedup** (4,149.5 ms to 3,329.9 ms). Conversely, for queries with multiple sequential stages and window operators—most notably `weekday_peak_hours` (0.50x) and `monthly_demand_trends` (0.58x)—the runtime coordination overhead of adaptive stage barriers and dynamic re-planning on a local 8-partition environment exceeded the marginal benefit of coalescing already tiny intermediate partitions.

**Explicit partition pruning.** Baseline plans pushed date range filters into the scan (`PushedFilters: [GreaterThanOrEqual(pickup_date, ...), LessThan(pickup_date, ...)]`), whereas optimized plans added explicit `PartitionFilters: [(pickup_year = 2024), (pickup_month = 2)]`. Because Delta Lake automatically maintains min/max date statistics per Parquet data file in its transaction log, date range predicates already skipped non-target data files during file planning. Explicit partition filters confirmed partition-level pruning in the Catalyst plan, but achieved virtually identical physical I/O, resulting in speedup factors between 0.94x and 1.04x across all six queries (within normal measurement variance).

**Broadcast hash join.** The unhinted baseline executed `SortMergeJoin LeftOuter`, requiring both sides to perform `Exchange hashpartitioning` and sorting on join keys. Shuffling and sorting 9.39M trip records across shuffle partitions dominated join latency. The hinted plans substituted `BroadcastHashJoin LeftOuter BuildRight`, broadcasting the 265-row zone table and 2,184-row hourly context table via `BroadcastExchange`. This eliminated the large-table shuffle exchange and sort entirely. In `weather_demand_variation`, eliminating multiple join shuffles reduced latency from 29,955.3 ms to 6,462.3 ms (**4.64x speedup**). Across all six analytical queries, broadcasting achieved consistent **1.83x to 4.64x speedups**.

## Performance discussion and trade-offs

The full benchmark matrix demonstrates a clear performance and architectural hierarchy:

1. **Preaggregation (Gold layer) is the most impactful architectural investment.** It delivered major speedups across all queries (2.06x to 11.07x). Constructing all four Gold tables took **23.40 seconds** in total and produced **114,319 bytes** of compact Delta storage. Because queries against Gold execute in 200–400 ms instead of 1–4 seconds, the build investment is amortized after fewer than 10 repeated analytical executions.
2. **Broadcast joins provide decisive speedups whenever un-denormalized sources are joined.** For dimensional joins against small lookups, broadcast joins eliminated expensive shuffle exchanges on 9.39M rows, achieving speedups of 1.83x to 4.64x across every query. Materializing the hourly context lookup required 2.47 seconds, an overhead recovered on the first analytical execution.
3. **In-memory caching is query-dependent rather than a universal accelerator.** Caching the 9.39M-row `trips` view required 10.85 seconds. While it accelerated `air_quality_demand` (2.36x) and `monthly_demand_trends` (1.37x), it caused regressions on `monthly_zone_demand` (0.63x) and `weather_demand_variation` (0.76x). Uncompressed in-memory representations can underperform vectorized Parquet readers on wide scans; caching is best reserved for compact lookup tables or heavily filtered query paths.
4. **Adaptive Query Execution requires appropriate stage scale.** AQE provided tangible gains on queries with heavy shuffle joins (`weather_demand_variation` at 1.25x), but added execution barrier latency on multi-window queries (`weekday_peak_hours` at 0.50x). In larger distributed deployments with hundreds of partitions, AQE's ability to coalesce partitions and mitigate skew becomes substantially more beneficial.
5. **Delta Lake data skipping reduces the necessity of explicit partition predicates.** Because Delta Lake tracks column min/max statistics automatically, date range filters prune irrelevant files at the transaction log level. Explicit partition columns remain best practice for catalog metadata clarity and query planning, but do not provide additional speedup over well-indexed columnar ranges at this dataset scale.
