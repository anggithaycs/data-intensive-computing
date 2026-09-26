# Week 3 Task 2: Maintain Analytical Consistency

## 1. Objective

The objective is to keep Week 2 analytical queries and data products valid and up to date after incremental data arrival and schema evolution, while minimizing unnecessary recomputation. The work preserved the frozen Week 2 baseline, extended the integrated Taxi dataset with accepted Task 1 data, reran the existing queries, and refreshed the four Gold products with incremental or affected-scope strategies.

## 2. Week 2 Baseline

The Week 2 analytical layer contains six queries:

- Monthly taxi demand by zone (`monthly_zone_demand`)
- Average trip distance by weather (`weather_trip_distance`)
- Air quality and taxi demand relationship (`air_quality_demand`)
- Taxi-zone demand variation by weather (`weather_demand_variation`)
- Peak travel hours by weekday (`weekday_peak_hours`)
- Monthly taxi-demand trends (`monthly_demand_trends`)

It also contains four Gold analytical products: Daily Mobility Summary, Taxi Zone Statistics, Weather Impact Summary, and Air Quality Impact Summary.

The frozen integrated baseline was `integrated_taxi_trips` Delta version **0**, with **9,394,330 rows** covering pickup dates **2024-01-01 through 2024-03-31**. The baseline query and product evidence came from Week 2 run `20260925_085344_238641`; that run used one measured repeat. The baseline was not rerun.

Baseline source: [`task2_baseline.json`](../../storage/metrics/week3/task2_baseline.json).

## 3. Incremental Integration

The current Task 1 Silver Taxi data was compared with the integrated baseline using a left anti-join on the Taxi business key `trip_id`. The integration reused the existing Week 1 pipeline’s context joins and performed an insert-only Delta `MERGE`:

- Baseline: **9,394,330 rows**, Delta version **0**
- Newly integrated Taxi trips: **659,476**
- Final integrated table: **10,053,806 rows**, Delta version **1**
- New pickup-date range: **2024-04-02 through 2025-01-07**
- Measured integration execution time: **344.928 seconds**
- Duplicate trip IDs ignored and rejected/unsupported rows: **0** each

Matched Taxi records were preserved; existing historical integrated rows were not updated. Context matching reused existing semantics: weather was matched to the containing UTC pickup/dropoff hour; air quality used the exact UTC hour and borough PM2.5 with citywide fallback; and pickup/dropoff zone lookups remained left joins. Of the new trips, **659,007** had pickup weather and air-quality context, while **469** lacked pickup air-quality context. Dropoff context matched **659,004** trips, with **472** lacking dropoff air-quality context. All new pickup and dropoff location IDs mapped to zones.

Weather and Air Quality Task 1 observations covered the update window from **2025-01-01 02:00** through **2025-01-08 01:00** (168 distinct timestamps in each source). Their context availability was limited to matching observations; the taxi updates themselves extended from April 2024 through January 2025.

Integration evidence: [`task2_integration_refresh.json`](../../storage/metrics/week3/task2_integration_refresh.json).

## 4. Analytical Query Consistency

All six unchanged Week 2 queries executed successfully against integrated Delta version 1. Every query result changed, and all retained compatible output schemas.

| Query | Baseline Rows | Updated Rows | Changed? | Compatible? |
|---|---:|---:|---|---|
| Monthly zone demand | 773 | 2,916 | Yes | Yes |
| Weather trip distance | 3 | 3 | Yes | Yes |
| Air quality demand | 1 | 1 | Yes | Yes |
| Weather demand variation | 262 | 262 | Yes | Yes |
| Weekday peak hours | 168 | 168 | Yes | Yes |
| Monthly demand trends | 3 | 13 | Yes | Yes |

The updated table added demand observations over an expanded date range. Some queries gained new groups (monthly zone demand and monthly trends), while other queries retained the same number of output groups but changed aggregate values or rankings. The existing Week 2 SQL does not directly reference the new `humidity` and `aqi` columns, so those additions did not break query execution or output compatibility.

Query evidence: [`query_comparison.json`](../../storage/metrics/week3/query_comparison.json).

## 5. Analytical Product Refresh Strategy

| Product | Strategy | Why |
|---|---|---|
| Daily Mobility Summary | Incremental | Additive daily/borough counts, sums, and non-null counts; the 1,726 new groups were after the baseline and had no overlapping baseline groups. |
| Taxi Zone Statistics | Incremental | Additive month/zone aggregates; the 2,143 new groups had no overlapping baseline groups. |
| Weather Impact Summary | Partial recomputation | New trips add demand aggregates, but exposure-hour counts change by weather category across zones. The implementation recomposed the affected zone/weather output rather than simply appending rows. |
| Air Quality Impact Summary | Partial recomputation | The hourly product was recomputed for the newly covered calendar interval and appended; the 6,769 new hourly rows did not overlap baseline hours. |

No product was classified as requiring complete recomputation for its incremental maintenance strategy.

## 6. Product Correctness

Each incrementally refreshed Gold product was validated against a full-result reference using the existing Week 2 product SQL/views. In this Step 5 validation, all four products matched their full-result references: schemas were compatible, and each had **zero missing keys, zero unexpected keys, and zero value mismatches**.

Step 6 then performed a separate full-refresh benchmark: the existing Week 2 SQL/views were executed against integrated Delta version 1 and written as actual Delta outputs at temporary staging paths. Those staged outputs were also compared with the current Step 5 Gold tables. All four had matching row counts and schemas, with **zero missing keys, zero unexpected keys, and zero value mismatches**:

| Product | Week 2 Baseline Rows | Incremental Gold Rows | Full-Refresh Rows | Correctness |
|---|---:|---:|---:|---|
| Daily Mobility Summary | 714 | 2,440 | 2,440 | Equivalent |
| Taxi Zone Statistics | 773 | 2,916 | 2,916 | Equivalent |
| Weather Impact Summary | 786 | 786 | 786 | Equivalent |
| Air Quality Impact Summary | 2,183 | 8,952 | 8,952 | Equivalent |

The Step 5 in-memory reference timings are not the full-refresh benchmark timings reported below. Step 6 used actual staged Delta writes and removed those temporary outputs after validation.

## 7. Incremental vs Full Refresh Performance

| Product | Incremental (s) | Full (s) | Refresh Savings | Speedup |
|---|---:|---:|---:|---:|
| Daily Mobility | 4.225 | 3.092007 | -36.6427% | 0.731836× |
| Taxi Zone | 3.391 | 2.064224 | -64.2748% | 0.608736× |
| Weather Impact | 10.249 | 4.350660 | -135.5734% | 0.424496× |
| Air Quality Impact | 3.202 | 3.114062 | -2.8239% | 0.972536× |

Refresh savings are negative in this experiment because incremental Delta `MERGE` overhead exceeded the computation saved at this data scale. This is not a failure: it demonstrates a trade-off in the measured workload. Incremental refresh reduced the logical recomputation scope, but Delta merge/write overhead could dominate for these relatively small outputs. The scalability benefit may become more meaningful as data volume and product size increase; that possibility was not measured here.

The full-refresh timer covered execution of the product SQL and materialization of a Delta output at a staging path, following the Week 2 builder’s `coalesce(1)` and overwrite behavior. Post-write row-count and equivalence checks were outside the timed interval. Incremental times are the actual measured Step 5 refresh durations. The benchmark used one measured execution per product, not a multi-run statistical benchmark.

Benchmark evidence: [`analytical_refresh.json`](../../storage/metrics/week3/analytical_refresh.json).

## 8. Schema Evolution

Task 1 added `humidity` to Weather Silver and `aqi` to Air Quality Silver. The existing integration projection did not propagate these fields into `integrated_taxi_trips`; it retained the established relative-humidity and PM2.5 context fields used by the Week 2 layer. The integrated schema remained unchanged.

The Week 2 queries and four Gold products do not directly use the new `humidity` or `aqi` fields. The existing queries therefore remained compatible, and the Gold product schemas did not need extension. This compatibility was achieved by leaving the new source fields in their Silver tables and preserving existing projections and SQL—not by automatic propagation into the analytical products.

If a future query or product explicitly depends on `humidity` or `aqi`, its source projection, SQL, and output schema would need to be deliberately updated and validated. No such change was needed for this Task 2 refresh.

## 9. Discussion

### Which analytical data products can be refreshed incrementally?

Daily Mobility Summary and Taxi Zone Statistics can be incrementally maintained for the new dates/months because their stored counts, sums, and non-null counts are additive and the new groups did not overlap baseline groups.

### Which require complete recomputation?

No product required complete recomputation in this implementation. Weather Impact Summary and Air Quality Impact Summary used partial recomputation of the affected zone/weather rows or newly covered hours, respectively.

### Which schema changes can be handled automatically?

The existing queries and products remained compatible while `humidity` and `aqi` stayed in their source Silver tables and were not added to the integrated/product schemas. That was the actual handling used; the fields were not automatically incorporated into Gold outputs.

### Which schema changes require manual intervention?

A future analytical product that explicitly depends on a new source field would require an intentional query/projection change and a corresponding output-schema decision. This task did not implement or test such a dependency.

### How does the design reduce unnecessary computation?

- The integrated table uses a business-key anti-join and insert-only Delta `MERGE` to add new Taxi rows while preserving historical records.
- Additive daily and month/zone products insert only new aggregate groups.
- Weather Impact recomputes its affected zone/weather aggregates and exposures rather than rebuilding unrelated analytical layers.
- Air Quality Impact recomputes only the new hourly calendar interval.
- Existing unaffected historical product groups are preserved.
- Both the Step 5 full-result references and Step 6 staged full-refresh outputs were used to validate the maintained products.

## 10. Limitations and Trade-offs

- The measurements were taken in a local environment at the assignment’s data and product scale; they should not be generalized to other cluster sizes or larger production workloads.
- Incremental Delta `MERGE` overhead exceeded full-refresh time for all four measured products in this single experiment.
- Each product was measured once; no repeated-run distribution or statistical comparison was collected.
- New Weather and Air Quality observations covered only the final 168-timestamp window from 2025-01-01 02:00 through 2025-01-08 01:00. Context for new trips therefore depended on the existing exact-hour matching rules and available environmental observations.
- Existing Week 2 queries do not directly use `humidity` or `aqi`; this task validates compatibility, not analytical use of those fields.

## 11. Evidence Files

- [`task2_baseline.json`](../../storage/metrics/week3/task2_baseline.json)
- [`task2_integration_refresh.json`](../../storage/metrics/week3/task2_integration_refresh.json)
- [`query_comparison.json`](../../storage/metrics/week3/query_comparison.json)
- [`product_comparison.json`](../../storage/metrics/week3/product_comparison.json)
- [`analytical_refresh.json`](../../storage/metrics/week3/analytical_refresh.json)

## 12. Files Created / Modified

Task 2 evidence and report:

- `storage/metrics/week3/task2_baseline.json`
- `storage/metrics/week3/task2_integration_refresh.json`
- `storage/metrics/week3/query_comparison.json`
- `storage/metrics/week3/product_comparison.json`
- `storage/metrics/week3/analytical_refresh.json`
- `reports/week3/task2.md`

Task 2 implementation scripts:

- `scripts/run_week3_task2_incremental_integration.py`
- `scripts/run_week3_task2_incremental_products.py`
- `scripts/run_week3_task2_full_refresh_benchmark.py`

No Week 2 SQL or Task 1 files were modified for this report.
