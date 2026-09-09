# Week 1 Architecture

```mermaid
flowchart TD
    Config["YAML: paths, rules, timezones, partitions, Spark settings"] --> Runner[Week 1 runner]
    Taxi[Taxi Parquet] --> Engine[Generic ingestion lifecycle]
    Weather[Weather CSV] --> Engine
    Air[EPA CSV] --> Engine
    Zones[Zone CSV] --> Engine
    Runner --> Preflight[Resolve all input, rule and integration schemas before writes]
    Preflight --> Engine
    Engine --> Bronze[Bronze Delta source snapshots]
    Engine --> Scope[Account for geographical scope]
    Scope --> Normalize[Rename, safely cast, normalize UTC timestamps]
    Normalize --> Validate[Null-safe validation and duplicate ranking]
    Validate --> Reject[Per-dataset quarantine Delta tables]
    Validate --> Silver[Four clean Silver Delta tables]
    Validate --> Audit[Row accounting and run metadata]
    Silver --> Join[Left broadcast joins by zone and containing UTC hour]
    Silver --> SiteHour[Average instruments within physical site and UTC hour]
    SiteHour --> BoroughHour[Equal-site borough-hour mean]
    SiteHour --> CityHour[Equal-site available-NYC hour mean]
    BoroughHour --> BoroughJoin[Parallel left join: endpoint borough and UTC hour]
    CityHour --> CityJoin[Parallel left join: endpoint UTC hour]
    Join --> Context[Retain separate borough and citywide estimates]
    BoroughJoin --> Context
    CityJoin --> Context
    Context --> Fallback[Convenience PM2.5 fallback plus source flag]
    Fallback --> Check[Assert trip ID uniqueness and exact set preservation]
    Check --> Integrated[Integrated taxi trips Delta table]
    Taxi --> Benchmark[Independent raw-to-clean layout writes and repeated equivalent queries]
    Silver --> ZoneRef[Clean zone lookup for borough queries]
    ZoneRef --> Benchmark
    Benchmark --> Flat[Unpartitioned]
    Benchmark --> Daily[Daily partitions]
    Benchmark --> Monthly[Monthly partitions]
    Benchmark --> Metrics[JSON timings, variability, file sizes and counts]
    Audit --> Metrics
```

The integrated dataset remains in the configured Silver directory. Quarantine
is append-only across runs; Bronze and Silver are replaced by full runs.
Benchmark output paths include a run ID. Diagrams deliberately omit fixed row
and file counts because they change when data or validation rules change.

The active YAML partitions taxi and integrated tables by local pickup year/month.
Small clean context tables are unpartitioned. The benchmark rereads raw taxi input;
it does not use the integrated table as its timed input. A standalone diagram is
available in `architecture.pdf` and `architecture.png`.

`pickup_pm25_borough` stays null without local coverage. `pickup_pm25_citywide`
is the same available-site estimate for every pickup in that UTC hour and is
null if no sites report. The convenience fallback and source flag are separate;
analogous dropoff columns use dropoff time and borough.
