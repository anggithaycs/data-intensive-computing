WITH averages AS (
    SELECT
        weekday,
        local_hour,
        COUNT(*) AS hour_occurrences,
        AVG(trip_count) AS avg_demand
    FROM gold_air_quality_impact_summary
    GROUP BY weekday, local_hour
)
SELECT
    *,
    dense_rank() OVER (
        PARTITION BY weekday
        ORDER BY avg_demand DESC
    ) AS demand_rank
FROM averages
ORDER BY weekday, demand_rank, local_hour
