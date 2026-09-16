WITH rates AS (
    SELECT
        pickup_location_id,
        pickup_zone,
        try_divide(trip_count, observed_hours) AS demand_per_hour,
        observed_hours
    FROM gold_weather_impact_summary
    WHERE weather IN ('dry', 'wet')
),
variation AS (
    SELECT
        pickup_location_id,
        pickup_zone,
        MAX(demand_per_hour) - MIN(demand_per_hour) AS demand_range,
        MIN(observed_hours) AS minimum_exposure_hours
    FROM rates
    GROUP BY pickup_location_id, pickup_zone
    HAVING COUNT(*) = 2
)
SELECT
    *,
    dense_rank() OVER (ORDER BY demand_range DESC) AS demand_rank
FROM variation
ORDER BY demand_rank, pickup_location_id
