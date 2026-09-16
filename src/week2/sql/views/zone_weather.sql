WITH zones AS (
    SELECT DISTINCT
        pickup_location_id,
        pickup_zone
    FROM trips
),
exposure AS (
    SELECT
        weather,
        COUNT(*) AS observed_hours
    FROM hourly_demand
    GROUP BY weather
),
totals AS (
    SELECT
        pickup_location_id,
        weather,
        COUNT(*) AS trip_count,
        SUM(trip_distance) AS distance_sum,
        COUNT(trip_distance) AS distance_count
    FROM trips
    GROUP BY pickup_location_id, weather
)
SELECT
    z.pickup_location_id,
    z.pickup_zone,
    e.weather,
    e.observed_hours,
    coalesce(t.trip_count, 0L) AS trip_count,
    coalesce(t.distance_sum, 0.0D) AS distance_sum,
    coalesce(t.distance_count, 0L) AS distance_count
FROM zones z
CROSS JOIN exposure e
LEFT JOIN totals t
    ON z.pickup_location_id = t.pickup_location_id
    AND e.weather = t.weather
