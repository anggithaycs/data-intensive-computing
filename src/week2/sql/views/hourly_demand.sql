WITH counts AS (
    SELECT
        hour_utc,
        COUNT(*) AS trip_count,
        MAX(pickup_pm25_citywide) AS pm25,
        MAX(weather) AS weather
    FROM trips
    GROUP BY hour_utc
)
SELECT
    c.*,
    coalesce(t.trip_count, 0L) AS trip_count,
    t.pm25,
    coalesce(t.weather, 'unknown') AS weather
FROM calendar_hours c
LEFT JOIN counts t
    ON c.hour_utc = t.hour_utc
