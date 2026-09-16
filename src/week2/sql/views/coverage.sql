SELECT
    COUNT(*) AS trip_count,
    MIN(pickup_date) AS first_date,
    MAX(pickup_date) AS last_date,
    COUNT(DISTINCT pickup_date) AS days_with_trips,
    SUM(
        CASE WHEN weather = 'unknown' THEN 1 ELSE 0 END
    ) AS unknown_weather_trips
FROM trips
