SELECT
    weather,
    SUM(trip_count) AS trip_count,
    SUM(distance_count) AS distance_count,
    try_divide(SUM(distance_sum), SUM(distance_count)) AS avg_distance
FROM gold_weather_impact_summary
GROUP BY weather
HAVING SUM(trip_count) > 0
ORDER BY weather
