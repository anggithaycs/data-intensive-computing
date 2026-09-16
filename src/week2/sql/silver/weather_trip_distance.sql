SELECT
    weather,
    COUNT(*) AS trip_count,
    COUNT(trip_distance) AS distance_count,
    AVG(trip_distance) AS avg_distance
FROM trips
GROUP BY weather
ORDER BY weather
