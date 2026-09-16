SELECT
    pickup_date,
    pickup_borough,
    COUNT(*) AS trip_count,
    SUM(trip_distance) AS distance_sum,
    COUNT(trip_distance) AS distance_count,
    SUM(trip_duration_minutes) AS duration_sum,
    COUNT(trip_duration_minutes) AS duration_count,
    SUM(fare_amount) AS fare_sum,
    COUNT(fare_amount) AS fare_count
FROM trips
GROUP BY pickup_date, pickup_borough
