SELECT
    trunc(pickup_date, 'month') AS month,
    pickup_location_id,
    pickup_zone,
    pickup_borough,
    COUNT(*) AS trip_count,
    SUM(trip_distance) AS distance_sum,
    COUNT(trip_distance) AS distance_count,
    SUM(fare_amount) AS fare_sum,
    COUNT(fare_amount) AS fare_count
FROM trips
GROUP BY
    trunc(pickup_date, 'month'),
    pickup_location_id,
    pickup_zone,
    pickup_borough
