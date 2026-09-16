SELECT
    trunc(pickup_date, 'month') AS month,
    pickup_location_id,
    pickup_zone,
    pickup_borough,
    COUNT(*) AS trip_count
FROM trips
GROUP BY
    trunc(pickup_date, 'month'),
    pickup_location_id,
    pickup_zone,
    pickup_borough
ORDER BY month, pickup_location_id
