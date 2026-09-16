SELECT
    month,
    pickup_location_id,
    pickup_zone,
    pickup_borough,
    trip_count
FROM gold_taxi_zone_statistics
ORDER BY month, pickup_location_id
