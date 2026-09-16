SELECT {hint}
    trunc(t.pickup_date, 'month') AS month,
    t.pickup_location_id,
    coalesce(z.zone, 'Unknown') AS pickup_zone,
    coalesce(z.borough, 'Unknown') AS pickup_borough,
    COUNT(*) AS trip_count
FROM clean_trips t
LEFT JOIN zones z
    ON t.pickup_location_id = z.location_id
GROUP BY
    trunc(t.pickup_date, 'month'),
    t.pickup_location_id,
    coalesce(z.zone, 'Unknown'),
    coalesce(z.borough, 'Unknown')
ORDER BY month, pickup_location_id
