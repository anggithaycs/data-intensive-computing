SELECT {hint}
    t.pickup_datetime,
    t.pickup_date,
    t.pickup_year,
    t.pickup_month,
    t.pickup_location_id,
    coalesce(z.zone, 'Unknown') AS pickup_zone,
    coalesce(z.borough, 'Unknown') AS pickup_borough,
    t.trip_distance,
    t.trip_duration_minutes,
    t.fare_amount,
    c.pm25 AS pickup_pm25_citywide,
    date_trunc('hour', t.pickup_datetime) AS hour_utc,
    coalesce(c.weather, 'unknown') AS weather
FROM clean_trips t
LEFT JOIN zones z ON t.pickup_location_id = z.location_id
LEFT JOIN benchmark_hourly_context c
    ON date_trunc('hour', t.pickup_datetime) = c.hour_utc
