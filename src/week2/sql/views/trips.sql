SELECT
    pickup_datetime,
    pickup_date,
    pickup_year,
    pickup_month,
    pickup_location_id,
    pickup_zone,
    pickup_borough,
    trip_distance,
    trip_duration_minutes,
    fare_amount,
    pickup_pm25_citywide,
    date_trunc('hour', pickup_datetime) AS hour_utc,
    CASE
        WHEN pickup_weather_matched AND pickup_precipitation_mm > 0 THEN 'wet'
        WHEN pickup_weather_matched AND pickup_precipitation_mm = 0 THEN 'dry'
        ELSE 'unknown'
    END AS weather
FROM integrated_trips
