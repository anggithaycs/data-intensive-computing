SELECT
    hour_utc,
    MAX(pickup_pm25_citywide) AS pm25,
    MAX(weather) AS weather
FROM benchmark_original_trips
GROUP BY hour_utc
