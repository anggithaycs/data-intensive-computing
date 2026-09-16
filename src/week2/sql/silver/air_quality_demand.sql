SELECT
    COUNT(*) AS total_hours,
    COUNT(pm25) AS matched_hours,
    CASE
        WHEN COUNT(pm25) >= 2
            AND stddev_pop(pm25) > 0
            AND stddev_pop(
                CASE WHEN pm25 IS NOT NULL THEN trip_count END
            ) > 0
        THEN corr(pm25, CAST(trip_count AS DOUBLE))
    END AS correlation
FROM hourly_demand
