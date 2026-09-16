SELECT
    pickup_location_id,
    COUNT(*) AS trip_count
FROM trips
WHERE pickup_date >= DATE '{month_start}'
    AND pickup_date < add_months(DATE '{month_start}', 1)
    {partition_filter}
GROUP BY pickup_location_id
ORDER BY pickup_location_id
