SELECT *
FROM benchmark_original_trips
WHERE pickup_date >= DATE '{start}'
    AND pickup_date < DATE '{end}'
    {partition_filter}
