SELECT 1
FROM trips
WHERE pickup_date >= DATE '{month_start}'
    AND pickup_date < add_months(DATE '{month_start}', 1)
LIMIT 1
