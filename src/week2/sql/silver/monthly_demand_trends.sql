WITH months AS (
    SELECT
        trunc(local_date, 'month') AS month,
        COUNT(DISTINCT local_date) AS observed_days,
        SUM(trip_count) AS trip_count
    FROM hourly_demand
    GROUP BY trunc(local_date, 'month')
),
changes AS (
    SELECT
        *,
        lag(trip_count) OVER (ORDER BY month) AS previous_month_trips
    FROM months
)
SELECT
    *,
    try_divide(trip_count, observed_days) AS avg_daily_demand,
    100.0 * try_divide(
        trip_count - previous_month_trips,
        previous_month_trips
    ) AS change_pct
FROM changes
ORDER BY month
