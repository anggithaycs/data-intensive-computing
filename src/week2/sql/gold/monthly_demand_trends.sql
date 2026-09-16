WITH months AS (
    SELECT
        trunc(local_date, 'month') AS month,
        COUNT(DISTINCT local_date) AS observed_days
    FROM calendar_hours
    GROUP BY trunc(local_date, 'month')
),
totals AS (
    SELECT
        trunc(pickup_date, 'month') AS month,
        SUM(trip_count) AS trip_count
    FROM gold_daily_mobility_summary
    GROUP BY trunc(pickup_date, 'month')
),
changes AS (
    SELECT
        m.month,
        m.observed_days,
        coalesce(t.trip_count, 0L) AS trip_count,
        lag(coalesce(t.trip_count, 0L)) OVER (
            ORDER BY m.month
        ) AS previous_month_trips
    FROM months m
    LEFT JOIN totals t
        ON m.month = t.month
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
