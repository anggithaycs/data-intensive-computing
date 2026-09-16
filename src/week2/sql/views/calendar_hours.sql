SELECT
    hour_utc,
    CAST(
        from_utc_timestamp(hour_utc, 'America/New_York') AS DATE
    ) AS local_date,
    weekday(from_utc_timestamp(hour_utc, 'America/New_York')) + 1 AS weekday,
    hour(from_utc_timestamp(hour_utc, 'America/New_York')) AS local_hour
FROM (
    SELECT
        explode(
            sequence(
                to_utc_timestamp(
                    TIMESTAMP '{start} 00:00:00',
                    'America/New_York'
                ),
                to_utc_timestamp(
                    CAST(date_add(DATE '{end}', 1) AS TIMESTAMP),
                    'America/New_York'
                ) - INTERVAL 1 HOUR,
                INTERVAL 1 HOUR
            )
        ) AS hour_utc
)
