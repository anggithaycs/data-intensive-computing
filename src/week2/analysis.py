from src.week2.sql_files import read_sql

# These identify rows when comparing answers, regardless of output ordering.
QUERY_KEYS = {
    "monthly_zone_demand": ["month", "pickup_location_id"],
    "weather_trip_distance": ["weather"],
    "air_quality_demand": [],  # One row describes the whole period.
    "weather_demand_variation": ["pickup_location_id"],
    "weekday_peak_hours": ["weekday", "local_hour"],
    "monthly_demand_trends": ["month"],
}

QUERIES = {name: read_sql(f"silver/{name}.sql") for name in QUERY_KEYS}


def prepare_views(spark, integrated):
    """Register the input and shared views used by the analytical queries.
    """

    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Use a UTC Spark session, as configured in Week 1")

    integrated.createOrReplaceTempView("integrated_trips")
    spark.sql(read_sql("views/trips.sql")).createOrReplaceTempView("trips")

    coverage = spark.sql(read_sql("views/coverage.sql")).first().asDict()
    if not coverage["trip_count"] or coverage["first_date"] is None:
        raise ValueError("Run Week 1 first: the integrated input is empty")

    prepare_derived_views(spark, coverage)
    return coverage


def prepare_derived_views(spark, coverage):
    """Rebuild dependent views after changing the trips input or date scope."""

    calendar_sql = read_sql("views/calendar_hours.sql").format(
        start=coverage["first_date"],
        end=coverage["last_date"],
    )
    spark.sql(calendar_sql).createOrReplaceTempView("calendar_hours")

    for name in ("hourly_demand", "zone_weather"):
        spark.sql(read_sql(f"views/{name}.sql")).createOrReplaceTempView(name)
