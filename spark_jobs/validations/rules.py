# spark_jobs/validations/rules.py
from pyspark.sql import functions as F
from pyspark.sql.column import Column

REASON_NEGATIVE_FARE = "negative_fare"
REASON_SUSPICIOUS_DISTANCE = "suspicious_distance"
REASON_INVALID_DURATION = "invalid_duration"
REASON_DURATION_GT_24H = "duration_gt_24h"
REASON_INVALID_LOCATION_ID = "invalid_location_id"
REASON_NULL_DATETIME = "null_datetime"

MAX_TRIP_DISTANCE_MILES = 500.0
MAX_TRIP_DURATION_MINUTES = 1440
VALID_LOCATION_ID_MIN = 1
VALID_LOCATION_ID_MAX = 265


def is_valid_fare() -> Column:
    return F.col("fare_amount") >= 0


def is_valid_distance() -> Column:
    return (F.col("trip_distance") >= 0) & (
        F.col("trip_distance") <= MAX_TRIP_DISTANCE_MILES
    )


def is_valid_duration() -> Column:
    duration_minutes = (
        F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")
    ) / 60
    return (duration_minutes > 0) & (duration_minutes <= MAX_TRIP_DURATION_MINUTES)


def is_valid_location_id() -> Column:
    valid_pu = F.col("pu_location_id").between(
        VALID_LOCATION_ID_MIN, VALID_LOCATION_ID_MAX
    )
    valid_do = F.col("do_location_id").between(
        VALID_LOCATION_ID_MIN, VALID_LOCATION_ID_MAX
    )
    return valid_pu & valid_do


def is_not_null_datetime() -> Column:
    return F.col("pickup_datetime").isNotNull() & F.col("dropoff_datetime").isNotNull()


def is_all_valid() -> Column:
    return (
        is_not_null_datetime()
        & is_valid_fare()
        & is_valid_duration()
        & is_valid_distance()
        & is_valid_location_id()
    )


def get_reason_code(df=None) -> Column:
    return (
        F.when(~is_not_null_datetime(), REASON_NULL_DATETIME)
        .when(~is_valid_fare(), REASON_NEGATIVE_FARE)
        .when(~is_valid_duration(), REASON_INVALID_DURATION)
        .when(~is_valid_distance(), REASON_SUSPICIOUS_DISTANCE)
        .when(~is_valid_location_id(), REASON_INVALID_LOCATION_ID)
        .otherwise(None)
    )
