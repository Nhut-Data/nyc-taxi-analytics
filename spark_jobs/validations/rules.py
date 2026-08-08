# spark_jobs/validations/rules.py
from pyspark.sql import functions as F
from pyspark.sql.column import Column

REASON_INVALID_FARE = "invalid_fare"
REASON_SUSPICIOUS_DISTANCE = "suspicious_distance"
REASON_INVALID_DURATION = "invalid_duration"
REASON_DURATION_GT_24H = "duration_gt_24h"
REASON_INVALID_LOCATION_ID = "invalid_location_id"
REASON_NULL_DATETIME = "null_datetime"
REASON_WRONG_PERIOD = "wrong_period"

MAX_TRIP_DISTANCE_MILES = 500.0
MAX_TRIP_DURATION_MINUTES = 1440
VALID_LOCATION_ID_MIN = 1
VALID_LOCATION_ID_MAX = 265
MIN_FARE_AMOUNT = 2.5  # gia mo cua toi thieu luat dinh NYC (~$3), chua bien an toan


def is_valid_fare() -> Column:
    return F.col("fare_amount") >= MIN_FARE_AMOUNT


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


def is_valid_period(expected_year: int = None, expected_month: int = None) -> Column:
    """
    Kiem tra pickup_datetime co nam dung trong thang file dang xu ly khong.
    Chan rac du lieu vendor nhu pickup_datetime=2002-12-31 trong file 2024-01.
    Neu khong truyen expected_year/expected_month, luon tra ve True (bo qua
    check) - giu tuong thich nguoc cho unit test goi is_all_valid() khong
    tham so.
    """
    if expected_year is None or expected_month is None:
        return F.lit(True)
    return (F.year(F.col("pickup_datetime")) == expected_year) & (
        F.month(F.col("pickup_datetime")) == expected_month
    )


def is_all_valid(expected_year: int = None, expected_month: int = None) -> Column:
    return (
        is_not_null_datetime()
        & is_valid_fare()
        & is_valid_duration()
        & is_valid_distance()
        & is_valid_location_id()
        & is_valid_period(expected_year, expected_month)
    )


def get_reason_code(expected_year: int = None, expected_month: int = None) -> Column:
    return (
        F.when(~is_not_null_datetime(), REASON_NULL_DATETIME)
        .when(~is_valid_period(expected_year, expected_month), REASON_WRONG_PERIOD)
        .when(~is_valid_fare(), REASON_INVALID_FARE)
        .when(~is_valid_duration(), REASON_INVALID_DURATION)
        .when(~is_valid_distance(), REASON_SUSPICIOUS_DISTANCE)
        .when(~is_valid_location_id(), REASON_INVALID_LOCATION_ID)
        .otherwise(None)
    )
