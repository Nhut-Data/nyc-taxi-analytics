# tests/spark/test_validations.py

import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    DoubleType,
    StringType,
    TimestampType,
)

from spark_jobs.validations.rules import (
    is_valid_fare,
    is_valid_distance,
    is_valid_duration,
    is_valid_location_id,
    is_all_valid,
    get_reason_code,
    REASON_NEGATIVE_FARE,
    REASON_SUSPICIOUS_DISTANCE,
)
from spark_jobs.jobs.conform_trips import (
    normalize_columns,
    fill_optional_cols,
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("nyc_taxi_test")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def ts(s):
    """Helper: string → datetime object cho TimestampType."""
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def make_trip_df(spark, rows: list[dict]):
    schema = StructType(
        [
            StructField("fare_amount", DoubleType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("pickup_datetime", TimestampType(), True),
            StructField("dropoff_datetime", TimestampType(), True),
            StructField("pu_location_id", LongType(), True),
            StructField("do_location_id", LongType(), True),
        ]
    )
    return spark.createDataFrame(rows, schema)


# ── TestIsValidFare ───────────────────────────────────────────


class TestIsValidFare:
    def test_positive_fare_is_valid(self, spark):
        df = make_trip_df(
            spark,
            [
                {
                    "fare_amount": 10.5,
                    "trip_distance": 2.0,
                    "pickup_datetime": None,
                    "dropoff_datetime": None,
                    "pu_location_id": 1,
                    "do_location_id": 2,
                }
            ],
        )
        assert df.filter(is_valid_fare()).count() == 1

    def test_zero_fare_is_valid(self, spark):
        df = make_trip_df(
            spark,
            [
                {
                    "fare_amount": 0.0,
                    "trip_distance": 0.0,
                    "pickup_datetime": None,
                    "dropoff_datetime": None,
                    "pu_location_id": 1,
                    "do_location_id": 2,
                }
            ],
        )
        assert df.filter(is_valid_fare()).count() == 1

    def test_negative_fare_is_invalid(self, spark):
        df = make_trip_df(
            spark,
            [
                {
                    "fare_amount": -5.0,
                    "trip_distance": 2.0,
                    "pickup_datetime": None,
                    "dropoff_datetime": None,
                    "pu_location_id": 1,
                    "do_location_id": 2,
                }
            ],
        )
        assert df.filter(is_valid_fare()).count() == 0


# ── TestIsValidDistance ───────────────────────────────────────


class TestIsValidDistance:
    def test_normal_distance_is_valid(self, spark):
        df = make_trip_df(
            spark,
            [
                {
                    "fare_amount": 10.0,
                    "trip_distance": 5.0,
                    "pickup_datetime": None,
                    "dropoff_datetime": None,
                    "pu_location_id": 1,
                    "do_location_id": 2,
                }
            ],
        )
        assert df.filter(is_valid_distance()).count() == 1

    def test_zero_distance_is_valid(self, spark):
        df = make_trip_df(
            spark,
            [
                {
                    "fare_amount": 2.5,
                    "trip_distance": 0.0,
                    "pickup_datetime": None,
                    "dropoff_datetime": None,
                    "pu_location_id": 1,
                    "do_location_id": 2,
                }
            ],
        )
        assert df.filter(is_valid_distance()).count() == 1

    def test_negative_distance_is_invalid(self, spark):
        df = make_trip_df(
            spark,
            [
                {
                    "fare_amount": 10.0,
                    "trip_distance": -1.0,
                    "pickup_datetime": None,
                    "dropoff_datetime": None,
                    "pu_location_id": 1,
                    "do_location_id": 2,
                }
            ],
        )
        assert df.filter(is_valid_distance()).count() == 0

    def test_extreme_distance_is_invalid(self, spark):
        df = make_trip_df(
            spark,
            [
                {
                    "fare_amount": 10.0,
                    "trip_distance": 312722.0,
                    "pickup_datetime": None,
                    "dropoff_datetime": None,
                    "pu_location_id": 1,
                    "do_location_id": 2,
                }
            ],
        )
        assert df.filter(is_valid_distance()).count() == 0


# ── TestIsValidDuration ───────────────────────────────────────


class TestIsValidDuration:
    def test_normal_duration_is_valid(self, spark):
        df = make_trip_df(
            spark,
            [
                {
                    "fare_amount": 10.0,
                    "trip_distance": 2.0,
                    "pickup_datetime": ts("2024-01-01 08:00:00"),
                    "dropoff_datetime": ts("2024-01-01 08:15:00"),
                    "pu_location_id": 1,
                    "do_location_id": 2,
                }
            ],
        )
        assert df.filter(is_valid_duration()).count() == 1

    def test_zero_duration_is_invalid(self, spark):
        df = make_trip_df(
            spark,
            [
                {
                    "fare_amount": 10.0,
                    "trip_distance": 2.0,
                    "pickup_datetime": ts("2024-01-01 08:00:00"),
                    "dropoff_datetime": ts("2024-01-01 08:00:00"),
                    "pu_location_id": 1,
                    "do_location_id": 2,
                }
            ],
        )
        assert df.filter(is_valid_duration()).count() == 0

    def test_negative_duration_is_invalid(self, spark):
        df = make_trip_df(
            spark,
            [
                {
                    "fare_amount": 10.0,
                    "trip_distance": 2.0,
                    "pickup_datetime": ts("2024-01-01 08:00:00"),
                    "dropoff_datetime": ts("2024-01-01 07:00:00"),
                    "pu_location_id": 1,
                    "do_location_id": 2,
                }
            ],
        )
        assert df.filter(is_valid_duration()).count() == 0

    def test_duration_gt_24h_is_invalid(self, spark):
        df = make_trip_df(
            spark,
            [
                {
                    "fare_amount": 10.0,
                    "trip_distance": 2.0,
                    "pickup_datetime": ts("2024-01-01 08:00:00"),
                    "dropoff_datetime": ts("2024-01-03 08:00:00"),
                    "pu_location_id": 1,
                    "do_location_id": 2,
                }
            ],
        )
        assert df.filter(is_valid_duration()).count() == 0


# ── TestIsValidLocationId ─────────────────────────────────────


class TestIsValidLocationId:
    def test_valid_location_ids(self, spark):
        df = make_trip_df(
            spark,
            [
                {
                    "fare_amount": 10.0,
                    "trip_distance": 2.0,
                    "pickup_datetime": None,
                    "dropoff_datetime": None,
                    "pu_location_id": 132,
                    "do_location_id": 236,
                }
            ],
        )
        assert df.filter(is_valid_location_id()).count() == 1

    def test_location_id_out_of_range(self, spark):
        df = make_trip_df(
            spark,
            [
                {
                    "fare_amount": 10.0,
                    "trip_distance": 2.0,
                    "pickup_datetime": None,
                    "dropoff_datetime": None,
                    "pu_location_id": 999,
                    "do_location_id": 1,
                }
            ],
        )
        assert df.filter(is_valid_location_id()).count() == 0

    def test_location_id_zero_is_invalid(self, spark):
        df = make_trip_df(
            spark,
            [
                {
                    "fare_amount": 10.0,
                    "trip_distance": 2.0,
                    "pickup_datetime": None,
                    "dropoff_datetime": None,
                    "pu_location_id": 0,
                    "do_location_id": 1,
                }
            ],
        )
        assert df.filter(is_valid_location_id()).count() == 0


# ── TestReasonCode ────────────────────────────────────────────


class TestReasonCode:
    def test_negative_fare_gets_correct_reason(self, spark):
        df = make_trip_df(
            spark,
            [
                {
                    "fare_amount": -10.0,
                    "trip_distance": 2.0,
                    "pickup_datetime": ts("2024-01-01 08:00:00"),
                    "dropoff_datetime": ts("2024-01-01 08:15:00"),
                    "pu_location_id": 1,
                    "do_location_id": 2,
                }
            ],
        )
        result = (
            df.filter(~is_all_valid())
            .withColumn("reason_code", get_reason_code())
            .select("reason_code")
            .collect()[0]["reason_code"]
        )
        assert result == REASON_NEGATIVE_FARE

    def test_extreme_distance_gets_correct_reason(self, spark):
        df = make_trip_df(
            spark,
            [
                {
                    "fare_amount": 10.0,
                    "trip_distance": 99999.0,
                    "pickup_datetime": ts("2024-01-01 08:00:00"),
                    "dropoff_datetime": ts("2024-01-01 08:15:00"),
                    "pu_location_id": 1,
                    "do_location_id": 2,
                }
            ],
        )
        result = (
            df.filter(~is_all_valid())
            .withColumn("reason_code", get_reason_code())
            .select("reason_code")
            .collect()[0]["reason_code"]
        )
        assert result == REASON_SUSPICIOUS_DISTANCE


# ── TestNormalizeColumns ──────────────────────────────────────


class TestNormalizeColumns:
    def test_lowercase_and_rename(self, spark):
        schema = StructType(
            [
                StructField("VendorID", LongType(), True),
                StructField("Airport_fee", DoubleType(), True),
                StructField("tpep_pickup_datetime", StringType(), True),
            ]
        )
        df = spark.createDataFrame([(1, 2.5, "2024-01-01")], schema)
        result = normalize_columns(df)

        assert "vendor_id" in result.columns
        assert "airport_fee" in result.columns
        assert "pickup_datetime" in result.columns
        assert "VendorID" not in result.columns
        assert "Airport_fee" not in result.columns


# ── TestFillOptionalCols ──────────────────────────────────────


class TestFillOptionalCols:
    def test_missing_cols_filled_with_null(self, spark):
        schema = StructType(
            [
                StructField("vendor_id", LongType(), True),
                StructField("fare_amount", DoubleType(), True),
            ]
        )
        df = spark.createDataFrame([(1, 10.0)], schema)
        result = fill_optional_cols(df)

        assert "congestion_surcharge" in result.columns
        assert "airport_fee" in result.columns
        assert "cbd_congestion_fee" in result.columns
        assert result.select("congestion_surcharge").collect()[0][0] is None
