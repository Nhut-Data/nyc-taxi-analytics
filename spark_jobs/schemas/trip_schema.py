# spark_jobs/schemas/trip_schema.py

from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

COLUMN_RENAME_MAP = {
    "vendorid": "vendor_id",
    "pulocationid": "pu_location_id",
    "dolocationid": "do_location_id",
    "ratecodeid": "ratecode_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "lpep_pickup_datetime": "pickup_datetime",
    "lpep_dropoff_datetime": "dropoff_datetime",
}

CONFORMED_SCHEMA = StructType(
    [
        StructField("vendor_id", LongType(), nullable=True),
        StructField("service_type", StringType(), nullable=False),
        StructField("pickup_datetime", TimestampNTZType(), nullable=True),
        StructField("dropoff_datetime", TimestampNTZType(), nullable=True),
        StructField("pickup_date", DateType(), nullable=False),
        StructField("passenger_count", LongType(), nullable=True),
        StructField("trip_distance", DoubleType(), nullable=True),
        StructField("pu_location_id", LongType(), nullable=True),
        StructField("do_location_id", LongType(), nullable=True),
        StructField("pu_borough", StringType(), nullable=True),
        StructField("pu_zone", StringType(), nullable=True),
        StructField("do_borough", StringType(), nullable=True),
        StructField("do_zone", StringType(), nullable=True),
        StructField("ratecode_id", LongType(), nullable=True),
        StructField("store_and_fwd_flag", StringType(), nullable=True),
        StructField("payment_type", LongType(), nullable=True),
        StructField("fare_amount", DoubleType(), nullable=True),
        StructField("extra", DoubleType(), nullable=True),
        StructField("mta_tax", DoubleType(), nullable=True),
        StructField("tip_amount", DoubleType(), nullable=True),
        StructField("tolls_amount", DoubleType(), nullable=True),
        StructField("improvement_surcharge", DoubleType(), nullable=True),
        StructField("total_amount", DoubleType(), nullable=True),
        StructField("congestion_surcharge", DoubleType(), nullable=True),
        StructField("airport_fee", DoubleType(), nullable=True),
        StructField("cbd_congestion_fee", DoubleType(), nullable=True),
        StructField("ingestion_date", DateType(), nullable=False),
        StructField("source_file", StringType(), nullable=True),
    ]
)
