# spark_jobs/jobs/conform_trips.py
"""
Spark Conform Job — Phase 3 core logic.

Flow:
1. Đọc raw Parquet từ GCS (không có schema cứng — đọc tự do)
2. Lowercase + rename cột về snake_case chuẩn
3. Cast type về chuẩn (long cho integer columns)
4. Fill null cho cột xuất hiện theo thời gian
   (congestion_surcharge, airport_fee, cbd_congestion_fee)
5. Validate rules → tách valid / invalid
6. Broadcast join với zone lookup (tường minh dùng broadcast() hint)
7. Thêm metadata columns (service_type, pickup_date, ingestion_date, source_file)
8. Ghi valid → nyc_taxi_conformed.trips
9. Ghi invalid → nyc_taxi_quarantine.rejected_trips
"""

import argparse
import logging
from datetime import date

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DoubleType, DateType

from spark_jobs.schemas.trip_schema import COLUMN_RENAME_MAP, CONFORMED_SCHEMA
from spark_jobs.validations.rules import is_all_valid, get_reason_code

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ── Hằng số ─────────────────────────────────────────────────

# Cột integer cần cast về LongType để thống nhất giữa các năm
INTEGER_COLS_TO_LONG = [
    "vendor_id",
    "passenger_count",
    "pu_location_id",
    "do_location_id",
    "ratecode_id",
    "payment_type",
]

# Cột xuất hiện theo thời gian — fill null nếu file không có
OPTIONAL_COLS = [
    "congestion_surcharge",
    "airport_fee",
    "cbd_congestion_fee",
]


# ── Step functions ───────────────────────────────────────────


def read_raw(spark: SparkSession, gcs_path: str) -> DataFrame:
    """Đọc raw Parquet từ GCS, không enforce schema."""
    logger.info(f"Reading raw parquet: {gcs_path}")
    return spark.read.parquet(gcs_path)


def normalize_columns(df: DataFrame) -> DataFrame:
    """
    Bước 1: lowercase toàn bộ tên cột.
    Bước 2: rename theo COLUMN_RENAME_MAP (xử lý VendorID→vendor_id, tpep_→pickup_datetime, v.v.)
    Thứ tự quan trọng: lowercase trước để gom airport_fee + Airport_fee về 1 trước khi rename.
    """
    # Lowercase toàn bộ
    df = df.toDF(*[c.lower() for c in df.columns])

    # Rename theo mapping
    for old_name, new_name in COLUMN_RENAME_MAP.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    return df


def cast_types(df: DataFrame) -> DataFrame:
    """Cast integer columns về LongType để thống nhất giữa file 2011 (long) và 2024 (integer)."""
    for col_name in INTEGER_COLS_TO_LONG:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(LongType()))
    return df


def fill_optional_cols(df: DataFrame) -> DataFrame:
    """
    Thêm các cột xuất hiện theo thời gian nếu file không có.
    VD: file 2011 không có congestion_surcharge → thêm cột null thay vì báo lỗi.
    """
    for col_name in OPTIONAL_COLS:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast(DoubleType()))
    return df


def split_valid_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Tách DataFrame thành 2:
    - valid: record pass tất cả rules
    - invalid: record vi phạm ít nhất 1 rule, kèm reason_code
    """
    valid_df = df.filter(is_all_valid())
    invalid_df = df.filter(~is_all_valid()).withColumn(
        "reason_code", get_reason_code(df)
    )
    return valid_df, invalid_df


def join_zone_lookup(df: DataFrame, zone_df: DataFrame) -> DataFrame:
    """
    Broadcast join với zone lookup để thêm borough/zone name.
    Dùng broadcast() hint tường minh — không để Spark tự quyết định join strategy.
    Zone lookup nhỏ (~265 rows) → broadcast an toàn, tránh shuffle.
    Join 2 lần: 1 cho pickup, 1 cho dropoff.
    """
    zone_pu = zone_df.select(
        F.col("LocationID").alias("pu_location_id"),
        F.col("Borough").alias("pu_borough"),
        F.col("Zone").alias("pu_zone"),
    )
    zone_do = zone_df.select(
        F.col("LocationID").alias("do_location_id"),
        F.col("Borough").alias("do_borough"),
        F.col("Zone").alias("do_zone"),
    )

    df = df.join(F.broadcast(zone_pu), on="pu_location_id", how="left")
    df = df.join(F.broadcast(zone_do), on="do_location_id", how="left")

    return df


def add_metadata(
    df: DataFrame,
    service_type: str,
    source_file: str,
    ingestion_date: date,
) -> DataFrame:
    """Thêm các cột metadata cần thiết cho BigQuery."""
    return (
        df.withColumn("service_type", F.lit(service_type))
        .withColumn("pickup_date", F.col("pickup_datetime").cast(DateType()))
        .withColumn("ingestion_date", F.lit(str(ingestion_date)).cast(DateType()))
        .withColumn("source_file", F.lit(source_file))
    )


def select_conformed_cols(df: DataFrame) -> DataFrame:
    """Select đúng các cột theo CONFORMED_SCHEMA, theo đúng thứ tự."""
    cols = [
        field.name
        for field in CONFORMED_SCHEMA.fields
        if field.name not in ("reason_code", "reason_detail")
    ]
    return df.select(*cols)


def select_quarantine_cols(df: DataFrame) -> DataFrame:
    """Select các cột cho quarantine table."""
    quarantine_cols = [
        "vendor_id",
        "service_type",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "pu_location_id",
        "do_location_id",
        "fare_amount",
        "total_amount",
        "reason_code",
        "ingestion_date",
        "source_file",
    ]
    return df.select(*quarantine_cols)


# ── Main ─────────────────────────────────────────────────────


def run(
    spark: SparkSession,
    gcs_raw_path: str,
    zone_lookup_path: str,
    bq_conformed_table: str,
    bq_quarantine_table: str,
    service_type: str,
    ingestion_date: date,
    gcs_temp_bucket: str,
) -> dict:
    """
    Chạy toàn bộ conform pipeline cho 1 file trip data.

    Returns:
        dict với row counts để Airflow sanity check
    """
    source_file = gcs_raw_path.split("/")[-1]

    # 1. Đọc raw
    df = read_raw(spark, gcs_raw_path)
    raw_count = df.count()
    logger.info(f"Raw row count: {raw_count:,}")

    # 2. Normalize + cast + fill optional
    df = normalize_columns(df)
    df = cast_types(df)
    df = fill_optional_cols(df)

    # 3. Tách valid / invalid
    valid_df, invalid_df = split_valid_invalid(df)

    # 4. Join zone lookup (chỉ valid records cần join)
    zone_df = spark.read.csv(zone_lookup_path, header=True, inferSchema=True)
    valid_df = join_zone_lookup(valid_df, zone_df)

    # 5. Thêm metadata
    valid_df = add_metadata(valid_df, service_type, source_file, ingestion_date)
    invalid_df = (
        invalid_df.withColumn("service_type", F.lit(service_type))
        .withColumn("ingestion_date", F.lit(str(ingestion_date)).cast(DateType()))
        .withColumn("source_file", F.lit(source_file))
    )

    # 6. Select đúng cột
    valid_df = select_conformed_cols(valid_df)
    invalid_df = select_quarantine_cols(invalid_df)

    # 7. Đếm để log + return
    valid_count = valid_df.count()
    invalid_count = invalid_df.count()
    logger.info(
        f"Valid: {valid_count:,} | Invalid: {invalid_count:,} | "
        f"Reject rate: {invalid_count/raw_count*100:.2f}%"
    )

    # 8. Ghi BigQuery
    logger.info(f"Writing conformed → {bq_conformed_table}")
    (
        valid_df.write.format("bigquery")
        .option("table", bq_conformed_table)
        .option("temporaryGcsBucket", gcs_temp_bucket)
        .option("partitionField", "pickup_date")
        .option("clusteredFields", "service_type,pu_borough")
        .mode("append")
        .save()
    )

    logger.info(f"Writing quarantine → {bq_quarantine_table}")
    (
        invalid_df.write.format("bigquery")
        .option("table", bq_quarantine_table)
        .option("temporaryGcsBucket", gcs_temp_bucket)
        .option("partitionField", "ingestion_date")
        .mode("append")
        .save()
    )

    return {
        "raw_count": raw_count,
        "valid_count": valid_count,
        "invalid_count": invalid_count,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NYC Taxi Conform Job")
    parser.add_argument("--gcs-raw-path", required=True)
    parser.add_argument("--zone-lookup-path", required=True)
    parser.add_argument("--bq-conformed-table", required=True)
    parser.add_argument("--bq-quarantine-table", required=True)
    parser.add_argument("--service-type", default="yellow")
    parser.add_argument("--ingestion-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--gcs-temp-bucket", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("nyc_taxi_conform").getOrCreate()

    run(
        spark=spark,
        gcs_raw_path=args.gcs_raw_path,
        zone_lookup_path=args.zone_lookup_path,
        bq_conformed_table=args.bq_conformed_table,
        bq_quarantine_table=args.bq_quarantine_table,
        service_type=args.service_type,
        ingestion_date=date.fromisoformat(args.ingestion_date),
        gcs_temp_bucket=args.gcs_temp_bucket,
    )

    spark.stop()
