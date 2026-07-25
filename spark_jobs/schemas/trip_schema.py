# spark_jobs/schemas/trip_schema.py
"""
StructType tường minh cho từng service_type.
Mục đích: fix schema drift (INT64 vs INTEGER, airport_fee vs Airport_fee)
khi đọc Parquet từ nhiều năm khác nhau.

Nguyên tắc:
- Dùng LongType cho tất cả integer column (long an toàn hơn integer)
- Lowercase toàn bộ tên cột
- Các cột mới (congestion_surcharge, airport_fee, cbd_congestion_fee)
  khai báo optional=True — Spark fill null nếu file không có
"""

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

# Schema chuẩn cho Yellow taxi
# Tên cột đã lowercase, type đã chuẩn hóa
YELLOW_SCHEMA = StructType([
    StructField("vendor_id",             LongType(),         nullable=True),
    StructField("tpep_pickup_datetime",  TimestampNTZType(), nullable=True),
    StructField("tpep_dropoff_datetime", TimestampNTZType(), nullable=True),
    StructField("passenger_count",       LongType(),         nullable=True),
    StructField("trip_distance",         DoubleType(),       nullable=True),
    StructField("ratecode_id",           LongType(),         nullable=True),
    StructField("store_and_fwd_flag",    StringType(),       nullable=True),
    StructField("pu_location_id",        LongType(),         nullable=True),
    StructField("do_location_id",        LongType(),         nullable=True),
    StructField("payment_type",          LongType(),         nullable=True),
    StructField("fare_amount",           DoubleType(),       nullable=True),
    StructField("extra",                 DoubleType(),       nullable=True),
    StructField("mta_tax",              DoubleType(),       nullable=True),
    StructField("tip_amount",            DoubleType(),       nullable=True),
    StructField("tolls_amount",          DoubleType(),       nullable=True),
    StructField("improvement_surcharge", DoubleType(),       nullable=True),
    StructField("total_amount",          DoubleType(),       nullable=True),
    # Cột xuất hiện theo thời gian — nullable=True để không lỗi khi đọc file cũ
    StructField("congestion_surcharge",  DoubleType(),       nullable=True),
    StructField("airport_fee",           DoubleType(),       nullable=True),
    StructField("cbd_congestion_fee",    DoubleType(),       nullable=True),
])


def get_schema(service_type: str) -> StructType:
    """
    Trả về schema tương ứng với service_type.
    Hiện tại chỉ support yellow — mở rộng sau.
    """
    schemas = {
        "yellow": YELLOW_SCHEMA,
    }
    if service_type not in schemas:
        raise ValueError(
            f"Unsupported service_type '{service_type}'. "
            f"Supported: {list(schemas.keys())}"
        )
    return schemas[service_type]