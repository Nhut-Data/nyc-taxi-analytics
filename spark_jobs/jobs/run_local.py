# spark_jobs/jobs/run_local.py
"""
Integration test local — đọc file local thay vì GCS, ghi output ra /tmp thay vì BQ.
Mục đích: verify pipeline logic end-to-end trước khi submit Dataproc.
"""

from datetime import date
from pyspark.sql import SparkSession
from spark_jobs.jobs.conform_trips import (
    read_raw,
    normalize_columns,
    cast_types,
    fill_optional_cols,
    split_valid_invalid,
    join_zone_lookup,
    add_metadata,
    select_conformed_cols,
    select_quarantine_cols,
)

LOCAL_TRIP_FILE = "data/raw/yellow_tripdata_2024-01.parquet"
LOCAL_ZONE_FILE = "data/raw/taxi_zone_lookup.csv"
OUTPUT_VALID = "/tmp/nyc_taxi_valid"
OUTPUT_INVALID = "/tmp/nyc_taxi_invalid"

spark = (
    SparkSession.builder.appName("nyc_taxi_conform_local_test")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Chạy từng bước — dễ debug nếu fail
print("Step 1: read raw")
df = read_raw(spark, LOCAL_TRIP_FILE)
raw_count = df.count()
print(f"  raw rows: {raw_count:,}")
print(f"  columns : {df.columns}")

print("\nStep 2: normalize columns")
df = normalize_columns(df)
print(f"  columns after normalize: {df.columns}")

print("\nStep 3: cast types")
df = cast_types(df)

print("\nStep 4: fill optional cols")
df = fill_optional_cols(df)

print("\nStep 5: split valid/invalid")
valid_df, invalid_df = split_valid_invalid(df)
valid_count = valid_df.count()
invalid_count = invalid_df.count()
print(f"  valid  : {valid_count:,}")
print(f"  invalid: {invalid_count:,}")
print(f"  reject rate: {invalid_count/raw_count*100:.2f}%")

print("\nStep 6: join zone lookup")
zone_df = spark.read.csv(LOCAL_ZONE_FILE, header=True, inferSchema=True)
valid_df = join_zone_lookup(valid_df, zone_df)

print("\nStep 7: add metadata")
valid_df = add_metadata(
    valid_df, "yellow", "yellow_tripdata_2024-01.parquet", date(2024, 1, 1)
)
invalid_df = (
    invalid_df.withColumn(
        "service_type",
        __import__("pyspark.sql.functions", fromlist=["lit"]).lit("yellow"),
    )
    .withColumn(
        "ingestion_date",
        __import__("pyspark.sql.functions", fromlist=["lit"])
        .lit("2024-01-01")
        .cast("date"),
    )
    .withColumn(
        "source_file",
        __import__("pyspark.sql.functions", fromlist=["lit"]).lit(
            "yellow_tripdata_2024-01.parquet"
        ),
    )
)

print("\nStep 8: select final cols")
valid_df = select_conformed_cols(valid_df)
invalid_df = select_quarantine_cols(invalid_df)

print("\nStep 9: write output (local parquet)")
valid_df.write.mode("overwrite").parquet(OUTPUT_VALID)
invalid_df.write.mode("overwrite").parquet(OUTPUT_INVALID)

print("\nStep 10: verify output")
out_valid = spark.read.parquet(OUTPUT_VALID)
out_invalid = spark.read.parquet(OUTPUT_INVALID)
print(f"  conformed rows : {out_valid.count():,}")
print(f"  quarantine rows: {out_invalid.count():,}")
print("\n  conformed schema:")
out_valid.printSchema()
print("\n  sample quarantine reason_codes:")
out_invalid.groupBy("reason_code").count().show()

spark.stop()
print("\nDONE — local integration test passed")
