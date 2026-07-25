-- ============================================================
-- infra/bq_setup.sql
-- Tạo bảng đích cho NYC Taxi Analytics Platform
-- Project: banking-data-platform-500108
-- Chạy 1 lần để setup, không chạy lại (có IF NOT EXISTS để safe)
-- ============================================================

-- ============================================================
-- 1. nyc_taxi_conformed.trips
--    Partition by pickup_date (DATE) → filter theo thời gian hiệu quả
--    Cluster by service_type, borough → 2 dimension lọc thường xuyên nhất
--    Đây là bảng Spark ghi vào sau khi conform + validate
-- ============================================================
CREATE TABLE IF NOT EXISTS `banking-data-platform-500108.nyc_taxi_conformed.trips`
(
  -- IDs
  vendor_id             INT64,
  service_type          STRING  NOT NULL,  -- "yellow" | "green" — thêm lúc ingest

  -- Datetime (dùng DATE partition column riêng thay vì partition trực tiếp trên TIMESTAMP)
  pickup_datetime       TIMESTAMP,
  dropoff_datetime      TIMESTAMP,
  pickup_date           DATE    NOT NULL,  -- derived từ pickup_datetime, dùng để PARTITION

  -- Passenger & distance
  passenger_count       INT64,
  trip_distance         FLOAT64,

  -- Location (đã join zone lookup, có tên borough/zone)
  pu_location_id        INT64,
  do_location_id        INT64,
  pu_borough            STRING,
  pu_zone               STRING,
  do_borough            STRING,
  do_zone               STRING,

  -- Rate & payment
  ratecode_id           INT64,
  store_and_fwd_flag    STRING,
  payment_type          INT64,

  -- Fare components
  fare_amount           FLOAT64,
  extra                 FLOAT64,
  mta_tax               FLOAT64,
  tip_amount            FLOAT64,
  tolls_amount          FLOAT64,
  improvement_surcharge FLOAT64,
  congestion_surcharge  FLOAT64,  -- null nếu file cũ không có
  airport_fee           FLOAT64,  -- null nếu file cũ không có
  cbd_congestion_fee    FLOAT64,  -- null nếu file cũ không có, từ 2025
  total_amount          FLOAT64,

  -- Metadata
  ingestion_date        DATE    NOT NULL,  -- ngày Spark job chạy
  source_file           STRING             -- tên file Parquet gốc để trace back
)
PARTITION BY pickup_date
CLUSTER BY service_type, pu_borough
OPTIONS (
  description = "Conformed NYC taxi trips — validated, schema-reconciled, zone-joined. Spark writes here.",
  require_partition_filter = FALSE
);

-- ============================================================
-- 2. nyc_taxi_quarantine.rejected_trips
--    Partition by ingestion_date (ngày job chạy, không phải pickup_date
--    vì record lỗi có thể có pickup_date null/invalid)
--    Không cluster — table này nhỏ, query chủ yếu theo tháng ingest
-- ============================================================
CREATE TABLE IF NOT EXISTS `banking-data-platform-500108.nyc_taxi_quarantine.rejected_trips`
(
  -- Raw fields — giữ nguyên, không transform (để audit được)
  vendor_id             INT64,
  service_type          STRING,
  pickup_datetime       TIMESTAMP,
  dropoff_datetime      TIMESTAMP,
  passenger_count       INT64,
  trip_distance         FLOAT64,
  pu_location_id        INT64,
  do_location_id        INT64,
  fare_amount           FLOAT64,
  total_amount          FLOAT64,

  -- Quarantine metadata
  reason_code           STRING  NOT NULL,  -- "negative_fare" | "invalid_duration" | ...
  reason_detail         STRING,            -- chi tiết thêm, VD giá trị thực tế gây lỗi
  ingestion_date        DATE    NOT NULL,
  source_file           STRING
)
PARTITION BY ingestion_date
OPTIONS (
  description = "Rejected NYC taxi records — raw values preserved for audit. reason_code explains why rejected.",
  require_partition_filter = FALSE
);