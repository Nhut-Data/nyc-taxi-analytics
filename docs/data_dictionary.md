# Data Dictionary — NYC Taxi Analytics Platform

> Dựa trên exploration thực tế (Phase 1), không phải TLC documentation.
> File explore: `notebooks/01_explore_raw_schema.ipynb`
> Ngày explore: 2026-07-25

---

## 1. Nguồn dữ liệu

| Item | Chi tiết |
|---|---|
| Nguồn | NYC TLC Trip Record Data |
| URL pattern | `https://d37ci6vzurychx.cloudfront.net/trip-data/{service_type}_tripdata_{YYYY}-{MM}.parquet` |
| Format | Parquet (toàn bộ file lịch sử từ 5/2022) |
| Nhịp cập nhật | Hàng tháng, trễ ~2 tháng (verified: tháng 7/2026, data mới nhất là 4/2026) |
| Zone lookup | `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv` — 265 rows, tĩnh, tải 1 lần |

---

## 2. Schema — Yellow Taxi (verified từ file thật)

### 2a. Schema drift theo thời gian

| Cột | 2011 type | 2024 type | Vấn đề | Xử lý Phase 3 |
|---|---|---|---|---|
| `VendorID` | `long` | `integer` | type drift | cast về `long` |
| `PULocationID` | `long` | `integer` | type drift | cast về `long` |
| `DOLocationID` | `long` | `integer` | type drift | cast về `long` |
| `airport_fee` | `double` (lowercase) | — | — | — |
| `Airport_fee` | — | `double` (capitalized) | case drift — cùng 1 cột, khác convention | lowercase toàn bộ tên cột khi đọc vào |

**Nguyên tắc xử lý:**
- Cast toàn bộ integer → long (long an toàn hơn, chứa được mọi giá trị của integer)
- Lowercase toàn bộ tên cột **trước** mọi xử lý khác — tránh `airport_fee` vs `Airport_fee` tạo ra 2 cột riêng khi union

### 2b. Cột xuất hiện theo thời gian (schema evolution)

| Cột | Xuất hiện từ | Lý do |
|---|---|---|
| `congestion_surcharge` | ~2019 | NYC congestion pricing policy |
| `airport_fee` / `Airport_fee` | ~2022 | TLC airport surcharge policy |
| `cbd_congestion_fee` | 2025 | NYC CBD congestion pricing (mới nhất) |

**Xử lý Phase 3:** Khi đọc file cũ không có các cột này → fill `null`, không báo lỗi.

### 2c. Schema drift theo service_type (chưa explore, ghi chú để nhớ)

| Service | Pickup col | Dropoff col |
|---|---|---|
| Yellow taxi | `tpep_pickup_datetime` | `tpep_dropoff_datetime` |
| Green taxi | `lpep_pickup_datetime` | `lpep_dropoff_datetime` |

**Xử lý Phase 3 khi mở rộng sang Green:** rename về tên chuẩn thống nhất
(`pickup_datetime`, `dropoff_datetime`) trước khi union.

### 2d. Full schema chuẩn (target sau khi reconcile)

| Cột | Type chuẩn | Nullable | Ghi chú |
|---|---|---|---|
| `vendor_id` | `long` | true | lowercase từ VendorID |
| `pickup_datetime` | `timestamp_ntz` | false | chuẩn hóa từ tpep_/lpep_ |
| `dropoff_datetime` | `timestamp_ntz` | false | chuẩn hóa từ tpep_/lpep_ |
| `passenger_count` | `long` | true | |
| `trip_distance` | `double` | true | |
| `ratecode_id` | `long` | true | lowercase từ RatecodeID |
| `store_and_fwd_flag` | `string` | true | |
| `pu_location_id` | `long` | true | lowercase từ PULocationID |
| `do_location_id` | `long` | true | lowercase từ DOLocationID |
| `payment_type` | `long` | true | |
| `fare_amount` | `double` | true | |
| `extra` | `double` | true | |
| `mta_tax` | `double` | true | |
| `tip_amount` | `double` | true | |
| `tolls_amount` | `double` | true | |
| `improvement_surcharge` | `double` | true | |
| `total_amount` | `double` | true | |
| `congestion_surcharge` | `double` | true | null nếu file cũ không có |
| `airport_fee` | `double` | true | null nếu file cũ không có |
| `cbd_congestion_fee` | `double` | true | null nếu file cũ không có, từ 2025 |
| `service_type` | `string` | false | thêm lúc ingest: "yellow"/"green" |

---

## 3. Data Quality — Observed từ file thật

### 3a. Row count theo năm (business context)

| Tháng | Row count | Ghi chú |
|---|---|---|
| 2011-01 | 13,464,997 | Yellow taxi gần như độc chiếm thị trường |
| 2024-01 | 2,964,624 | Giảm ~78% — mất thị phần vào Uber/Lyft (FHVHV) |

### 3b. Validate rules — ngưỡng từ data thật

| Rule | Observed (2011) | Observed (2024) | Ngưỡng Phase 3 | reason_code |
|---|---|---|---|---|
| `fare_amount < 0` | 0 (0.00%) | 37,448 (1.26%) | reject nếu < 0 | `negative_fare` |
| `fare_amount = 0` | 0 (0.00%) | 893 (0.03%) | flag, không reject | `zero_fare` |
| `trip_distance <= 0` | 76,093 (0.57%) | 60,371 (2.04%) | reject nếu < 0, flag nếu = 0 | `invalid_distance` |
| `trip_distance > 500` | 0 | có (max=312,722) | reject | `suspicious_distance` |
| `duration_minutes <= 0` | 23,148 (0.17%) | 870 (0.03%) | reject | `invalid_duration` |
| `duration_minutes > 1440` | 4 (0.00%) | 16 (0.00%) | reject | `duration_gt_24h` |
| `PULocationID` invalid | 0 (0.00%) | 0 (0.00%) | reject nếu ngoài 1-265 | `invalid_location_id` |
| `DOLocationID` invalid | 0 (0.00%) | 0 (0.00%) | reject nếu ngoài 1-265 | `invalid_location_id` |

### 3c. Fare distribution (để phát hiện outlier ở dbt layer)

| Percentile | 2011 | 2024 |
|---|---|---|
| p1 | 3.3 | -5.1 |
| p25 | 5.7 | 8.6 |
| p50 | 7.7 | 12.8 |
| p75 | 10.9 | 20.5 |
| p99 | 45.0 | 76.2 |

### 3d. Duration distribution (minutes)

| Percentile | 2011 | 2024 |
|---|---|---|
| p1 | 1.4 | 0.6 |
| p25 | 6.0 | 7.2 |
| p50 | 9.8 | 11.6 |
| p75 | 15.0 | 18.7 |
| p95 | 27.3 | 37.9 |
| p99 | 42.6 | 60.4 |
| p99.9 | 69.8 | 115.2 |

2024 có duration dài hơn đáng kể ở mọi percentile — phản ánh traffic NYC tăng
và/hoặc Yellow taxi chủ yếu phục vụ các chuyến dài hơn (airport, outer borough)
sau khi mất thị phần ngắn vào rideshare.

---

## 4. Zone Lookup

| Item | Chi tiết |
|---|---|
| File | `taxi_zone_lookup.csv` |
| Rows | 265 |
| Columns | `LocationID`, `Borough`, `Zone`, `service_zone` |
| LocationID range | 1 → 265 |
| Boroughs | EWR, Queens, Bronx, Manhattan, Staten Island, Brooklyn, Unknown, NaN |

**Lưu ý:** `Borough = NaN` tồn tại trong file lookup — LocationID hợp lệ về kỹ thuật
nhưng thiếu borough name. Khi join ở Phase 3, không reject record này,
chỉ để borough = null và handle ở dbt layer.