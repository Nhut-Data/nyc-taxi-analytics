# ADR-0002: Ranh giới trách nhiệm giữa Spark và dbt

## Trạng thái
Đã chấp nhận (Accepted)

## Bối cảnh

Pipeline có cả Spark (chạy trên Dataproc Serverless) và dbt (chạy trên
BigQuery). Cả hai đều có khả năng transform dữ liệu — nếu không định nghĩa
rõ ranh giới, dễ xảy ra tình trạng logic bị trùng lặp ở cả hai nơi, hoặc
tệ hơn là mỗi lần thêm transform mới lại phải tự hỏi "cái này nên viết ở
Spark hay dbt", làm chậm phát triển và khó bảo trì.

## Quyết định

Ranh giới được chốt theo nguyên tắc: **Spark sở hữu tính đúng đắn của dữ
liệu thô (correctness), dbt sở hữu ngữ nghĩa nghiệp vụ và tổng hợp
(business semantics & aggregation)**. Cụ thể qua 4 tầng thật trong pipeline:

### Tầng 1 — Spark (`conform_trips.py`)
Chạy trên Dataproc Serverless, đọc trực tiếp file Parquet thô từ GCS.
Trách nhiệm:
- **Schema reconciliation**: xử lý lệch kiểu dữ liệu giữa các file theo
  năm (ví dụ INT64 vs DOUBLE cho cùng 1 cột ở file khác năm) — bài toán
  cần xử lý ở mức file/raw, trước khi dữ liệu vào một bảng có schema cố
  định.
- **Data quality gate**: áp dụng toàn bộ validation rule (`MIN_FARE_AMOUNT`,
  kiểm tra `wrong_period`, `invalid_duration`, `suspicious_distance`...)
  theo đúng thứ tự ưu tiên `reason_code`, tách dữ liệu thành 2 nhánh:
  `nyc_taxi_conformed.trips` (hợp lệ) và `nyc_taxi_quarantine.rejected_trips`
  (bị từ chối, có lý do rõ ràng). Xem chi tiết tại ADR-0004.
- **Enrichment cần join phân tán**: broadcast join với bảng zone lookup để
  thêm `pu_borough`, `pu_zone`, `do_borough`, `do_zone` — vì từ 7/2016 TLC
  chỉ cung cấp `LocationID`, không có toạ độ GPS trực tiếp (xem ADR-0001).
- Ghi kết quả bằng `overwrite` mode, đảm bảo idempotent (đã verify bằng
  cách trigger lại DAG nhiều lần không xoá bảng, count không đổi).

**Lý do các việc này thuộc Spark, không phải dbt**: đây là các bước xử lý
trên dữ liệu **thô, chưa có schema cố định**, cần đọc trực tiếp file và xử
lý ở mức phân tán trước khi dữ liệu có thể nằm gọn trong 1 bảng BigQuery
sạch. dbt chỉ vận hành trên dữ liệu đã ở dạng bảng SQL, không đọc được file
Parquet thô.

### Tầng 2 — dbt staging (`stg_trips.sql`)
Passthrough/rename thuần tuý từ `nyc_taxi_conformed.trips` — comment trong
chính file ghi rõ **"KHÔNG join lại, KHÔNG filter thêm, KHÔNG business
logic. Spark đã lo toàn bộ ở conform job."** Vai trò duy nhất: làm lớp
interface cách ly các model phía sau khỏi tên/cấu trúc bảng nguồn — nếu
sau này đổi tên bảng nguồn hay thêm cột, chỉ cần sửa đúng 1 file này.

### Tầng 3 — dbt intermediate (business logic)
- `int_trips_enriched.sql`: tính các metric nghiệp vụ trên dữ liệu đã sạch
  — `trip_duration_minutes`, `tip_percentage`, `revenue_per_mile`, bucket
  `time_of_day`, `pickup_hour`, `day_of_week`, `pickup_month/year`. Đây là
  logic **thuần SQL, biểu đạt tốt bằng dbt**, không cần sức mạnh tính toán
  phân tán của Spark vì chạy trên BigQuery (đã serverless, tự scale).
- `int_data_quality.sql`: tổng hợp bảng quarantine theo tháng/service
  type/reason_code — đọc **trực tiếp từ `nyc_taxi_quarantine` source**,
  không qua `stg_trips` (đây là nhánh song song, phục vụ dashboard Data
  Quality riêng, không phải một phần của luồng `trips` chính).

**Lý do các việc này thuộc dbt, không phải Spark**: đây là transform trên
dữ liệu **đã có schema cố định, đã ở BigQuery** — SQL declarative của dbt
biểu đạt rõ ràng hơn, có version control cho từng model, có test
(`dbt test`) tích hợp sẵn, và tận dụng compute engine serverless của
BigQuery thay vì phải giữ 1 Spark cluster chạy cho việc này.

### Tầng 4 — dbt marts
Bảng tổng hợp cuối cùng phục vụ trực tiếp Looker Studio
(`agg_hourly_demand_by_zone`, `agg_monthly_zone_revenue`,
`agg_monthly_data_quality`, `fact_trips`), build trên tầng intermediate.

## Hệ quả

**Lợi ích**: ranh giới rõ ràng giúp code review nhanh hơn — nhìn vào loại
thay đổi (đọc file thô/join lớn → Spark PR; tính metric/aggregate → dbt
PR) là biết ngay nên sửa ở đâu, tránh tranh luận lặp lại.

**Đánh đổi đã chấp nhận**: `stg_trips` gần như không làm gì (chỉ rename) —
có thể bị coi là tầng thừa nếu nhìn hời hợt. Nhưng giữ lại có chủ đích: đây
là "hợp đồng interface" giữa 2 hệ thống (Spark ghi, dbt đọc), tách biệt để
một bên đổi cấu trúc không làm vỡ bên còn lại ngay lập tức.
