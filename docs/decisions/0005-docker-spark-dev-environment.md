# ADR 0005 — Docker cho Spark Local Dev Environment

## Trạng thái
Accepted

## Bối cảnh
Spark conform job (Phase 3) chạy production trên Dataproc Serverless runtime 2.3 LTS.
Nếu dev local bằng venv trần, dễ lệch version so với cloud:
- Máy dev đang có Java 11, nhưng runtime 2.3 dùng Java 17
- Python 3.10 (máy dev) vs Python 3.11 (runtime 2.3)
- PySpark 3.5.3 đã cài đúng, nhưng không đảm bảo nếu ai đó chạy `pip install pyspark` không chỉ version

## Quyết định
Dùng Docker container để pin cứng dev environment khớp với Dataproc Serverless runtime 2.3 LTS:
- Python 3.11
- PySpark 3.5.3
- Java 17

File: `spark_jobs/Dockerfile.spark-dev`
Đặt trong `spark_jobs/` vì đây là dev tool cho riêng Spark layer,
không liên quan đến Airflow (docker-compose.yaml ở root giữ nguyên).

## Hệ quả
- Mọi code/test trong `spark_jobs/` chạy bên trong container này
- Airflow vẫn dùng docker-compose.yaml riêng ở root, không đụng vào
- Thêm `spark_jobs/Dockerfile.spark-dev` vào repo, không gitignore

## Phương án đã cân nhắc nhưng loại
- **venv trần**: nhanh hơn nhưng không đảm bảo parity Java/Python version với cloud
- **Nâng Java local lên 17**: fix được Java nhưng không fix được Python version mismatch
## Cập nhật: BigQuery connector jar (2026-08)

**Vấn đề phát hiện:** File `spark-bigquery-with-dependencies_2.13-*.jar` từng được
`COPY` vào image nhưng lệch Scala version so với `pyspark==3.5.3` (build trên
Scala 2.12) — rủi ro lỗi runtime khi load class. Ngoài ra file chưa từng được
git track (bị `.gitignore` chặn), khiến ai clone repo mới build sẽ lỗi thiếu file.

**Quyết định:** Đổi sang `RUN curl` tải trực tiếp bản `_2.12` đúng version từ
HTTPS public bucket của Google (`storage.googleapis.com/spark-lib/bigquery/...`)
ngay trong lúc build image, thay vì commit binary ~54MB vào git. Xác nhận HTTPS
thuần tải được bình thường — hạn chế mạng gặp trước đây khả năng đến từ
`gsutil`/`gcloud` (cần trao đổi credential), không phải chặn network hoàn toàn.

**Lợi ích:** không phình repo, luôn dùng đúng Scala version khớp PySpark,
build được ngay khi clone mới mà không cần bước setup thủ công.
## Cập nhật: Chênh lệch Scala version giữa local dev và production (2026-08)

**Phát hiện khi review lại dự án**: tra cứu tài liệu chính thức Google Cloud
xác nhận Dataproc runtime 2.3 (production) dùng cấu hình sau:

| Component | Local dev (`Dockerfile.spark-dev`) | Production (Dataproc runtime 2.3) |
|---|---|---|
| Apache Spark | 3.5.3 | 3.5.3 |
| Scala | 2.12 (mặc định của PyPI `pyspark`) | 2.13 |
| BigQuery Connector | Tự tải qua HTTPS, bản `_2.12` | Built-in sẵn, bản `_2.13` |

Số Spark version khớp nhau, nhưng **Scala build khác nhau** — đặc thù của
Apache Spark: PyPI wheel mặc định luôn đóng gói bản Scala 2.12, còn Google
tự chọn đóng gói bản Scala 2.13 cho Dataproc managed runtime.

**Vì sao đây không phải bug cần fix**: production không cần đính kèm jar
nào (built-in sẵn, tự khớp đúng Scala 2.13 nội bộ), nên local dev dùng
Scala 2.12 không ảnh hưởng gì tới hành vi production. Fix trước đó (đổi
jar sang bản `_2.12`) là đúng đắn — khớp chính xác với Scala build của
`pyspark` cài qua pip cho môi trường local.

**Bài học cần nhớ**: môi trường local dev **không phải bản sao hoàn toàn
chính xác** của production ở tầng JVM/Scala. Với code Python thuần, không
ảnh hưởng gì. Nhưng nếu sau này thêm bất kỳ thư viện JVM/Scala-specific
nào khác, cần luôn kiểm tra khớp đúng Scala version của **từng môi trường
cụ thể**, không giả định 1 phiên bản dùng chung cho cả 2 nơi.
