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