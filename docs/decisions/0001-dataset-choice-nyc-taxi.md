# ADR-0001: Lựa chọn dataset — NYC TLC Taxi Trip Data

## Trạng thái
Đã chấp nhận (Accepted)

## Bối cảnh

Project 2 cần một dataset đủ để chứng minh nhu cầu xử lý big data phân tán
(Spark) là có lý do kỹ thuật thật, không phải thêm công cụ vào cho đủ bộ
skill. Combo skill mục tiêu (SQL, Python, ETL, Airflow, dbt, Spark, GCP,
data validation) được xác định từ nghiên cứu JD thị trường tuyển dụng thực
tế cho vị trí Data Engineer, không phải chọn ngẫu nhiên.

Yêu cầu đặt ra cho dataset: phải tự nhiên đòi hỏi xử lý phân tán, có nhịp
cập nhật thật để orchestration (Airflow) có ý nghĩa, và có đủ vấn đề chất
lượng dữ liệu thật để việc xây dựng data quality / quarantine pattern
không phải là "diễn".

## Các phương án đã cân nhắc

| # | Dataset | Ưu điểm | Lý do loại |
|---|---------|---------|------------|
| 1 | GA4 BigQuery public dataset | Sẵn có trên BigQuery, dễ truy cập, quen thuộc với Analytics Engineering | Quy mô nhỏ, BigQuery SQL serverless xử lý đủ tốt — dùng Spark ở đây không có lý do kỹ thuật thật, dễ bị hỏi ngược "sao không dùng SQL cho lẹ" |
| 2 | Yelp Open Dataset | Multi-table (business/review/checkin/tip/user), join phức tạp — lý do hợp lý để dùng Spark | (a) Volume ~8-10GB hơi nhẹ, phải dùng đủ 5 bảng + tận dụng data skew mới đủ thuyết phục cần Spark. (b) Là snapshot tĩnh (release ~1 lần/năm), không có nhịp cập nhật thật — phải giả lập batch ingest theo thời gian, làm giảm tính chân thực của câu chuyện orchestration |
| 3 | **NYC TLC Taxi Trip Data (chọn)** | Xem mục Quyết định | — |

## Quyết định

Chọn **NYC TLC Taxi Trip Data**. Đây là dataset duy nhất trong 3 phương án
mà mọi lý do dùng Spark/Airflow/dbt đều phát sinh **tự nhiên từ chính đặc
tính dữ liệu**, không phải thiết kế ngược để ép công cụ vào:

- **Nhịp cập nhật thật**: TLC công bố dữ liệu hàng tháng, trễ ~2 tháng —
  cadence thật cho Airflow, không cần giả lập.
- **Schema drift thật**: kiểu dữ liệu không nhất quán giữa các file qua
  nhiều năm (ví dụ cột INT64 ở file này, DOUBLE ở file khác) — bài toán
  reconciliation thật cho Spark.
- **Schema evolution thật**: cột mới xuất hiện theo chính sách thay đổi
  theo thời gian (ví dụ `airport_fee`, và từ 2025 có `cbd_congestion_fee`
  do chính sách congestion pricing mới).
- **Join thật cần thiết**: từ 7/2016 TLC không cung cấp toạ độ GPS trực
  tiếp, chỉ có `LocationID` — bắt buộc broadcast join với bảng zone lookup
  để có ngữ cảnh khu vực.
- **Quy mô đủ lớn**: trải dài từ 2009 tới nay, đủ để chứng minh nhu cầu xử
  lý phân tán thật khi chạy đủ nhiều tháng/năm dữ liệu.

## Hệ quả

**Rủi ro cần lưu ý**: NYC Taxi là dataset phổ biến trong giới học Data
Engineering (nhiều tutorial/course dùng) — cần nêu rõ điểm khác biệt của
project (business question tự đặt, quarantine pattern, ranh giới
Spark/dbt) trong README để tránh bị hiểu nhầm là làm theo course có sẵn.

**Đánh đổi đã chấp nhận**: mất đi phần "join quan hệ nhiều bảng phức tạp"
như Yelp, nhưng đổi lại không phải giả lập bất kỳ điều gì — mọi quyết định
kiến trúc đều có bằng chứng thật từ dataset.
