# ADR-0004: Quarantine pattern cho data quality

## Trạng thái
Đã chấp nhận (Accepted)

## Bối cảnh

Dữ liệu NYC TLC Taxi có các vấn đề chất lượng thật, không phải giả lập:
lỗi nhập liệu từ phía vendor (fare âm hoặc bằng 0, thời lượng chuyến đi âm
hoặc bất thường), và rò rỉ dữ liệu sai kỳ (bản ghi có `pickup_datetime`
thuộc năm/tháng khác lẫn vào file đang xử lý — ví dụ file tháng 2024-01
nhưng có dòng `pickup_datetime = 2002-12-31`).

Hai cách xử lý sai đã bị loại: (1) im lặng loại bỏ dòng lỗi — mất khả năng
audit, không ai biết pipeline đã vứt bao nhiêu dữ liệu và vì sao; (2) giữ
nguyên dòng lỗi trong bảng chính — làm nhiễu mọi phân tích phía sau
(ví dụ: 1 dòng fare bất thường từng khiến `tip_percentage` tính ra 3016%
trên dashboard, xem chi tiết trong lịch sử fix của dự án).

## Quyết định

Tách dữ liệu thành 2 bảng dựa trên kết quả validate, thay vì lọc âm thầm:
- `nyc_taxi_conformed.trips` — dữ liệu vượt qua toàn bộ rule.
- `nyc_taxi_quarantine.rejected_trips` — dữ liệu bị từ chối, kèm cột
  `reason_code` ghi rõ lý do.

### Bộ rule và ngưỡng (`spark_jobs/validations/rules.py`)

| Rule | Ngưỡng | Reason code |
|---|---|---|
| Datetime không null | `pickup_datetime` và `dropoff_datetime` phải có giá trị | `null_datetime` |
| Đúng kỳ xử lý | `pickup_datetime` phải khớp năm/tháng đang xử lý | `wrong_period` |
| Fare hợp lệ | `fare_amount >= 2.5` | `invalid_fare` |
| Duration hợp lệ | `0 < duration <= 1440 phút` (24h) | `invalid_duration` |
| Distance hợp lệ | `0 <= trip_distance <= 500 dặm` | `suspicious_distance` |
| Location ID hợp lệ | `pu/do_location_id` trong khoảng `1-265` | `invalid_location_id` |

`MIN_FARE_AMOUNT = 2.5` là **ngưỡng an toàn xấp xỉ**, không phải con số
quy định chính thức đã xác minh — comment gốc trong code ghi rõ đây là
"giá mở cửa tối thiểu luật định NYC (~$3), chưa biết chắc", chọn 2.5 để
có biên độ an toàn, tránh loại nhầm chuyến đi hợp lệ ở mức giá thấp biên.
Đây là giới hạn đã biết, có thể tinh chỉnh lại nếu xác minh được số chính
thức từ TLC.

### Thứ tự ưu tiên reason_code

Mỗi dòng chỉ nhận **đúng 1 reason_code** — dòng nào fail nhiều rule cùng
lúc, chỉ ghi nhận rule có độ ưu tiên cao nhất, theo thứ tự:
null_datetime → wrong_period → invalid_fare → invalid_duration
→ suspicious_distance → invalid_location_id
**Lý do thứ tự này có chủ đích, không phải ngẫu nhiên**: `null_datetime`
đứng đầu vì đây là lỗi cấu trúc — không có datetime hợp lệ thì không thể
đánh giá được cả `wrong_period` lẫn bất kỳ rule nào khác. `wrong_period`
đứng thứ 2, trước các rule về nội dung (`fare`, `duration`, `distance`) —
vì đây là vấn đề **tính toàn vẹn của batch** (bản ghi về bản chất không
thuộc kỳ dữ liệu đang xử lý), cần được gắn cờ trước khi đánh giá nội dung
chuyến đi có hợp lý hay không.

**Bằng chứng thực tế cho thứ tự này**: khi thêm rule `wrong_period` vào
pipeline (trước đó chưa có), số liệu quarantine thay đổi như sau —
`invalid_fare` giảm từ 39.841 xuống 39.840 (giảm đúng 1), trong khi
`wrong_period` xuất hiện mới với 18 dòng. Điều này xác nhận có đúng 1 dòng
**vừa sai kỳ vừa có fare thấp** — trước khi có `wrong_period` trong chuỗi
ưu tiên, dòng này bị gắn nhãn `invalid_fare`; sau khi thêm, nó bị bắt bởi
`wrong_period` trước vì đứng ưu tiên cao hơn. Đây là bằng chứng trực tiếp
logic ưu tiên hoạt động đúng thiết kế, không phải suy luận lý thuyết.

## Hệ quả

**Lợi ích**:
- Không có dữ liệu nào bị vứt âm thầm — mọi dòng bị loại đều có lý do ghi
  lại, truy vết được.
- `reason_code` cho phép theo dõi xu hướng chất lượng dữ liệu theo thời
  gian qua `int_data_quality.sql` (tổng hợp theo tháng/service_type/
  reason_code), phục vụ dashboard Data Quality riêng.

**Đánh đổi đã chấp nhận**: mỗi dòng chỉ có 1 reason_code (không phải danh
sách đầy đủ mọi rule bị fail). Một dòng fail nhiều rule cùng lúc sẽ chỉ lộ
ra lý do có độ ưu tiên cao nhất; các lý do phụ khác không hiển thị trực
tiếp trong `reason_code`, muốn biết đầy đủ phải tự truy vấn giá trị gốc
của dòng đó.

**Nợ kỹ thuật nhỏ đã phát hiện khi viết ADR này**: hằng số
`REASON_DURATION_GT_24H` được định nghĩa trong `rules.py` nhưng chưa được
sử dụng — hiện tại `is_valid_duration()` gộp cả 2 trường hợp (duration
≤0 và duration >24h) vào chung 1 reason `invalid_duration`. Nếu cần phân
tích sâu hơn (phân biệt lỗi "duration âm/bằng 0" — khả năng cao là lỗi
nhập liệu — với "duration bất thường dài" — khả năng cao là lỗi đồng hồ/
GPS), có thể tách `get_reason_code()` thành 2 nhánh riêng trong tương lai.
