# ADR-0007: Kiểm soát tường minh overwrite thay vì phụ thuộc partitionOverwriteMode

## Trạng thái
Đã chấp nhận (Accepted) — verify thực tế qua backfill đầy đủ 12 tháng dữ
liệu (~40 triệu dòng), xác nhận không tháng nào bị mất khi thêm tháng mới.

## Bối cảnh

Ban đầu, Spark job dùng `.mode("append")` để ghi vào BigQuery — gây bug
dữ liệu nhân đôi tới 7 lần khi job chạy lại (do lỗi hạ tầng, retry thủ
công...) mà không xoá dữ liệu cũ trước. Fix ban đầu: đổi sang
`.mode("overwrite")`. Test idempotent (trigger lại DAG không xoá bảng)
pass — tưởng đã giải quyết xong.

Vấn đề thật chỉ lộ ra khi backfill nhiều tháng: `.mode("overwrite")` của
BigQuery Spark connector mặc định dùng `partitionOverwriteMode = STATIC`
— **xoá sạch toàn bộ bảng**, không chỉ đúng partition đang ghi. Test
idempotent trước đó "pass" chỉ vì nó vô tình che giấu bug này — chạy lại
**cùng 1 tháng** thì overwrite toàn bảng hay overwrite đúng partition cho
kết quả giống hệt nhau, không phân biệt được. Bug chỉ lộ ra khi ghi
**tháng khác** — lúc đó dữ liệu các tháng trước bị xoá sạch.

## Quyết định

Không dùng `.mode("overwrite")` của connector nữa. Thay vào đó, tự kiểm
soát tường minh bằng 2 bước:

1. **`DELETE`** đúng phạm vi partition sắp ghi, dùng BigQuery Python
   client trực tiếp trong Spark job (trước khi ghi):
```python
   client.query(f"DELETE FROM `{table}` WHERE pickup_date >= @start AND pickup_date < @end", ...)
```
2. **`.mode("append")`** để ghi dữ liệu mới vào — an toàn vì phần cũ của
   đúng partition đó đã được xoá ở bước 1.

**Lý do không dùng `partitionOverwriteMode = DYNAMIC`** (tưởng là giải
pháp đơn giản hơn): tra cứu cho thấy nhiều báo cáo lỗi thật từ cộng đồng
(GitHub issues của spark-bigquery-connector) xác nhận **ngay cả khi set
DYNAMIC, connector vẫn có trường hợp xoá nhầm partition khác** — hành vi
không ổn định đã biết, không đáng tin cho 1 pipeline cần đúng dữ liệu
tuyệt đối. Tự kiểm soát bằng DELETE tường minh loại bỏ hoàn toàn phụ
thuộc vào hành vi nội bộ khó đoán của bên thứ ba.

## Hệ quả

**Lợi ích**: đã verify thực tế — backfill tuần tự 12 tháng, mỗi tháng giữ
đúng số liệu riêng, không tháng nào bị ghi đè nhầm. Logic dễ hiểu, dễ
debug (query DELETE có thể chạy tay để kiểm tra độc lập).

**Chi phí phát sinh nhỏ**: mỗi lần chạy job tốn thêm 1 BigQuery DELETE
query trước khi ghi — chi phí không đáng kể ở quy mô dữ liệu hiện tại
(vài triệu dòng/tháng).

**Rủi ro còn lại đã biết**: nếu Spark job bị crash **giữa** bước DELETE
và bước ghi xong (ví dụ do lỗi hạ tầng GCP), tháng đó sẽ tạm thời rỗng
cho tới lần chạy thành công tiếp theo. Không phải bug nhân đôi (dễ phát
hiện hơn nhiều — số liệu tụt bất thường, không phải tăng ảo), và task
`row_count_sanity_check` sẵn có trong DAG sẽ tự bắt được tình huống này.

**Bài học rút ra**: một bài test "pass" không có nghĩa là đã kiểm chứng
đúng kịch bản quan trọng nhất — idempotent test ban đầu chỉ test đúng 1
trường hợp (chạy lại cùng tháng), không phải trường hợp thực tế sẽ xảy ra
khi vận hành lâu dài (nhiều tháng khác nhau).
