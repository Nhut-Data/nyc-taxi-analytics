# ADR-0006: Slack notification qua Airflow task callback

## Trạng thái
Đã chấp nhận (Accepted) — đã verify hoạt động thực tế 2 lần: 1 lần khi
`submit_dataproc_job` fail thật do hạ tầng GCP (zone capacity exhaustion),
1 lần khi trigger lại DAG để test. Cả 2 lần đều nhận được message trên
Slack đúng như thiết kế.

## Bối cảnh

DAG `nyc_taxi_monthly_pipeline` chạy không có người giám sát trực tiếp
(scheduled hoặc trigger tay rồi rời đi). Nếu không có cơ chế cảnh báo chủ
động, cách duy nhất để biết pipeline fail là tự vào Airflow UI kiểm tra —
không thực tế cho vận hành thật, và không thể hiện được thực hành alerting
chuẩn của một hệ thống production.

## Quyết định

Dùng **Airflow task callback** (`on_failure_callback` / `on_success_callback`)
kết hợp `requests.post()` gọi trực tiếp **Slack Incoming Webhook**, thay vì
dùng `SlackWebhookOperator` có sẵn trong Airflow provider.

**Lý do không dùng `SlackWebhookOperator`**: operator này tạo ra **1 task
riêng biệt** trong DAG graph. Về mặt ngữ nghĩa, việc "thông báo trạng thái
pipeline" không phải là một bước xử lý dữ liệu trong pipeline — nó là một
side-effect quan sát trạng thái của các bước khác. Dùng callback giữ đúng
ngữ nghĩa: thông báo là phản ứng với kết quả của task, không phải bản thân
là một task cần lên lịch/dependency riêng.

### Phạm vi áp dụng

- `on_failure_callback=notify_failure` gắn ở 3 task quan trọng nhất trong
  luồng: `submit_dataproc_job`, `dbt_run`, `dbt_test` — đây là các điểm
  fail có khả năng cao nhất và tốn thời gian debug nhất nếu không biết
  sớm.
- `on_success_callback=notify_success` chỉ gắn ở task cuối cùng
  (`dbt_docs_generate`) — báo hiệu toàn bộ pipeline tháng đó đã chạy sạch
  từ đầu đến cuối, không cần thông báo thành công ở từng task trung gian
  (sẽ gây spam kênh Slack không cần thiết).

### Quản lý secret

Webhook URL lưu qua **Airflow Variable** (`SLACK_WEBHOOK_URL`), set giá
trị qua biến môi trường `AIRFLOW_VAR_SLACK_WEBHOOK_URL` trong `.env` —
không commit giá trị thật vào git (đã có trong `.gitignore`, xem
`.env.example` để biết cấu trúc biến cần thiết).

## Hệ quả

**Lợi ích**: không làm phình thêm node vào DAG graph chỉ để gửi thông báo
— DAG graph giữ đúng số lượng task phản ánh các bước xử lý dữ liệu thật.
Callback tận dụng được context có sẵn của task (tên task, lý do fail) mà
không cần XCom hay logic truyền dữ liệu riêng.

**Đánh đổi đã chấp nhận**: vì callback không phải là 1 task riêng, nó
**không hiển thị trên DAG graph view** — khi xem nhanh sơ đồ pipeline, dễ
bỏ sót rằng có cơ chế alerting đang hoạt động phía sau. Người mới tiếp cận
project cần đọc code (`airflow/dags/callbacks.py` và tham số
`on_failure_callback`/`on_success_callback` trong từng task) mới biết được
đầy đủ, không thể chỉ nhìn UI là đủ.
