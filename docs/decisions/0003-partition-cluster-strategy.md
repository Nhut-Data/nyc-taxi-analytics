# ADR-0003: Partition & cluster strategy cho BigQuery marts

## Trạng thái
Đã chấp nhận (Accepted)

## Bối cảnh

Khi rà lại toàn bộ dbt models trong lúc viết tài liệu dự án, phát hiện
**không có model nào** khai `partition_by`/`cluster_by` — đây không phải
một quyết định có chủ đích từ đầu, mà là một khoảng trống thật (chưa nghĩ
tới lúc build). Ghi nhận trung thực điều này thay vì nguỵ tạo lý do, vì
đây chính xác là quá trình review lại một hệ thống thật sẽ trải qua.

`partition_by`/`cluster_by` chỉ áp dụng được cho **table** vật lý, không
áp dụng cho **view**. Theo cấu hình `dbt_project.yml`, chỉ tầng `marts`
được materialize dạng `table`; `staging`/`intermediate` là `view`. Vậy
phạm vi cần xem xét chỉ giới hạn ở 4 bảng mart:
`fact_trips`, `agg_hourly_demand_by_zone`, `agg_monthly_zone_revenue`,
`agg_monthly_data_quality`.

## Quyết định

**Chỉ thêm partition + cluster cho `fact_trips`**, không đụng tới 3 bảng
`agg_*` còn lại:

```sql
{{ config(
    partition_by={'field': 'pickup_date', 'data_type': 'date'},
    cluster_by=['pu_borough']
) }}
```

**Lý do chọn `fact_trips`**:
- Grain 1-dòng-1-chuyến-đi, ~2.9 triệu dòng/tháng — bảng lớn nhất trong
  toàn bộ mart, đúng đối tượng partition/cluster được thiết kế để tối ưu.
- `pickup_date`: cột `DATE` có sẵn, mọi chart dashboard đều lọc theo
  khoảng ngày — partition theo ngày giúp BigQuery chỉ quét đúng partition
  cần, giảm bytes scanned và chi phí on-demand query.
- `pu_borough`: cardinality thấp-vừa (8 giá trị), là cột breakdown/legend
  xuất hiện ở hầu hết mọi chart trong dashboard Looker Studio — cluster
  theo cột này giúp BigQuery sắp xếp dữ liệu vật lý gần nhau khi filter
  hoặc `GROUP BY pu_borough`.

**Lý do KHÔNG thêm cho 3 bảng `agg_*`**: các bảng này đã là kết quả tổng
hợp sẵn (ví dụ `agg_hourly_demand_by_zone` chỉ ~77 nghìn dòng — tổ hợp
ngày × giờ × zone của 1 tháng dữ liệu). Ở quy mô này, partition/cluster
mang lại lợi ích không đáng kể, thậm chí có thể phản tác dụng (nhiều
partition chứa quá ít dữ liệu mỗi phần làm tăng overhead quản lý metadata
thay vì giảm). Đây là quyết định có chủ đích tránh over-engineering, không
phải bỏ sót.

## Hệ quả

**Cần full-refresh sau khi áp dụng**: partition/cluster không áp dụng
được qua incremental update thông thường của dbt — BigQuery cần tạo lại
bảng vật lý với cấu trúc partition mới
(`dbt run --full-refresh --select fact_trips`).

**Giới hạn của demo hiện tại**: vì scope chỉ có 1 tháng dữ liệu
(2024-01), lợi ích thực tế của partition theo ngày chưa thể hiện rõ ràng
bằng số liệu (chỉ có 1 partition). Quyết định này được đưa ra dựa trên
thực hành chuẩn khi vận hành ở quy mô nhiều tháng/năm — đúng với thiết kế
ban đầu của DAG (`get_target_period` tự tính tháng theo lịch, xem
ADR liên quan tới fix hardcode period), không phải quy mô demo hiện tại.
