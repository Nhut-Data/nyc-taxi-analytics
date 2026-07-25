-- models/marts/agg_monthly_data_quality.sql
-- Câu hỏi kinh doanh 3: data quality trend theo tháng
-- Grain: 1 row = 1 (ingestion_month, service_type, reason_code)

select
    ingestion_month,
    service_type,
    reason_code,
    rejected_count,
    sum(rejected_count) over (
        partition by ingestion_month, service_type
    )                                   as total_rejected_that_month,
    round(
        rejected_count * 100.0 / nullif(
            sum(rejected_count) over (partition by ingestion_month, service_type),
            0
        ), 2
    )                                   as pct_of_monthly_rejected

from {{ ref('int_data_quality') }}
order by ingestion_month desc, rejected_count desc
