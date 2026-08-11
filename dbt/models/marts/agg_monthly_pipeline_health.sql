-- models/marts/agg_monthly_pipeline_health.sql
-- Grain: 1 row = 1 (ingestion_month, service_type)
-- Tỷ lệ lỗi thật = rejected / (valid + rejected), join theo ingestion_date
-- (không dùng pickup_date vì chính dòng wrong_period có pickup_date sai)
{{ config(materialized='table') }}

with valid_counts as (
    select
        date_trunc(ingestion_date, month) as ingestion_month,
        service_type,
        count(*)                          as total_valid_trips
    from {{ ref('fact_trips') }}
    group by 1, 2
),

rejected_counts as (
    select
        ingestion_month,
        service_type,
        sum(rejected_count) as total_rejected_trips
    from {{ ref('int_data_quality') }}
    group by 1, 2
)

select
    coalesce(v.ingestion_month, r.ingestion_month) as ingestion_month,
    coalesce(v.service_type, r.service_type)       as service_type,
    coalesce(v.total_valid_trips, 0)                as total_valid_trips,
    coalesce(r.total_rejected_trips, 0)             as total_rejected_trips,
    coalesce(v.total_valid_trips, 0) + coalesce(r.total_rejected_trips, 0)
                                                     as total_processed,
    round(
        coalesce(r.total_rejected_trips, 0) * 100.0 / nullif(
            coalesce(v.total_valid_trips, 0) + coalesce(r.total_rejected_trips, 0), 0
        ), 2
    )                                                as rejection_rate_pct
from valid_counts v
full outer join rejected_counts r
    on v.ingestion_month = r.ingestion_month
    and v.service_type = r.service_type
