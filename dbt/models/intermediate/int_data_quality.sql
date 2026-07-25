-- models/intermediate/int_data_quality.sql
with quarantine as (
    select * from {{ source('nyc_taxi_quarantine', 'rejected_trips') }}
),

monthly_quality as (
    select
        date_trunc(ingestion_date, month)   as ingestion_month,
        service_type,
        reason_code,
        count(*)                            as rejected_count
    from quarantine
    group by 1, 2, 3
)

select * from monthly_quality
