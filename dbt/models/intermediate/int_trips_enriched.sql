-- models/intermediate/int_trips_enriched.sql
-- Business logic layer: tính các metric phục vụ marts
-- Input: stg_trips (đã sạch, đã có zone info)

with trips as (
    select * from {{ ref('stg_trips') }}
),

enriched as (
    select
        -- Pass-through tất cả cột gốc
        *,

        -- Trip duration
        timestamp_diff(dropoff_datetime, pickup_datetime, minute) as trip_duration_minutes,

        -- Tip percentage (tránh divide by zero)
        case
            when fare_amount > 0 then round(tip_amount / fare_amount * 100, 2)
            else null
        end as tip_percentage,

        -- Revenue per mile (tránh divide by zero)
        case
            when trip_distance > 0 then round(total_amount / trip_distance, 2)
            else null
        end as revenue_per_mile,

        -- Time of day bucket
        case
            when extract(hour from pickup_datetime) between 6 and 11  then 'morning'
            when extract(hour from pickup_datetime) between 12 and 16 then 'afternoon'
            when extract(hour from pickup_datetime) between 17 and 21 then 'evening'
            else 'night'
        end as time_of_day,

        -- Hour of day (0-23) cho demand analysis
        extract(hour from pickup_datetime) as pickup_hour,

        -- Day of week (1=Sunday, 7=Saturday trong BigQuery)
        extract(dayofweek from pickup_date) as day_of_week,

        -- Month/Year cho trend analysis
        extract(month from pickup_date) as pickup_month,
        extract(year from pickup_date)  as pickup_year

    from trips
)

select * from enriched
