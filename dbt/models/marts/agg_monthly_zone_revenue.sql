-- models/marts/agg_monthly_zone_revenue.sql
-- Câu hỏi kinh doanh 2: revenue/tip theo zone/tháng
-- Grain: 1 row = 1 (pickup_year, pickup_month, pu_borough, service_type)

select
    pickup_year,
    pickup_month,
    date_trunc(pickup_date, month)      as pickup_month_date,
    service_type,
    pu_borough,
    pu_zone,

    -- Volume
    count(*)                            as trip_count,

    -- Revenue
    round(sum(total_amount), 2)         as total_revenue,
    round(avg(total_amount), 2)         as avg_revenue_per_trip,
    round(avg(fare_amount), 2)          as avg_fare,
    round(sum(tip_amount), 2)           as total_tips,
    round(avg(tip_percentage), 2)       as avg_tip_percentage,
    round(avg(revenue_per_mile), 2)     as avg_revenue_per_mile,

    -- Distance & duration
    round(avg(trip_distance), 2)        as avg_trip_distance,
    round(avg(trip_duration_minutes), 2) as avg_duration_minutes

from {{ ref('int_trips_enriched') }}
where pu_borough is not null
group by 1, 2, 3, 4, 5, 6
