-- models/marts/agg_hourly_demand_by_zone.sql
-- Câu hỏi kinh doanh 1: demand theo giờ/zone
-- Grain: 1 row = 1 (pickup_date, pickup_hour, pu_borough, service_type)

select
    pickup_date,
    pickup_year,
    pickup_month,
    pickup_hour,
    day_of_week,
    time_of_day,
    service_type,
    pu_borough,
    pu_zone,
    count(*)                            as trip_count,
    sum(passenger_count)                as total_passengers,
    round(avg(trip_distance), 2)        as avg_trip_distance,
    round(avg(trip_duration_minutes), 2) as avg_duration_minutes

from {{ ref('int_trips_enriched') }}
where pu_borough is not null
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
