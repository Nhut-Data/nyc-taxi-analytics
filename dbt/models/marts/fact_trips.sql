-- models/marts/fact_trips.sql
-- Grain: 1 row = 1 chuyến taxi
-- Câu hỏi 1 + 2: demand và revenue analysis
{{ config(
    partition_by={'field': 'pickup_date', 'data_type': 'date'},
    cluster_by=['pu_borough']
) }}

select
    -- Keys
    vendor_id,
    service_type,
    pu_location_id,
    do_location_id,
    pu_borough,
    pu_zone,
    do_borough,
    do_zone,
    payment_type,
    ratecode_id,

    -- Datetime dimensions
    pickup_datetime,
    dropoff_datetime,
    pickup_date,
    pickup_hour,
    day_of_week,
    pickup_month,
    pickup_year,
    time_of_day,

    -- Trip metrics
    passenger_count,
    trip_distance,
    trip_duration_minutes,

    -- Revenue metrics
    fare_amount,
    tip_amount,
    tolls_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    total_amount,
    tip_percentage,
    revenue_per_mile,

    -- Metadata
    ingestion_date,
    source_file

from {{ ref('int_trips_enriched') }}
