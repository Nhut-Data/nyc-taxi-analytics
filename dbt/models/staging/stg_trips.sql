-- models/staging/stg_trips.sql
-- Staging layer: alias/rename nhẹ từ conformed table
-- KHÔNG join lại, KHÔNG filter thêm, KHÔNG business logic
-- Spark đã lo toàn bộ ở conform job

with source as (
    select * from {{ source('nyc_taxi_conformed', 'trips') }}
)

select
    -- IDs
    vendor_id,
    service_type,

    -- Datetime
    pickup_datetime,
    dropoff_datetime,
    pickup_date,

    -- Trip info
    passenger_count,
    trip_distance,

    -- Location (đã join zone lookup từ Spark)
    pu_location_id,
    do_location_id,
    pu_borough,
    pu_zone,
    do_borough,
    do_zone,

    -- Rate & payment
    ratecode_id,
    store_and_fwd_flag,
    payment_type,

    -- Fare components
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    total_amount,

    -- Metadata
    ingestion_date,
    source_file

from source
