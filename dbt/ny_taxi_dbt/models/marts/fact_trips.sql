{{ config(materialized='incremental', unique_key='trip_id') }}

with yellow as (

    select
        trip_id,
        'yellow' as taxi_type,
        pickup_ts,
        dropoff_ts,
        pickup_date,
        pickup_hour,
        pu_location_id,
        do_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        total_amount
    from {{ ref('stg_yellow_trips') }}

),

green as (

    select
        trip_id,
        'green' as taxi_type,
        pickup_ts,
        dropoff_ts,
        pickup_date,
        pickup_hour,
        pu_location_id,
        do_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        total_amount
    from {{ ref('stg_green_trips') }}

),

fhv as (

    select
        trip_id,
        'fhv' as taxi_type,
        pickup_ts,
        dropoff_ts,
        pickup_date,
        pickup_hour,
        pu_location_id,
        do_location_id,
        cast(null as int64)   as passenger_count,
        cast(null as float64) as trip_distance,
        cast(null as numeric) as fare_amount,
        cast(null as numeric) as total_amount
    from {{ ref('stg_fhv_trips') }}

)

select * from yellow
union all
select * from green
union all
select * from fhv
