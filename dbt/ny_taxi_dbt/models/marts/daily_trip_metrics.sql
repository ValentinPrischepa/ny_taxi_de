
{{ config(materialized='table') }}

with src as (
    select
        pickup_date,
        taxi_type,
        passenger_count,
        trip_distance,
        fare_amount,
        total_amount,
        pu_location_id,
        do_location_id
    from {{ ref('fact_trips') }}
),

by_type as (
    select
        pickup_date,
        taxi_type,
        count(*)                                  as total_trips,
        sum(total_amount)                         as total_amount_sum,
        sum(fare_amount)                          as fare_amount_sum,
        avg(total_amount)                         as avg_total_amount,
        avg(fare_amount)                          as avg_fare_amount,
        sum(trip_distance)                        as total_distance,
        avg(trip_distance)                        as avg_trip_distance,
        avg(passenger_count)                      as avg_passenger_count,
        count(distinct pu_location_id)            as distinct_pu_locations,
        count(distinct do_location_id)            as distinct_do_locations
    from src
    group by pickup_date, taxi_type
),

overall as (
    select
        pickup_date,
        'all' as taxi_type,
        count(*)                                  as total_trips,
        sum(total_amount)                         as total_amount_sum,
        sum(fare_amount)                          as fare_amount_sum,
        avg(total_amount)                         as avg_total_amount,
        avg(fare_amount)                          as avg_fare_amount,
        sum(trip_distance)                        as total_distance,
        avg(trip_distance)                        as avg_trip_distance,
        avg(passenger_count)                      as avg_passenger_count,
        count(distinct pu_location_id)            as distinct_pu_locations,
        count(distinct do_location_id)            as distinct_do_locations
    from src
    group by pickup_date
)

select * from by_type
union all
select * from overall