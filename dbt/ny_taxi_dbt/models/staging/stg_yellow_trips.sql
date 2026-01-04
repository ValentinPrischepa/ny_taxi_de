{{ config(materialized='view') }}

select
    to_hex(md5(
        concat(
            cast(tpep_pickup_datetime as string),
            cast(tpep_dropoff_datetime as string),
            cast(pulocationid as string),
            cast(dolocationid as string)
        )
    )) as trip_id,
    cast(tpep_pickup_datetime as timestamp) as pickup_ts,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_ts,
    date(tpep_pickup_datetime) as pickup_date,
    extract(hour from tpep_pickup_datetime) as pickup_hour,
    cast(pulocationid as int64) as pu_location_id,
    cast(dolocationid as int64) as do_location_id,
    cast(passenger_count as int64) as passenger_count,
    cast(ratecodeid as int64) as ratecode_id,
    cast(store_and_fwd_flag as string) as store_and_fwd_flag,
    cast(payment_type as int64) as payment_type,
    cast(congestion_surcharge as float64) as congestion_surcharge,
    cast(trip_distance as float64) as trip_distance,
    cast(extra as float64) as extra,
    cast(mta_tax as float64) as mta_tax,
    cast(tip_amount as float64) as tip_amount,
    cast(tolls_amount as float64) as tolls_amount,
    cast(improvement_surcharge as float64) as improvement_surcharge,
    cast(airport_fee as float64) as airport_fee,
    cast(fare_amount as float64) as fare_amount,
    cast(total_amount as float64) as total_amount

from {{ source('ny_taxi_raw', 'yellow_trips') }}
