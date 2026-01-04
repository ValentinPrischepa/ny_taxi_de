{{ config(materialized='view') }}

select
    to_hex(md5(concat(
            cast(dispatching_base_num as string),
            cast(pickup_datetime as string),
            cast(pulocationid as string),
            cast(dolocationid as string)
        ))) as trip_id,
    cast(pickup_datetime as timestamp) as pickup_ts,
    cast(dropoff_datetime as timestamp) as dropoff_ts,
    date(pickup_datetime) as pickup_date,
    extract(hour from pickup_datetime) as pickup_hour,
    cast(pulocationid as int64) as pu_location_id,
    cast(dolocationid as int64) as do_location_id,
    SR_Flag,
    cast(Affiliated_base_number as string) as affiliated_base_number

    from {{ source('ny_taxi_raw', 'fhv_trips') }}