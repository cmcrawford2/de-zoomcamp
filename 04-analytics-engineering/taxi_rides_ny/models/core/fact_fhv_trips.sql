{{ config(materialized="table") }}

with
    tripdata as (
        select * from {{ ref("stg_fhv_tripdata") }}
    ),
    fhv_zones as (select * from {{ ref("fhv_zones") }} where borough != 'Unknown')
select
    tripdata.dispatching_base_num,
    tripdata.pickup_datetime,
    tripdata.dropoff_datetime,
    tripdata.pickup_locationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    tripdata.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    tripdata.sr_flag,
    tripdata.affiliated_base_number
from tripdata
inner join
    fhv_zones as pickup_zone on tripdata.pickup_locationid = pickup_zone.locationid
inner join
    fhv_zones as dropoff_zone
    on tripdata.dropoff_locationid = dropoff_zone.locationid

