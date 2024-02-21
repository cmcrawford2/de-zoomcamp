{{ config(materialized="table") }}

with
    tripdata as (
        select * from {{ ref("stg_fhv_tripdata") }}
    ),
    dim_zones as (select * from {{ ref("dim_zones") }} where borough != 'Unknown')
select
    *
from tripdata
inner join
    dim_zones as pickup_zone on tripdata.pickup_locationid = pickup_zone.locationid
inner join
    dim_zones as dropoff_zone
    on tripdata.dropoff_locationid = dropoff_zone.locationid

