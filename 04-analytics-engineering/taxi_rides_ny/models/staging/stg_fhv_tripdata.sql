{{ config(materialized="view") }}

with
    tripdata as (
        select *, row_number() over (partition by dispatching_base_num, pickup_datetime) as rn
        from {{ source("staging", "fhv_tripdata") }}
        where dispatching_base_num is not null
    )
select
    -- identifiers
    {{ dbt_utils.surrogate_key(["dispatching_base_num", "pickup_datetime"]) }} as tripid,
    {{ dbt.safe_cast("dispatching_base_num", api.Column.translate_type("string")) }} as dispatching_base_num,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    {{ dbt.safe_cast("sr_flag", api.Column.translate_type("string")) }} as sr_flag,
    {{ dbt.safe_cast("affiliated_base_number", api.Column.translate_type("string")) }} as affiliated_base_number,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
from tripdata
where pickup_datetime > "2018-12-31 23:59:59" and pickup_datetime < "2020-01-01 00:00:00"
and rn = 1

-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var("is_test_run", default=true) %} limit 100 {% endif %}
