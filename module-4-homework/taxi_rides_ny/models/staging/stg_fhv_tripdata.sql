{{ config(materialized="view") }}

with

    tripdata as (

        select *
        from {{ source("staging", "fhv_tripdata_2019") }}
        where
            dispatching_base_num is not null
            and pickup_datetime >= '2018-12-31'
            and dropoff_datetime <= '2020-01-01'
    )

select

    -- identifiers
    {{ dbt_utils.generate_surrogate_key(["dispatching_base_num", "pickup_datetime"]) }}
    as tripid,
    dispatching_base_num,
    affiliated_base_number,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }}
    as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }}
    as dropoff_locationid,

    -- timestamps             
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- tripinfo
    cast(sr_flag as string) as store_and_fwd_flag

from tripdata

{% if var("is_test_run", default=true) %} limit 100 {% endif %}
