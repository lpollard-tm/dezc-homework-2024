{{ config(materialized="table") }}

with
    fhv_tripdata_2019 as (
        select *, 'fhv' as service_type from {{ ref("stg_fhv_tripdata") }}
    ),
    dim_zones as (select * from {{ ref("dim_zones") }} where borough != 'Unknown')
select
    tripid,
    service_type,
    dispatching_base_num,
    affiliated_base_number,
    pickup_locationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_locationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    pickup_datetime,
    dropoff_datetime,
    store_and_fwd_flag
from fhv_tripdata_2019
inner join dim_zones as pickup_zone on pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone on dropoff_locationid = dropoff_zone.locationid
