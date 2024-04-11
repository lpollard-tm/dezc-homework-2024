-- Define a model for staging earthquake data
{{
    config(
        materialized="view",
        partition_by={
            "field": "date_partition",
            "data_type": "date",
            "granularity": "day",
            cluster_by: ["mag_cluster", "locationSource", "net"],
        },
    )
}}

with
source_data as (
        select *, row_number() over (partition by event_date, latitude, longitude, time order by event_date) as rn
        from {{ source("staging", "usgs_data_raw_2024") }}
        where event_date is not null
    )
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(["id", "event_datetime"]) }} as unique_event_id,
    {{ dbt.safe_cast("id", "varchar") }} as event_id,
    {{ dbt.safe_cast("time", "timestamp") }} as event_datetime,

    {{ dbt.safe_cast("updated", "timestamp") }} as last_updated,
    -- cast(updated as timestamp('%d-%m-%Y%-H:%M:%S'))
    cast(time as timestamp('%Y-%m-%d')) as event_date,
    cast(time as timestamp('%H:%M:%S')) as event_time,
    -- format_timestamp('%Y-%m-%d', time) as date,
    -- format_timestamp('%H:%M:%S', time) as time,
    -- location
    {{ dbt.safe_cast("latitude", "float") }} as event_latitude,
    {{ dbt.safe_cast("longitude", "float") }} as event_longitude,
    -- get country from lat/lon
    -- get city/region from lat/lon
    -- seismic data
    cast(depth as numeric) as depth_km,
    cast(mag as numeric) as event_magnitude,
    -- create mag_alert level from mag
    cast(magtype as varchar) as magnitude_conversion_type,
    -- convert abbreviations to the actual converstion type 
    cast(nst as numeric) as no_stations_to_calculate_location,
    cast(gap as decimal) as largest_gap_between_stations,
    cast(dmin as decimal) as dist_epicenter_to_station_degrees,
    cast(rms as decimal) as root_mean_square_residual_seconds,
    cast(net as varchar) as data_contributor_network_id,
    -- convert abbreviations to the name of the network place
    cast(place as varchar) as descriptive_geographic_region,
    cast(type as varchar) as event_type,
    -- try to make these uniform, need to inspect data
    cast(horizontalerror as decimal) as horizontal_location_error_km,
    cast(deptherror as decimal) as depth_uncertainty_km,
    cast(magerror as decimal) as magnitude_uncertainty,
    cast(magnst as numeric) as no_stations_to_calculate_magnitude,
    cast(status as varchar) as event_review_status,
    cast(locationsource as varchar) as network_reported_by,
    cast(magsource as varchar) as network_magnitude_author

from source_data
where rn = 1

{% if var("is_test_run", default=true) %} limit 100 {% endif %}
