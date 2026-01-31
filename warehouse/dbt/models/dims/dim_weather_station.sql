{{ config(materialized='table') }}

with stations as (
  select distinct station_id, station_name
  from {{ ref('stg_noaa_weather_daily') }}
)
select
  row_number() over (order by station_id) as station_key,
  station_id,
  station_name,
  null::integer as country_key,
  null::double as latitude,
  null::double as longitude
from stations
