{{ config(materialized='view') }}

with raw as (
  select * from read_parquet({{ s3_raw_path('noaa') }} || '**/*.parquet')
),
clean as (
  select
    cast(strftime(date, '%Y%m%d') as int) as date_key,
    station_id,
    station_name,
    cast(temp_avg_c as double) as temp_avg_c,
    cast(precipitation_mm as double) as precipitation_mm,
    cast(wind_avg_mps as double) as wind_avg_mps,
    cast(storm_flag as boolean) as storm_flag
  from raw
)
select * from clean
