{{ config(materialized='view') }}

with raw as (
  select *
  from read_parquet({{ s3_raw_path('gdelt') }} || '**/*.parquet')
),
clean as (
  select
    cast(strftime(date, '%Y%m%d') as int) as date_key,
    country_iso3,
    event_code,
    cast(event_count as int) as event_count,
    cast(avg_tone as double) as avg_tone
  from raw
)
select * from clean
