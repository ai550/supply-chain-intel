{{ config(materialized='view') }}

with raw as (
  select * from read_parquet({{ s3_raw_path('port_ops') }} || '**/*.parquet')
),
clean as (
  select
    cast(strftime(date, '%Y%m%d') as int) as date_key,
    port_name,
    cast(teu_total as double) as teu_total,
    cast(vessel_calls as int) as vessel_calls,
    cast(avg_vessel_wait_hours as double) as avg_vessel_wait_hours,
    cast(terminal_dwell_days as double) as terminal_dwell_days,
    cast(rail_dwell_days as double) as rail_dwell_days,
    cast(truck_turn_time_min as double) as truck_turn_time_min,
    cast(source_record_count as int) as source_record_count,
    cast(source_min_ts as timestamp) as source_min_ts,
    cast(source_max_ts as timestamp) as source_max_ts
  from raw
)
select * from clean
