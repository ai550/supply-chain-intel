{{ config(materialized='table') }}

select
  current_date as run_date,
  source_name,
  status,
  last_partition_loaded,
  rows_loaded,
  error_count,
  latency_hours
from read_parquet({{ s3_raw_path('manifests') }} || '**/*.parquet')
