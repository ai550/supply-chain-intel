{{ config(materialized='view') }}

with raw as (
  select * from read_parquet({{ s3_raw_path('comtrade') }} || '**/*.parquet')
),
clean as (
  select
    cast(period as int) as month_key,
    reporter_iso3,
    partner_iso3,
    hs_code,
    trade_flow,
    cast(trade_value_usd as double) as trade_value_usd,
    cast(net_weight_kg as double) as net_weight_kg,
    cast(quantity as double) as quantity,
    quantity_unit
  from raw
)
select * from clean
