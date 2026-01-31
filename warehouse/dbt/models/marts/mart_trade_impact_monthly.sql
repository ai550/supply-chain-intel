{{ config(materialized='table') }}

with base as (
  select
    month_key,
    reporter_country_key,
    commodity_key,
    sum(trade_value_usd) as trade_value_usd
  from {{ ref('fct_trade_monthly') }}
  group by 1, 2, 3
),
with_lags as (
  select
    b.*,
    lag(b.trade_value_usd, 1) over (
      partition by b.reporter_country_key, b.commodity_key order by b.month_key
    ) as prev_month_value,
    lag(b.trade_value_usd, 12) over (
      partition by b.reporter_country_key, b.commodity_key order by b.month_key
    ) as prev_year_value
  from base b
),
pct as (
  select
    month_key,
    reporter_country_key,
    commodity_key,
    trade_value_usd,
    case when prev_month_value > 0
      then (trade_value_usd - prev_month_value) / prev_month_value * 100
      else null
    end as trade_value_mom_pct,
    case when prev_year_value > 0
      then (trade_value_usd - prev_year_value) / prev_year_value * 100
      else null
    end as trade_value_yoy_pct
  from with_lags
),
strain_agg as (
  select
    cast(substr(cast(date_key as varchar), 1, 6) as int) as month_key,
    avg(strain_index) as avg_strain_index_month
  from {{ ref('mart_port_strain_daily') }}
  group by 1
),
disruption_agg as (
  select
    cast(substr(cast(date_key as varchar), 1, 6) as int) as month_key,
    sum(disruption_score) as disruption_score_month
  from {{ ref('fct_disruption_daily') }}
  group by 1
)
select
  p.month_key,
  p.reporter_country_key,
  p.commodity_key,
  p.trade_value_usd,
  p.trade_value_mom_pct,
  p.trade_value_yoy_pct,
  s.avg_strain_index_month,
  d.disruption_score_month
from pct p
left join strain_agg s on s.month_key = p.month_key
left join disruption_agg d on d.month_key = p.month_key
