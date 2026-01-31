{{ config(materialized='table') }}

with events as (
  select
    date_key as event_date_key,
    country_key,
    event_type_key,
    sum(disruption_score) as disruption_score
  from {{ ref('fct_disruption_daily') }}
  group by 1, 2, 3
),
ports as (
  select date_key, avg(strain_index) as avg_strain
  from {{ ref('mart_port_strain_daily') }}
  group by 1
),
trade as (
  select
    month_key,
    sum(trade_value_usd) as trade_value
  from {{ ref('fct_trade_monthly') }}
  group by 1
),
windows as (
  select
    e.event_date_key,
    e.country_key,
    e.event_type_key,
    14 as window_days,
    e.event_date_key + 14 as window_end_date_key,

    (select avg(avg_strain) from ports p
      where p.date_key between e.event_date_key - 14 and e.event_date_key - 1
    ) as baseline_strain,

    (select avg(avg_strain) from ports p
      where p.date_key between e.event_date_key and e.event_date_key + 14
    ) as window_strain
  from events e
)
select
  *,
  window_strain - baseline_strain as strain_delta,
  null::double as baseline_trade_value,
  null::double as window_trade_value,
  null::double as trade_delta_pct
from windows
