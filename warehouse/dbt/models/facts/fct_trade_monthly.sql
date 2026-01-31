{{ config(materialized='table') }}

select
  s.month_key,
  c_rep.country_key as reporter_country_key,
  c_par.country_key as partner_country_key,
  com.commodity_key,
  s.trade_flow,
  sum(s.trade_value_usd) as trade_value_usd,
  sum(s.net_weight_kg) as net_weight_kg,
  sum(s.quantity) as quantity,
  max(s.quantity_unit) as quantity_unit
from {{ ref('stg_comtrade_monthly') }} s
left join {{ ref('dim_country') }} c_rep on c_rep.iso3 = s.reporter_iso3
left join {{ ref('dim_country') }} c_par on c_par.iso3 = s.partner_iso3
left join {{ ref('dim_commodity') }} com on com.hs_code = s.hs_code
group by 1, 2, 3, 4, 5
