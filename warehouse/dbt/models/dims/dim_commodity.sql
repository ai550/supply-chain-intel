{{ config(materialized='table') }}

with codes as (
  select distinct hs_code
  from {{ ref('stg_comtrade_monthly') }}
  where hs_code is not null
)
select
  row_number() over (order by hs_code) as commodity_key,
  hs_code,
  case
    when length(hs_code) <= 2 then 2
    when length(hs_code) <= 4 then 4
    else 6
  end as hs_level,
  null::varchar as commodity_name
from codes
