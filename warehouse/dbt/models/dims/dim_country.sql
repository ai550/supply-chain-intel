{{ config(materialized='table') }}

-- Seed from distinct countries in staging data; enrich later with a static CSV
with gdelt_countries as (
  select distinct country_iso3 as iso3
  from {{ ref('stg_gdelt_events_daily') }}
  where country_iso3 is not null
),
comtrade_reporters as (
  select distinct reporter_iso3 as iso3 from {{ ref('stg_comtrade_monthly') }}
  union
  select distinct partner_iso3 as iso3 from {{ ref('stg_comtrade_monthly') }}
),
all_countries as (
  select iso3 from gdelt_countries
  union
  select iso3 from comtrade_reporters
  where iso3 is not null
)
select
  row_number() over (order by iso3) as country_key,
  null::varchar as iso2,
  iso3,
  iso3 as country_name,   -- placeholder; enrich with a lookup
  null::varchar as region,
  null::varchar as subregion
from all_countries
