{{ config(materialized='table') }}

with ports as (
  select distinct port_name
  from {{ ref('stg_port_ops_daily') }}
)
select
  row_number() over (order by port_name) as port_key,
  port_name,
  null::varchar as port_code,
  null::integer as country_key,
  null::double as latitude,
  null::double as longitude,
  null::varchar as timezone
from ports
