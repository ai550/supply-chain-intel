{{ config(materialized='table') }}

with date_spine as (
  select
    unnest(generate_series(date '2020-01-01', date '2030-12-31', interval 1 day)) as date
)
select
  cast(strftime(date, '%Y%m%d') as int) as date_key,
  date,
  extract(year from date)::int as year,
  extract(quarter from date)::int as quarter,
  extract(month from date)::int as month,
  extract(day from date)::int as day,
  extract(week from date)::int as week,
  extract(isodow from date)::int as day_of_week,
  extract(isodow from date) in (6, 7) as is_weekend
from date_spine
