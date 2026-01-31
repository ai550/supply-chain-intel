-- =============================================================================
-- Data quality checks (run against DuckDB warehouse)
-- =============================================================================

-- 1. No null primary keys in facts
select 'fct_port_ops_daily: null date_key' as check_name,
       count(*) as failures
from fct_port_ops_daily where date_key is null;

select 'fct_port_ops_daily: null port_key' as check_name,
       count(*) as failures
from fct_port_ops_daily where port_key is null;

select 'fct_trade_monthly: null month_key' as check_name,
       count(*) as failures
from fct_trade_monthly where month_key is null;

select 'fct_disruption_daily: null date_key' as check_name,
       count(*) as failures
from fct_disruption_daily where date_key is null;

select 'fct_weather_daily: null date_key' as check_name,
       count(*) as failures
from fct_weather_daily where date_key is null;

-- 2. Referential integrity: fact FK -> dim PK
select 'fct_port_ops_daily: orphan port_key' as check_name,
       count(*) as failures
from fct_port_ops_daily f
left join dim_port p on p.port_key = f.port_key
where p.port_key is null;

select 'fct_disruption_daily: orphan event_type_key' as check_name,
       count(*) as failures
from fct_disruption_daily f
left join dim_event_type et on et.event_type_key = f.event_type_key
where et.event_type_key is null;

-- 3. Value range checks
select 'fct_port_ops_daily: negative teu_total' as check_name,
       count(*) as failures
from fct_port_ops_daily where teu_total < 0;

select 'fct_trade_monthly: negative trade_value_usd' as check_name,
       count(*) as failures
from fct_trade_monthly where trade_value_usd < 0;

select 'mart_port_strain_daily: strain_index out of range' as check_name,
       count(*) as failures
from mart_port_strain_daily where strain_index < 0;

-- 4. Freshness: most recent partition per source
select 'freshness: fct_port_ops_daily' as check_name,
       max(date_key) as latest_partition
from fct_port_ops_daily;

select 'freshness: fct_disruption_daily' as check_name,
       max(date_key) as latest_partition
from fct_disruption_daily;

select 'freshness: fct_weather_daily' as check_name,
       max(date_key) as latest_partition
from fct_weather_daily;

select 'freshness: fct_trade_monthly' as check_name,
       max(month_key) as latest_partition
from fct_trade_monthly;

-- 5. Row-count sanity
select 'row_count: dim_date' as check_name, count(*) as cnt from dim_date;
select 'row_count: dim_country' as check_name, count(*) as cnt from dim_country;
select 'row_count: dim_port' as check_name, count(*) as cnt from dim_port;
select 'row_count: dim_commodity' as check_name, count(*) as cnt from dim_commodity;
