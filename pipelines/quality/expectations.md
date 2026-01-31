# Data Quality Expectations

## Fact Tables

### fct_port_ops_daily
- `date_key` and `port_key` are never null (composite PK)
- `teu_total` >= 0 when present
- `vessel_calls` >= 0 when present
- `source_record_count` >= 1
- No duplicate (date_key, port_key) pairs

### fct_trade_monthly
- `month_key` format YYYYMM, range 200001-209912
- `trade_flow` in ('import', 'export')
- `trade_value_usd` >= 0 when present
- `reporter_country_key` references dim_country

### fct_disruption_daily
- `date_key` and `event_type_key` are never null
- `event_count` >= 1
- `disruption_score` >= 0

### fct_weather_daily
- `date_key` and `station_key` are never null
- `temp_avg_c` between -90 and 60 when present
- `precipitation_mm` >= 0 when present

## Dimension Tables

### dim_date
- Complete date range with no gaps for loaded period
- `is_weekend` correctly computed (day_of_week in 6,7)

### dim_country
- `iso3` is unique and 3 characters

### dim_port
- `port_name` is unique and not null

### dim_commodity
- `hs_code` is not null
- `hs_level` in (2, 4, 6)

## Marts

### mart_port_strain_daily
- `strain_index` >= 0
- `strain_bucket` in ('low', 'medium', 'high', 'critical')
- Every row has a valid port_key in dim_port

### mart_pipeline_health
- One row per (run_date, source_name) per execution
- `status` in ('ok', 'warn', 'fail')

## Freshness

| Source | Expected Latency |
|--------|-----------------|
| port_ops | < 24 hours |
| gdelt | < 24 hours |
| noaa | < 48 hours |
| comtrade | < 45 days (monthly release lag) |
