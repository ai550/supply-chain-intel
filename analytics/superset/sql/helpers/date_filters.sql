-- Helper: common date filter CTEs for Superset virtual datasets

-- Last N days filter
-- Usage: join or WHERE d.date >= current_date - interval {N} day

-- Month key from date
-- cast(substr(cast(date_key as varchar), 1, 6) as int) as month_key

-- Date from date_key
-- select d.date from dim_date d where d.date_key = <date_key>
