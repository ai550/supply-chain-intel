-- Tile 11: Pipeline Health (freshness + latency)
-- Chart type: Table
-- Dataset: mart_pipeline_health

select run_date, source_name, status, latency_hours, rows_loaded, error_count
from mart_pipeline_health
order by run_date desc, source_name;
