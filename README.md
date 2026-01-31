# Supply Chain Disruption Intelligence

End-to-end data platform that ingests public supply-chain signals (port operations, trade flows, geopolitical events, weather), builds a composite **Port Strain Index**, and surfaces a 12-tile Superset dashboard for disruption monitoring.

## Stack

| Component | Role |
|-----------|------|
| **MinIO** | S3-compatible data lake (raw Parquet) |
| **DuckDB** | Analytical warehouse |
| **Airflow** | Orchestration (daily + monthly DAGs) |
| **dbt** (dbt-duckdb) | Staging, dims, facts, marts |
| **Superset** | BI dashboard |
| **Postgres** | Metadata DB for Airflow & Superset |

## Data Sources

| Source | Frequency | Description |
|--------|-----------|-------------|
| Port Ops | Daily | Container throughput, vessel wait times, dwell times |
| UN Comtrade | Monthly | Bilateral trade flows (HS-code level) |
| GDELT | Daily | Geopolitical event counts and tone scores |
| NOAA CDO | Daily | Weather observations near major ports |

## Project Structure

```
supply-chain-intel/
├── docker/                  # Dockerfiles for airflow, dbt, superset
├── docker-compose.yml       # Full stack: postgres, minio, airflow, dbt, superset
├── data/duckdb/             # Persisted DuckDB warehouse file
├── infra/                   # MinIO init scripts, Airflow connection bootstrap
├── pipelines/
│   ├── ingestion/           # Extract + normalize + load (per source)
│   ├── orchestration/       # Airflow DAGs
│   └── quality/             # SQL checks and expectations
├── warehouse/dbt/           # dbt project (staging → dims → facts → marts)
└── analytics/superset/      # Dashboard tile specs and SQL
```

## Prerequisites

- Docker and Docker Compose
- Python 3.11+ (for local linting/formatting)
- [Ruff](https://docs.astral.sh/ruff/) (`pip install ruff`)
- GNU Make

## Quickstart

### 1. Configure environment

```bash
cp .env.example .env
# Edit .env to set API keys (NOAA_TOKEN, COMTRADE_KEY) and secrets
```

### 2. Start all services

```bash
make up
```

This brings up:

- **MinIO** console: http://localhost:9001 (minio / minio12345)
- **Airflow** UI: http://localhost:8080 (admin / admin)
- **Superset** UI: http://localhost:8088 (admin / admin)

### 3. Run dbt

```bash
make dbt-run
```

### 4. Trigger ingestion manually (optional)

```bash
# Ingest GDELT for a specific date
make ingest-gdelt DATE=2025-01-15

# Ingest all daily sources for today
make ingest-all

# Or trigger via the Airflow UI
make airflow-trigger-daily
```

### 5. Connect Superset to DuckDB

In the Superset UI, add a new database connection:

```
duckdb:////opt/warehouse/warehouse.duckdb
```

Then create datasets from the mart tables and build charts using the SQL in `analytics/superset/sql/tiles/`.

## Makefile Reference

Run `make help` to see all available targets. Key commands:

| Command | Description |
|---------|-------------|
| `make up` | Start all services (detached) |
| `make down` | Stop all services |
| `make logs` | Tail logs for all services |
| `make lint` | Run Ruff linter checks |
| `make lint-fix` | Auto-fix lint issues |
| `make format` | Format code with Ruff |
| `make dbt-run` | Run dbt models |
| `make dbt-test` | Run dbt tests |
| `make ingest-gdelt DATE=YYYY-MM-DD` | Ingest GDELT for a date |
| `make ingest-all` | Ingest all daily sources |
| `make quality` | Run SQL quality checks |
| `make clean` | Remove artifacts and caches |
| `make nuke` | Stop containers and remove all volumes |

## Warehouse Schema

### Dimensions
- `dim_date` — date spine (2020-2030)
- `dim_country` — ISO3 countries from staging data
- `dim_port` — ports from port ops data
- `dim_commodity` — HS codes from Comtrade
- `dim_event_type` — GDELT event codes with severity weights
- `dim_weather_station` — NOAA stations

### Facts
- `fct_port_ops_daily` — daily port throughput, wait times, dwell times
- `fct_trade_monthly` — monthly bilateral trade values
- `fct_disruption_daily` — daily GDELT disruption scores
- `fct_weather_daily` — daily weather with anomaly scores

### Marts
- `mart_port_strain_daily` — composite strain index with 14d disruption and 7d weather rollups
- `mart_trade_impact_monthly` — trade values with MoM/YoY changes, strain/disruption context
- `mart_event_impact_windows` — pre/post event strain delta analysis (14d windows)
- `mart_pipeline_health` — ingestion run status, latency, error counts

## Strain Index

```
strain_index =
    ln(1 + teu_total)        * 0.25
  + terminal_dwell_days      * 0.30
  + avg_vessel_wait_hours    * 0.25
  + truck_turn_time_min      * 0.10
  + disruption_score_14d     * 0.07
  + weather_anomaly_7d       * 0.03
```

| Bucket | Range | Interpretation |
|--------|-------|----------------|
| Low | < 2 | Normal operations |
| Medium | 2 - 4 | Elevated activity; monitor |
| High | 4 - 6 | Congestion; potential delays |
| Critical | >= 6 | Severe strain; disruptions likely |

## Dashboard Tiles

1. Port Strain Index (time series)
2. Strain Bucket Distribution (last 30d)
3. Components of Strain (multi-series)
4. Disruption Score vs Strain (lag 0-14d)
5. Top Disruption Event Types (90d)
6. Weather Anomaly vs Strain (7d rolling)
7. Trade Value Trend (by commodity)
8. Trade MoM/YoY Volatility
9. Impact Windows Leaderboard
10. Port Strain Heatmap (month x port)
11. Pipeline Health
12. Explain the Index (documentation panel)

## Airflow DAGs

| DAG | Schedule | What it does |
|-----|----------|--------------|
| `daily_ingest_transform` | `@daily` | Ingest port_ops, GDELT, NOAA in parallel, then `dbt run` |
| `monthly_ingest_transform` | `@monthly` | Ingest Comtrade, then `dbt run` |
| `backfill` | Manual | Ingest a date range for any source, then `dbt run` |

## Data Quality

- SQL checks: `pipelines/quality/checks.sql` (null PKs, referential integrity, value ranges, freshness)
- Expectations doc: `pipelines/quality/expectations.md`
- dbt tests: `warehouse/dbt/models/schema.yml` (not_null, unique, accepted_values)

```bash
make quality      # SQL checks against DuckDB
make quality-dbt  # dbt schema + data tests
```

## Development

### Linting and Formatting

This project uses [Ruff](https://docs.astral.sh/ruff/) for linting and formatting, configured in `pyproject.toml`.

```bash
make lint         # Check for issues
make lint-fix     # Auto-fix issues
make format       # Format code
make format-check # Check formatting without modifying
```

### Cleanup

```bash
make clean  # Remove DuckDB file, dbt target, __pycache__
make nuke   # Stop containers, remove all volumes, and clean
```

## License

MIT
