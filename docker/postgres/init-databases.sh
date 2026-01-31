#!/bin/bash
set -e

# Create the superset database (Airflow DB is created via POSTGRES_DB env var)
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE superset;
EOSQL
