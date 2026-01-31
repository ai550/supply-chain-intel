import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "this_is_not_secure_change_me")

SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SQLALCHEMY_DATABASE_URI",
    "postgresql+psycopg2://meta:meta@postgres_meta:5432/superset",
)

# DuckDB warehouse connection string for Superset datasets
# Add via Superset UI: duckdb:////opt/warehouse/warehouse.duckdb
PREVENT_UNSAFE_DB_CONNECTIONS = False

WTF_CSRF_ENABLED = False
