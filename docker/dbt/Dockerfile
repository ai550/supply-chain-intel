FROM python:3.11-slim

RUN pip install --no-cache-dir \
    dbt-duckdb>=1.7 \
    duckdb>=0.10 \
    pyarrow>=14.0

COPY profiles.yml /root/.dbt/profiles.yml

WORKDIR /opt/dbt
