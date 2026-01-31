from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="daily_ingest_transform",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    ingest_port = BashOperator(
        task_id="ingest_port_ops",
        bash_command="python -m pipelines.ingestion.cli ingest --source port_ops --date {{ ds }}",
        cwd="/opt/pipelines",
    )

    ingest_gdelt = BashOperator(
        task_id="ingest_gdelt",
        bash_command="python -m pipelines.ingestion.cli ingest --source gdelt --date {{ ds }}",
        cwd="/opt/pipelines",
    )

    ingest_noaa = BashOperator(
        task_id="ingest_noaa",
        bash_command="python -m pipelines.ingestion.cli ingest --source noaa --date {{ ds }}",
        cwd="/opt/pipelines",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/dbt && dbt deps && dbt run",
    )

    [ingest_port, ingest_gdelt, ingest_noaa] >> dbt_run
