from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="monthly_ingest_transform",
    start_date=datetime(2025, 1, 1),
    schedule="@monthly",
    catchup=False,
) as dag:
    ingest_comtrade = BashOperator(
        task_id="ingest_comtrade",
        bash_command="python -m pipelines.ingestion.cli ingest --source comtrade --date {{ ds }}",
        cwd="/opt/pipelines",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/dbt && dbt deps && dbt run",
    )

    ingest_comtrade >> dbt_run
