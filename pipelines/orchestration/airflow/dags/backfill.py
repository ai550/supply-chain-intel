"""Backfill DAG: manually triggered to ingest a date range for a given source.

Trigger with config:
    {"source": "gdelt", "start_date": "2024-06-01", "end_date": "2024-06-30"}
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def _generate_dates(**context):
    conf = context["dag_run"].conf or {}
    start = conf.get("start_date", context["ds"])
    end = conf.get("end_date", context["ds"])
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    dates = []
    cur = start_dt
    while cur <= end_dt:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    context["ti"].xcom_push(key="dates", value=dates)
    context["ti"].xcom_push(key="source", value=conf.get("source", "gdelt"))


with DAG(
    dag_id="backfill",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # manual trigger only
    catchup=False,
) as dag:
    generate = PythonOperator(
        task_id="generate_dates",
        python_callable=_generate_dates,
    )

    run_backfill = BashOperator(
        task_id="run_backfill",
        bash_command="""
            set -e
            SOURCE={{ ti.xcom_pull(task_ids='generate_dates', key='source') }}
            DATES='{{ ti.xcom_pull(task_ids='generate_dates', key='dates') }}'
            for D in $(echo "$DATES" | python3 -c "import sys,json; [print(d) for d in json.load(sys.stdin)]"); do
                echo "Ingesting $SOURCE for $D"
                python -m pipelines.ingestion.cli ingest --source "$SOURCE" --date "$D"
            done
        """,
        cwd="/opt/pipelines",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/dbt && dbt deps && dbt run",
    )

    generate >> run_backfill >> dbt_run
