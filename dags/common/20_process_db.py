import logging
from datetime import timedelta

import pendulum as pm
from airflow.decorators import dag, task
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="20_PROCESS_DB",
    start_date=pm.datetime(2024, 8, 8),
    schedule=None,
    catchup=False,
    description="Common DAG for DB process",
    tags=["process", "common"],
    params={
        "source": Param(
            type="string",
            section="Important Params",
            description="Enter your process name.",
        ),
        "asat_dt": Param(
            default=str(pm.now(tz='Asia/Bangkok') - timedelta(days=1)),
            type="string",
            format="date-time",
            section="Important Params",
            description="Enter your asat date that you want.",
        ),
    },
    default_args=default_args,
)
def process_db():
    start = EmptyOperator(task_id='start')

    @task()
    def extract_db():
        context = get_current_context()
        logging.info(context["params"])
        logging.info(context["ds"])

    prepare_db = EmptyOperator(task_id='prepare-db')

    start >> extract_db() >> prepare_db


process_db()
