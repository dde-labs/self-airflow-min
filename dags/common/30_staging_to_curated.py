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
    dag_id="30_STG_TO_CURATED",
    start_date=pm.datetime(2024, 8, 8),
    schedule=None,
    catchup=False,
    description="Common DAG for move data from Staging to Curated",
    tags=["process", "common"],
    params={
        "process": Param(
            {
                "connection_id": "file-storage-connection-id",
                "file": {
                    "path": "file-path",
                    "name": "file-dynamic-name",
                }
            },
            type=["object", "null"],
            section="Important Params",
            description=(
                "Enter your process data that want to move staging to curated."
            ),
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
def staging_to_curated():
    start = EmptyOperator(task_id='start')

    @task()
    def move_file_to_curated():
        context = get_current_context()
        logging.info(context["params"])
        logging.info(context["ds"])

    external_table = EmptyOperator(task_id='create-external-table')

    start >> move_file_to_curated() >> external_table


staging_to_curated()
