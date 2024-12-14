from __future__ import annotations

import logging
from datetime import timedelta

import pendulum as pm
from airflow.decorators import dag, task
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context

from plugins.callback import process_failure_callback

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": process_failure_callback,
}


@dag(
    dag_id="20_PROCESS_TYPE3",
    start_date=pm.datetime(2024, 8, 8),
    schedule=None,
    catchup=False,
    description="Common DAG for DB process",
    tags=["process", "common"],
    params={
        "process": Param(
            {
                "connection_id": "database-connection-id",
                "source_schema_name": "source-schema-name",
            },
            type=["object", "null"],
            section="Important Params",
            description=(
                "Enter your process data that want to start process database."
            ),
        ),
        "asat_dt": Param(
            default=str(pm.now(tz="Asia/Bangkok") - timedelta(days=1)),
            type="string",
            format="date-time",
            section="Important Params",
            description="Enter your asat date that you want.",
        ),
    },
    default_args=default_args,
)
def process_gbq():
    start = EmptyOperator(task_id="start")

    @task()
    def extract_gbq():
        context = get_current_context()
        logging.info(context["params"])
        logging.info(context["ds"])

    @task(task_id="transform")
    def execute_transform():
        return

    @task(task_id="return_result")
    def prepare_result_for_parent_dag():
        return {
            "cnt_rec_src": 30000,
            "cnt_rec_tgt": 30000,
        }

    (
        start
        >> extract_gbq()
        >> execute_transform()
        >> prepare_result_for_parent_dag()
    )


process_gbq()
