import logging
from datetime import timedelta

import pendulum as pm
from airflow.decorators import dag, task
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="20_PROCESS_TYPE2",
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
            default=str(pm.now(tz='Asia/Bangkok') - timedelta(days=1)),
            type="string",
            format="date-time",
            section="Important Params",
            description="Enter your asat date that you want.",
        ),
    },
    default_args=default_args,
    render_template_as_native_obj=True,
)
def process_api():
    start = EmptyOperator(task_id='start')

    @task()
    def extract_api():
        context = get_current_context()
        logging.info(context["params"])
        logging.info(context["ds"])

    prepare_api = EmptyOperator(task_id='prepare-api')

    start >> extract_api() >> prepare_api

    staging_to_curated = TriggerDagRunOperator(
        task_id='staging-to-curated',
        trigger_dag_id="30_STG_TO_CURATED",
        trigger_run_id="{{ run_id }}",
        wait_for_completion=True,
        deferrable=False,
        reset_dag_run=True,
        conf={
            "process": "{{ params['process'] }}",
            "asat_dt": "{{ params['asat_dt'] }}",
        },
    )

    prepare_api >> staging_to_curated


process_api()
