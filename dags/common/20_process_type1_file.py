import logging
from datetime import timedelta

import pendulum as pm
from airflow.decorators import dag, task
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.edgemodifier import Label
from airflow.exceptions import AirflowFailException


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="20_PROCESS_TYPE1",
    start_date=pm.datetime(2024, 8, 8),
    schedule=None,
    catchup=False,
    description="Common DAG for File process",
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
                "Enter your process data that want to start process file."
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
def process_type01_file():
    start = EmptyOperator(task_id='start')

    @task.branch
    def switch_file_connection(**context):
        file_conn_type: str = (
            (
                context["params"].get("process", {}).get("conn", {})
                .get("service")
            ) or 'empty'
        )
        if file_conn_type not in ('onedrive', 'gcs', 's3', ):
            # NOTE: raise failed without retry
            raise AirflowFailException(
                f'File connection type from process object: {file_conn_type} '
                f'does not support or empty'
            )
        return [
            f'trigger-process-{file_conn_type.lower()}'
        ]

    trigger_onedrive = TriggerDagRunOperator(
        task_id='trigger-process-onedrive',
        trigger_dag_id='25_PROCESS_ONEDRIVE',
        trigger_run_id="{{ run_id }}",
        wait_for_completion=True,
        deferrable=False,
        reset_dag_run=True,
        conf={
            "process": "{{ params['process'] }}",
            "asat_dt": "{{ params['asat_dt'] }}",
        },
    )

    trigger_gcs = TriggerDagRunOperator(
        task_id='trigger-process-gcs',
        trigger_dag_id='25_PROCESS_GCS',
        trigger_run_id="{{ run_id }}",
        wait_for_completion=True,
        deferrable=False,
        reset_dag_run=True,
        conf={
            "process": "{{ params['process'] }}",
            "asat_dt": "{{ params['asat_dt'] }}",
        },
    )

    trigger_s3 = TriggerDagRunOperator(
        task_id='trigger-process-s3',
        trigger_dag_id='25_PROCESS_S3',
        trigger_run_id="{{ run_id }}",
        wait_for_completion=True,
        deferrable=False,
        reset_dag_run=True,
        conf={
            "process": "{{ params['process'] }}",
            "asat_dt": "{{ params['asat_dt'] }}",
        },
    )

    switch_file_format_task = switch_file_connection()

    start >> switch_file_format_task

    switch_file_format_task >> Label('ONE DRIVE') >> trigger_onedrive
    switch_file_format_task >> Label('GCS') >> trigger_gcs
    switch_file_format_task >> Label('AWS S3') >> trigger_s3


process_type01_file()
