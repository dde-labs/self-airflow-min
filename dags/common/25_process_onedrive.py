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
    dag_id="25_PROCESS_ONEDRIVE",
    start_date=pm.datetime(2024, 8, 8),
    schedule=None,
    catchup=False,
    description="Common DAG for File on Onedrive",
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
def process_onedrive():

    # TODO: Start checking JDBC connection for OneDrive.
    check_conn = EmptyOperator(task_id='check-connection')

    @task.branch
    def switch_file_format(**context):
        file_format: str = (
            context["params"]["process"].get("fi", {}).get("file_format")
            or 'empty'
        )
        if file_format not in ('xlsx', 'csv', 'json', ):
            # NOTE: raise failed without retry
            raise AirflowFailException(
                f'File format from process object: {file_format} does not '
                f'support or empty'
            )
        return [
            f'trigger_file_{file_format.lower()}'
        ]

    @task
    def trigger_file_xlsx():
        context = get_current_context()
        logging.info(context["params"])
        logging.info(context["ds"])
        return {}

    @task
    def trigger_file_json():
        context = get_current_context()
        logging.info(context["params"])
        logging.info(context["ds"])
        return {}

    @task
    def trigger_file_csv():
        context = get_current_context()
        logging.info(context["params"])
        logging.info(context["ds"])
        return {}

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

    switch_file_format_task = switch_file_format()
    trigger_file_json_task = trigger_file_json()
    trigger_file_csv_task = trigger_file_csv()
    trigger_file_xlsx_task = trigger_file_xlsx()

    check_conn >> switch_file_format_task

    switch_file_format_task >> Label('JSON') >> trigger_file_json_task
    switch_file_format_task >> Label('CSV') >> trigger_file_csv_task
    switch_file_format_task >> Label('XLSX') >> trigger_file_xlsx_task

    [
        trigger_file_json_task,
        trigger_file_csv_task,
        trigger_file_xlsx_task,
    ] >> staging_to_curated


process_onedrive()
