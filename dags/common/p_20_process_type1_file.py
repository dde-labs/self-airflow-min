from __future__ import annotations

import logging
from datetime import timedelta

import pendulum as pm
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.helpers import chain

from dags.common.p_25_process_gcs import process_gcs
from dags.common.p_25_process_onedrive import process_onedrive
from dags.common.p_25_process_s3 import process_s3
from plugins.callback import process_failure_callback

logger = logging.getLogger("airflow.task")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": process_failure_callback,
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
                },
            },
            type=["object", "null"],
            section="Important Params",
            description=(
                "Enter your process data that want to start process file."
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
    render_template_as_native_obj=True,
)
def process_type01_file():

    @task(task_id="start")
    def start_process():
        # from airflow.exceptions import AirflowFailException
        #
        # raise AirflowFailException(
        #     "Raise error manually for testing callback from process"
        # )
        return

    @task.branch
    def switch_file_connection(**context):
        """Switch file type connection with service name of source connection.
        Currently, this branch support only onedrive, gcs, and s3.
        """
        file_conn_type: str = (
            context["params"].get("process", {}).get("conn", {}).get("service")
        ) or "empty"
        if file_conn_type not in (
            "onedrive",
            "gcs",
            "s3",
        ):
            # NOTE: raise failed without retry
            raise AirflowFailException(
                f"File connection type from process object: {file_conn_type} "
                f"does not support or empty"
            )
        return [
            f"{file_conn_type.lower()}"
            f".trigger-process-{file_conn_type.lower()}"
        ]

    @task(task_id="return_result", trigger_rule="none_failed_min_one_success")
    def prepare_result_for_parent_dag():
        return {
            "cnt_rec_cntl": 10000,
            "cnt_rec_src": 10000,
            "cnt_rec_tgt": 10000,
            "cnt_fi_src": 1,
            "cnt_fi_tgt": 1,
        }

    switch_file_format_task = switch_file_connection()
    start_process() >> switch_file_format_task

    process_onedrive_task = process_onedrive()
    process_gcs_task = process_gcs()
    process_s3_task = process_s3()

    switch_file_format_task >> Label("ONE DRIVE") >> process_onedrive_task
    switch_file_format_task >> Label("GCS") >> process_gcs_task
    switch_file_format_task >> Label("AWS S3") >> process_s3_task

    end_onedrive = EmptyOperator(task_id="end-onedrive")
    end_gcs = EmptyOperator(task_id="end-gcs")
    end_s3 = EmptyOperator(task_id="end-s3")

    process_onedrive_task >> end_onedrive
    process_gcs_task >> end_gcs
    process_s3_task >> end_s3

    chain(
        [
            end_onedrive,
            end_gcs,
            end_s3,
        ],
        prepare_result_for_parent_dag(),
    )


process_type01_file()
