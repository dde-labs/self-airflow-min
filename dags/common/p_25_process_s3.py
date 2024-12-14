from __future__ import annotations

import logging

from airflow.decorators import task, task_group
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.edgemodifier import Label


@task_group(
    group_id="s3",
    tooltip="This task group is use for AWS S3",
    prefix_group_id=True,
)
def process_s3():
    check_conn = EmptyOperator(task_id="trigger-process-s3")

    @task.branch
    def switch_file_format(**context):
        file_format: str = (
            context["params"]["process"].get("fi", {}).get("file_format")
            or "empty"
        )
        if file_format not in (
            "xlsx",
            "csv",
            "json",
        ):
            # NOTE: raise failed without retry
            raise AirflowFailException(
                f"File format from process object: {file_format} does not "
                f"support or empty"
            )
        return [f"s3.trigger_file_{file_format.lower()}"]

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
        task_id="staging-to-curated",
        trigger_dag_id="30_STG_TO_CURATED",
        trigger_run_id="{{ run_id }}",
        wait_for_completion=True,
        deferrable=False,
        reset_dag_run=True,
        trigger_rule="none_failed_min_one_success",
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

    switch_file_format_task >> Label("JSON") >> trigger_file_json_task
    switch_file_format_task >> Label("CSV") >> trigger_file_csv_task
    switch_file_format_task >> Label("XLSX") >> trigger_file_xlsx_task

    [
        trigger_file_json_task,
        trigger_file_csv_task,
        trigger_file_xlsx_task,
    ] >> staging_to_curated
