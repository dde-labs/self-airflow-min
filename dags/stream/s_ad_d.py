import logging
from datetime import timedelta
from typing import Any

import pendulum as pm
from airflow.decorators import dag, task, task_group
from airflow.models import Param
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
    dag_id='S_AD_D',
    start_date=pm.datetime(2024, 8, 8),
    schedule=None,
    catchup=False,
    tags=["stream"],
    params={
        "mode": Param(
            default="N",
            enum=["N", "F", "R"],
            type="string",
            section="Important Params",
            description="Enter your stream running mode.",
        ),
    },
    default_args=default_args,
)
def s_ad_d():

    @task
    def get_stream_config():
        return {
            "tier": "CURATED",
            "stlmt_dt": -1,
            "dpnd_stlmt_dt": -1,
            "feq": "D",
            "data_feq": "D",
        }

    @task
    def get_asat_date(config: dict[str, Any]):
        """Generate asat_date value that calculate from logic from stream
        setting"""
        logging.info(config)
        return str(pm.now(tz='Asia/Bangkok') - timedelta(days=1))

    @task_group(group_id='PG_AD_FILE_GROUPS')
    def group_files():
        onedrive_xlsx_01 = TriggerDagRunOperator(
            task_id='P_AD_ONEDRIVE_XLSX_01_D',
            trigger_dag_id='10_PROCESS_COMMON',
            trigger_run_id="{{ run_id }}_P_AD_ONEDRIVE_XLSX_01_D",
            wait_for_completion=True,
            deferrable=False,
            reset_dag_run=True,
            conf={
                "process_name": "P_AD_ONEDRIVE_XLSX_01_D",
                "asat_dt": "{{ task_instance.xcom_pull('get_asat_date') }}",
                "mode": "{{ params['mode'] }}",
            },
        )

        onedrive_json_01 = TriggerDagRunOperator(
            task_id='P_AD_GCS_JSON_01_D',
            trigger_dag_id='10_PROCESS_COMMON',
            trigger_run_id="{{ run_id }}_P_AD_GCS_JSON_01_D",
            wait_for_completion=True,
            deferrable=False,
            reset_dag_run=True,
            conf={
                "process_name": "P_AD_GCS_JSON_01_D",
                "asat_dt": "{{ task_instance.xcom_pull('get_asat_date') }}",
                "mode": "{{ params['mode'] }}",
            },
        )

        onedrive_csv_01 = TriggerDagRunOperator(
            task_id='P_AD_S3_CSV_01_D',
            trigger_dag_id='10_PROCESS_COMMON',
            trigger_run_id="{{ run_id }}_P_AD_S3_CSV_01_D",
            wait_for_completion=True,
            deferrable=False,
            reset_dag_run=True,
            conf={
                "process_name": "P_AD_S3_CSV_01_D",
                "asat_dt": "{{ task_instance.xcom_pull('get_asat_date') }}",
                "mode": "{{ params['mode'] }}",
            },
        )

        [onedrive_xlsx_01, onedrive_csv_01] >> onedrive_json_01

    gbq_table_01 = TriggerDagRunOperator(
        task_id='P_AD_GBQ_TABLE_01_D',
        trigger_dag_id='10_PROCESS_COMMON',
        trigger_run_id="{{ run_id }}_P_AD_GBQ_TABLE_01_D",
        wait_for_completion=True,
        deferrable=False,
        reset_dag_run=True,
        conf={
            "process_name": "P_AD_GBQ_TABLE_01_D",
            "asat_dt": "{{ task_instance.xcom_pull('get_asat_date') }}",
            "mode": "{{ params['mode'] }}",
        },
    )

    get_asat_date_task = get_asat_date(get_stream_config())
    get_asat_date_task >> group_files() >> gbq_table_01


s_ad_d()
