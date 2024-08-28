from pathlib import Path
from datetime import timedelta
from typing import Any

import yaml
from yaml import CSafeLoader
import pendulum as pm
from airflow.decorators import dag, task, task_group
from airflow.configuration import AirflowConfigParser
from airflow.models import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowFailException

from plugins.metadata.schemas import StreamConfData

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
    def get_stream_config(**context):
        conf: Path = Path(__file__).parent.parent / 'conf/common/streams.yaml'
        with conf.open(mode='r', encoding='utf-8') as f:
            data: dict = yaml.load(f, CSafeLoader)[context['dag_run'].dag_id]
        try:
            StreamConfData.model_validate(obj=data)
        except Exception as err:
            raise AirflowFailException(
                f"Stream config data does not valid: {err}"
            )
        return data

    @task
    def get_asat_date(data: dict[str, Any], **context) -> str:
        """Generate asat_date value that calculate from logic from stream
        setting"""
        conf: AirflowConfigParser = context['conf']
        core_tz: str = conf.get("core", "default_timezone")
        stream = StreamConfData.model_validate(obj=data)
        return str(stream.get_asat_dt(pm.now(tz=core_tz)))

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

    api_01 = TriggerDagRunOperator(
        task_id='P_AD_API_01_D',
        trigger_dag_id='10_PROCESS_COMMON',
        trigger_run_id="{{ run_id }}_P_AD_API_01_D",
        wait_for_completion=True,
        deferrable=False,
        reset_dag_run=True,
        conf={
            "process_name": "P_AD_API_01_D",
            "asat_dt": "{{ task_instance.xcom_pull('get_asat_date') }}",
            "mode": "{{ params['mode'] }}",
        },
    )

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
    get_asat_date_task >> group_files() >> api_01 >> gbq_table_01

    write_log_task = EmptyOperator(task_id="write_stream_log")

    gbq_table_01 >> write_log_task


s_ad_d()
