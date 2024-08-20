from datetime import timedelta

import pendulum as pm
from airflow.decorators import dag
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
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
    dag_id='S_AD_COMMON_D',
    start_date=pm.datetime(2024, 8, 8),
    schedule=None,
    catchup=False,
    tags=["stream"],
    params={
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
def s_ad_d():
    start = EmptyOperator(task_id='start')

    process_file = TriggerDagRunOperator(
        task_id='P_AD_PROCESS_FILE_D_99',
        trigger_dag_id='10_PROCESS_COMMON',
        trigger_run_id="{{ run_id }}_P_AD_PROCESS_FILE_D_99",
        wait_for_completion=True,
        deferrable=False,
        reset_dag_run=True,
        conf={
            "process_name": "FILE",
            "asat_dt": "{{ params['asat_dt'] }}",
        },
    )

    process_db = TriggerDagRunOperator(
        task_id='P_AD_PROCESS_DB_D_99',
        trigger_dag_id='10_PROCESS_COMMON',
        trigger_run_id="{{ run_id }}_P_AD_PROCESS_FILE_D_99",
        wait_for_completion=True,
        deferrable=False,
        reset_dag_run=True,
        conf={
            "process_name": "DB",
            "asat_dt": "{{ params['asat_dt'] }}",
        },
    )

    end = EmptyOperator(task_id='end')

    start >> [
        process_file,
        process_db,
    ] >> end


s_ad_d()
