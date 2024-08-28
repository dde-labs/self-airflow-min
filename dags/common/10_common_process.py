import logging
from pathlib import Path
from datetime import timedelta
from typing import Any

import yaml
from yaml import CSafeLoader
import pendulum as pm
from airflow.decorators import dag, task
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.edgemodifier import Label
from airflow.exceptions import AirflowFailException
# from airflow.utils.email import send_email

from plugins.metadata.schemas import ProcessConfData


logger = logging.getLogger("airflow.task")


# def custom_failure_email(context):
#     """Send custom email alerts."""
#     dag_run = context.get('task_instance').dag_id
#     subject: str = f"[ActionReq]-dag failure-{dag_run}"
#     body: str = (
#         "Hi Team,<br><br>"
#         "<b style=\"font-size:15px;color:red;\">Airflow job on error, please "
#         "find details below.</b>"
#         "Thank you!,<br>"
#     )
#     email_list = context['dag'].params['email']
#     for i in range(len(email_list)):
#         send_email(
#             str(email_list[i]), subject, body
#         )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    # "on_failure_callback": custom_failure_email,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="10_PROCESS_COMMON",
    start_date=pm.datetime(2024, 8, 8),
    schedule=None,
    catchup=False,
    # NOTE: UI params
    description="Common Process DAG",
    tags=["process", "common"],
    # NOTE: Other params
    params={
        "process_name": Param(
            type="string",
            section="Important Params",
            description="Enter your process name.",
        ),
        "asat_dt": Param(
            default=str(pm.now(tz='Asia/Bangkok') - timedelta(days=1)),
            type="string",
            format="date-time",
            section="Important Params",
            description="Enter the asat date that want to filter date.",
        ),
        "mode": Param(
            default="N",
            enum=["N", "F", "R"],
            type="string",
            section="Important Params",
            description="Enter your stream running mode.",
        ),
    },
    default_args=default_args,
    render_template_as_native_obj=True,
)
def process_common():

    @task
    def loading_process_conf(**context) -> dict[str, Any]:
        """Loading process config from `.yaml` file."""
        conf: Path = Path(__file__).parent.parent / 'conf/common/processes.yaml'
        with conf.open(mode='r', encoding='utf-8') as f:
            data: dict = (
                yaml.load(f, CSafeLoader)[context['params']['process_name']]
            )
        logging.info(data)
        try:
            ProcessConfData.model_validate(obj=data)
        except Exception as err:
            raise AirflowFailException(
                f"Process config data does not valid: {err}"
            )
        return data

    @task.branch
    def switch_process_load_type(data: dict[str, Any]):
        """Switch process for an incoming process loading type."""
        process_type: int = data.get('prcs_typ') or 99
        if process_type not in (1, 2, 3, ):
            return ['trigger-default']
        return [f'trigger-process-{process_type}']

    switch_process_load_task = switch_process_load_type(loading_process_conf())

    write_log_task = EmptyOperator(
        task_id="write_process_log",
        trigger_rule="none_failed_min_one_success",
    )

    for dag_id in (1, 2, 3, 99, ):
        if dag_id == 99:
            t_label: str = 'DEFAULT'
            t = EmptyOperator(task_id='trigger-default')
        else:
            t_label: str = f'TYPE {dag_id}'
            t = TriggerDagRunOperator(
                task_id=f'trigger-process-{dag_id}',
                trigger_dag_id=f'20_PROCESS_TYPE{dag_id}',
                trigger_run_id="{{ run_id }}",
                wait_for_completion=True,
                deferrable=False,
                reset_dag_run=True,
                conf={
                    # NOTE: pass process object to each process type.
                    "process": (
                        "{{ task_instance.xcom_pull('loading_process_conf') }}"
                    ),
                    "asat_dt": "{{ params['asat_dt'] }}",
                },
            )

        switch_process_load_task >> Label(t_label) >> t >> write_log_task


process_common()
