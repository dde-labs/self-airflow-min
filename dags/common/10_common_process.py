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
# from airflow.utils.email import send_email


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
    description="Common DAG for process",
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
    def get_run_id(**context):
        logger.info(str(context))
        dag_run = context['dag_run']
        logger.info(f"{type(dag_run)}: {dag_run}")
        run_id: str = dag_run.run_id
        logger.info(run_id)
        return run_id

    @task
    def loading_process_conf(**context) -> dict[str, Any]:
        """Loading process config from `.yaml` file."""
        conf: Path = Path(__file__).parent.parent / 'conf/common/processes.yaml'
        with conf.open(mode='r', encoding='utf-8') as f:
            data: dict = (
                yaml.load(f, CSafeLoader)[context['params']['process_name']]
            )
        logging.info(data)
        return data

    @task.branch
    def switch_process_load_type(config: dict):
        process_type: int = config.get('prcs_typ') or 99
        if process_type not in (1, 2, 3, ):
            return ['trigger-default']
        return [f'trigger-process-{process_type}']

    get_run_id_task = get_run_id()
    loading_conf_task = loading_process_conf()

    get_run_id_task >> loading_conf_task

    switch_process_load_task = switch_process_load_type(loading_conf_task)

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success"
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

        switch_process_load_task >> Label(t_label) >> t >> end


process_common()
