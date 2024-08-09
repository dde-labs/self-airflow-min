from datetime import timedelta

import pendulum as pm
from airflow.decorators import dag, task
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.edgemodifier import Label


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

PROCESS: dict[str, int] = {
    "DUMMY": 1,
    'FILE': 1,
    'DB': 2,
    'NULL': 99,
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
            description=(
                f"Enter your process name, like {list(PROCESS.keys())}."
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
)
def process_common():
    process_mapping = {
        1: '20_PROCESS_FILE',
        2: '20_PROCESS_DB',
        99: 'PROCESS_DEFAULT',
    }

    start = EmptyOperator(task_id='start')

    @task.branch
    def choose_branch(**context):
        trigger_dag_id = process_mapping.get(
            PROCESS.get(context["params"]["process_name"], 99)
        )
        return [
            f'trigger-process-{trigger_dag_id.split("_")[-1].lower()}'
        ]

    random_choice_instance = choose_branch()

    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success"
    )
    end = EmptyOperator(task_id='end')

    start >> random_choice_instance

    for dag_id in process_mapping.values():
        t_label: str = dag_id.split("_")[-1].lower()
        if dag_id != 'PROCESS_DEFAULT':
            t = TriggerDagRunOperator(
                task_id=f'trigger-process-{t_label}',
                trigger_dag_id=dag_id,
                wait_for_completion=True,
                deferrable=False,
                conf={
                    "source": "source-name",
                    "asat_dt": "{{ params['asat_dt'] }}",
                },
            )
        else:
            t = EmptyOperator(task_id=f'trigger-process-{t_label}')

        random_choice_instance >> Label(t_label) >> t >> join

    join >> end


process_common()
