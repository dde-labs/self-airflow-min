from datetime import timedelta
from pathlib import Path

import pendulum as pm
from airflow.decorators import dag, task, task_group
from airflow.models import Param
from airflow.operators.python import get_current_context
from airflow.utils.dag_parsing_context import get_parsing_context
from airflow.utils.helpers import chain

from dags.utils.common import read_stream


current_dag_id = get_parsing_context().dag_id
current_dir: Path = Path(__file__).parent
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

for dag_id, config in (
    read_stream(file=current_dir / f'../conf/{name}.yaml')
    for name in ('s_ad_d', 's_fm_d', )
):
    if not config:
        continue
    elif current_dag_id is not None and current_dag_id != dag_id:
        # NOTE: skip generation of non-selected DAG
        continue

    @dag(
        dag_id=dag_id,
        start_date=pm.datetime(2024, 7, 31),
        schedule=None,
        catchup=False,
        params={"mode": Param("N", type="string")},
        default_args=default_args,
    )
    def stream_common():
        f"""# Stream Common for {dag_id}"""

        # NOTE: Process Group should running with sequential.
        process_task_groups: list = []
        for process_group in sorted(
            config.get("process_groups", []),
            key=lambda x: x.get('priority', 99),
        ):

            @task_group(group_id=process_group["id"])
            def process_task_group():

                # NOTE: Process should running with parallel or concurrency
                #   limit.
                processes: list = process_group.get("processes", [])
                if not processes:
                    # FIXME: it will use empty operator for this case.
                    raise ValueError("Process Group does not set process")

                priority_tasks: list = []
                for priority in (
                    (y for y in processes if y.get('priority', 99) == p)
                    for p in set(
                        map(lambda x: x.get('priority', 99), processes)
                    )
                ):
                    process_tasks: list = []
                    for process in priority:

                        @task(task_id=process["id"])
                        def process_task():
                            # NOTE: Process code will implement here
                            context = get_current_context()
                            print(context["params"])
                            print(f"Start process type: {process['type']}")

                        process_tasks.append(process_task())

                    priority_tasks.append(
                        (
                            process_tasks
                            if len(process_tasks) > 1
                            else process_tasks[0]
                        )
                    )

                chain(*priority_tasks)

            process_task_groups.append(process_task_group())

        chain(*process_task_groups)

    stream_common()
