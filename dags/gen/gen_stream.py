"""
The file will use for generate the DAG from a config template
"""
from datetime import timedelta
from pathlib import Path

import pendulum as pm
from airflow.decorators import dag, task_group
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.helpers import chain

from dags.gen.gen_process import gen_process
from plugins.utils.sla import sla_callback
from plugins.utils.common import read_stream
from plugins.models import Process


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
    if dag_id is None:
        continue

    dag_doc: str = f"""
    ## Stream Common: `{dag_id}`
    
    This dag will generate from generator function. If you want to add or delete
    a dag from this generator process, you can navigate to `dags/conf` dir and
    delete or add a `.yaml` file.
    
    ### Getting Started
    
    Parameters:
    - mode: A stream running mode that should be only one in [`N`, `R`, `F`]
    """

    @dag(
        # NOTE: Basic params
        dag_id=dag_id,
        start_date=pm.datetime(2024, 7, 31),
        schedule=None,
        catchup=False,
        # NOTE: UI params
        description=f"Generated stream DAG: {dag_id}",
        tags=["stream", "auto-gen"],
        # NOTE: Other params
        params={
            "mode": Param(
                default="N",
                enum=["N", "F", "R"],
                type="string",
                section="Important Params",
                description="Enter your stream running mode.",
            ),
        },
        sla_miss_callback=sla_callback,
        default_args=default_args,
        doc_md=dag_doc,
    )
    def stream_common():
        # NOTE: Process Group should running with sequential.
        process_task_groups: list = []
        for process_group in sorted(
            config.process_groups,
            key=lambda x: x.priority,
        ):
            @task_group(group_id=process_group.id)
            def process_task_group():

                # NOTE: Process should running with parallel or concurrency
                #   limit in the same group priority.
                processes: list[Process] = process_group.processes.copy()
                if not processes:
                    EmptyOperator(
                        task_id=f"EMPTY_{process_group.id}"
                    )
                    return

                priority_tasks: list = []
                for priority in (
                    (y for y in processes if y.priority == p)
                    for p in set(map(lambda x: x.priority, processes))
                ):
                    process_tasks: list = []
                    for process in priority:

                        process_task = (
                            gen_process(process=process, extra={})
                            .override(
                                pool='default_pool',
                                task_id=process.id,
                            )
                        )

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
