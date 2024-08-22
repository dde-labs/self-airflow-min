"""
The file will use for generate the DAG from a config template
"""
from datetime import timedelta
from pathlib import Path

import pendulum as pm
from airflow.decorators import dag, task_group
from airflow.models import Param, DagRun, TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.utils.helpers import chain

from dags_bk.gen.gen_manual_process import gen_process
from plugins.utils.sla import sla_callback
from plugins.utils.common import read_stream, read_deployment
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


streams: dict[str, type[DagRun]] = {}
for dag_id, config in (
    read_stream(file=current_dir / f'../conf/{name}.yaml')
    for name in (
        read_deployment(current_dir / '../conf/deployment.yaml').manual_streams
    )
):
    if dag_id is None or config.id == 'EMPTY':
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
            config.process_groups, key=lambda x: x.priority
        ):

            @task_group(
                group_id=process_group.id,
                tooltip="This task group is a process grouping!!!",
            )
            def process_task_group():

                # NOTE: Process should running with parallel or concurrency
                #   limit in the same group priority.
                priorities: list[list[Process]] = process_group.priorities()
                if not priorities:
                    EmptyOperator(task_id=f"EMPTY_{process_group.id}")
                    return

                priority_tasks: list = []
                for priority in priorities:

                    process_tasks: list[TaskInstance] = []
                    for process in priority:
                        process_task = gen_process(process=process, extra={})
                        process_tasks.append(process_task())

                    priority_tasks.append(
                        (
                            process_tasks.copy()
                            if len(process_tasks) > 1
                            else process_tasks[0]
                        )
                    )
                    del process_tasks

                chain(*priority_tasks)
                del priority_tasks

            process_task_groups.append(process_task_group())

        chain(*process_task_groups)
        del process_task_groups

    # NOTE: Keep the stream DAG to list for reuse with sub-DAG
    streams[dag_id] = stream_common()
