"""
This file will be use to generate observation DAG for stream deps.

Ref: https://www.astronomer.io/docs/learn/cross-dag-dependencies
"""
from pathlib import Path

import pendulum as pm
from airflow.decorators import dag
from airflow.models import DagRun, TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.helpers import chain

from plugins.utils.common import read_batch, read_deployment


current_dir: Path = Path(__file__).parent
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}


batches: dict[str, type[DagRun]] = {}
for dag_id, config in (
    read_batch(file=current_dir / f'../conf/{name}.yaml')
    for name in (
        read_deployment(current_dir / f'../conf/deployment.yaml').batches
    )
):
    if dag_id is None or config.batch_id == 'EMPTY':
        continue

    @dag(
        dag_id=dag_id,
        start_date=pm.datetime(2024, 7, 31),
        schedule=None,
        catchup=False,
        description=f"Generated batch DAG: {dag_id}",
        tags=["batch", "auto-gen"],
        default_args=default_args,
    )
    def batch_common():
        start = EmptyOperator(task_id="start")

        batches_task: dict[str, TaskInstance] = {}
        batch_streams: list[TaskInstance] = []
        for stream in config.streams:
            trigger_stream = TriggerDagRunOperator(
                task_id=stream.alias,
                trigger_dag_id=stream.id,
                wait_for_completion=True,
                deferrable=False,
            )
            batch_streams.append(trigger_stream)
            batches_task[stream.alias] = trigger_stream

        end = EmptyOperator(task_id="end")

        chain(
            start,
            (batch_streams if len(batch_streams) > 1 else batch_streams),
            end,
        )


    batches[dag_id] = batch_common()
