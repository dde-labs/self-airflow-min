import logging
from datetime import timedelta

from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context

from plugins.models import Process


def process_file(
    process: Process,
):
    @task(
        sla=timedelta(minutes=process.sla),
    )
    def process_task():
        context = get_current_context()
        print(context["params"])
        logging.info("Start process type 1 (file)")
        logging.info(
            f"... Loading data from {process.source} to {process.target}"
        )

    return [
        EmptyOperator(task_id="start"),
        process_task.override(task_id='Main_Task')(),
    ]
