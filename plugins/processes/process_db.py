import logging

from airflow.operators.empty import EmptyOperator

from plugins.models import Process


def process_db(
    process: Process,
):
    logging.info("Start process type 2")
    logging.info(f"... Loading data from {process.source} to {process.target}")

    return [EmptyOperator(task_id="start")]
