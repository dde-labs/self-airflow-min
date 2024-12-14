from __future__ import annotations

import pendulum as pm
from airflow.decorators import dag, task

from plugins.metadata.services import initial_database

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}


@dag(
    dag_id="initial_db",
    start_date=pm.datetime(2024, 8, 8),
    schedule=None,
    catchup=False,
    tags=["operation"],
    default_args=default_args,
)
def init_db():

    @task(task_id="initial_db")
    def initial_database_task():
        initial_database()

    initial_database_task()


init_db()
