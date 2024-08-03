import logging

import pendulum as pm
from airflow.decorators import dag, task
from airflow.models import Param
from airflow.models.taskinstance import TaskInstance
from airflow.models.dagrun import DagRun
from airflow.operators.python import get_current_context
import pytest

try:
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
except ImportError:
    pytest.skip("MSSQL provider not available", allow_module_level=True)


@dag(
    # NOTE: Basic params
    dag_id='get_conn_mssql',
    start_date=pm.datetime(2024, 7, 31),
    schedule="@daily",
    catchup=False,
    # NOTE: UI params
    description="Get connection from mssql server that was provision on local",
    tags=["example"],
    # NOTE: Jinja params
    # NOTE: Other params
    default_args={
        "retries": 2,
    },
    params={
        "asat_dt": Param("{{ ds }}", type="string"),
    },
    dag_display_name="get_conn_mssql",
)
def get_conn_mssql():
    """# Get Connection Mssql

    This dag use for demo functional use case for getting connection to MSSQL
    and it will use to test passing parameters to dag.
    """

    @task(task_id='echo_connection_task', multiple_outputs=True)
    def echo_connection_hook():
        mssql_hook = MsSqlHook(mssql_conn_id="mssql_default", schema="DWHCNTL")
        logging.info(mssql_hook.schema)
        mssql_hook.test_connection()
        return {
            "conn_id": "mssql_default",
            "schema": "DWHCNTL",
        }

    @task
    def variable_task(
        conn_id: str,
        schema: str,
        task_instance: TaskInstance | None = None,
        dag_run: DagRun | None = None,
    ):
        mssql_hook = MsSqlHook(mssql_conn_id=conn_id, schema=schema)
        mssql_hook.test_connection()

        logging.info(f"start_date: {task_instance.start_date}")
        logging.info(f"end_date: {task_instance.end_date}")
        logging.info(f"Run ID: {task_instance.run_id}")
        logging.info(f"Duration: {task_instance.duration}")
        logging.info(f"DAG Run queued at: {dag_run.queued_at}")

        context = get_current_context()
        logging.info(context["params"])
        logging.info(context["ds"])

    echo_conn_task = echo_connection_hook()
    variable_task(
        echo_conn_task["conn_id"],
        echo_conn_task["schema"],
    )


get_conn_mssql()
