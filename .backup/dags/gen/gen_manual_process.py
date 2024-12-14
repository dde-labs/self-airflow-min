"""
The file will use for generate the Process function from a config template
"""
from datetime import timedelta
from typing import Any

from airflow.decorators import task
from airflow.operators.python import get_current_context

from plugins_bk.models import Process
from plugins_bk.manual_process.process_file import process_file
from plugins_bk.manual_process.process_db import process_db


TYPE_SUPPORTED: dict[int, callable] = {
    1: process_file,
    2: process_db,
}


def gen_process(process: Process, extra: dict[str, Any] | None = None):
    """Generator process"""
    if (process_gateway := TYPE_SUPPORTED.get(process.type)) is None:
        raise ValueError(f"Process Type {process.type} does not support.")

    @task(
        sla=timedelta(minutes=process.sla),
    )
    def process_task():
        context = get_current_context()
        print(context["params"])
        print(f"Start process type: {process.type}")
        process_gateway(
            source=process.source,
            target=process.target,
            extra=context.get("params", {}) | (extra or {}),
        )

    return process_task.override(
        pool='default_pool',
        task_id=process.id,
    )
