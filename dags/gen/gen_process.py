"""
The file will use for generate the Process function from a config template
"""
import logging
from typing import Any

from airflow.decorators import task
from airflow.operators.python import get_current_context

from plugins.models import Process, Source, Target


def process_type_1(
    source: Source,
    target: Target,
    extra: dict[str, Any],
):
    logging.info("Start process type 1")
    logging.info(f"... Loading data from {source} to {target}")
    logging.info(f"... Extra params: {extra}")


def process_type_2(
    source: Source,
    target: Target,
    extra: dict[str, Any],
):
    logging.info("Start process type 2")
    logging.info(f"... Loading data from {source} to {target}")
    logging.info(f"... Extra params: {extra}")


TYPE_SUPPORTED: dict[int, callable] = {
    1: process_type_1,
    2: process_type_2,
}


def gen_process(process: Process, extra: dict[str, Any] | None = None):
    """Generator process"""
    if (process_gateway := TYPE_SUPPORTED.get(process.type)) is None:
        raise ValueError(f"Process Type {process.type} does not support.")

    @task()
    def process_task():
        context = get_current_context()
        print(context["params"])
        print(f"Start process type: {process.type}")
        process_gateway(
            source=process.source,
            target=process.target,
            extra=context.get("params", {}) | (extra or {}),
        )

    return process_task
