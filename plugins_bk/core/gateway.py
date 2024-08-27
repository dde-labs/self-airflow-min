from typing import Union, Any

from airflow.models import TaskInstance

from plugins_bk.core.process_file import process_file
from plugins_bk.core.process_db import process_db
from plugins_bk.models import Process


TYPE_SUPPORTED: dict[int, Any] = {
    1: process_file,
    2: process_db,
}


def process_gateway(
    process: Process,
) -> list[Union[TaskInstance, list[TaskInstance]]]:
    if (gateway := TYPE_SUPPORTED.get(process.type)) is None:
        raise ValueError(
            f"Process type {process.type} does not support yet!!!"
        )
    return gateway(process)

