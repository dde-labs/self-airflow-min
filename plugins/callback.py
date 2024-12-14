from __future__ import annotations

import json
import logging
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow.utils.context import Context
from airflow.utils.db import provide_session

from plugins.metadata.schemas import Process, Stream
from plugins.metadata.services import (
    check_log_stream_remark,
    get_process_log_from_stream,
    update_log_process,
    update_log_stream,
)
from plugins.utils import get_dt_now

logger = logging.getLogger("airflow.task")

# NOTE: Auto create callback folder for keeping debugging file log.
internal: Path = Path(__file__).parent.parent / "logs/internal/callbacks"
internal.mkdir(parents=True, exist_ok=True)


def get_formatted_error(exception: Any):
    return "".join(
        traceback.format_exception(
            type(exception),
            value=exception,
            tb=exception.__traceback__,
        )
    ).strip()


def process_failure_callback(context: Context):
    """Send custom email alerts."""
    from airflow.models.taskinstance import TaskInstance

    internal_filename = internal / (
        f"process_failure_callback_"
        f"{get_dt_now(only_date=False):%Y%m%d%H%M%S}_{uuid.uuid4()}.json"
    )
    dag_run_id: str = context["run_id"]
    ti: TaskInstance = context["task_instance"]

    # NOTE: Skip writing log if error raise from TriggerDagRunOperator.
    if ti.operator == "TriggerDagRunOperator":
        return

    try:
        if process_obj := context["params"].get("process"):
            process: Process = Process.model_validate(process_obj)
        else:
            process: Process = Process.model_validate(
                ti.xcom_pull(
                    task_ids="start_process",
                    key="process_obj",
                )
            )

        ld_id: int = ti.xcom_pull(
            dag_id="10_PROCESS_COMMON",
            task_ids="start_process",
            key="process_id",
        )
        asat_dt: datetime = ti.xcom_pull(
            dag_id="10_PROCESS_COMMON",
            task_ids="start_process",
            key="asat_dt",
        )

        logger.info(
            f"Start process failure callback function with ld_id: {ld_id}, "
            f"asat_dt: {asat_dt:%Y-%m-%d %H:%M:%S}"
        )

        # NOTE: Get exception detail.
        exception = context.get("exception")
        formatted_exception: str = get_formatted_error(exception)
        logger.info(f"Getting error: {formatted_exception}")

        update_values: dict[str, str] = {
            "st": "FAILED",
            "rmrk": f"{type(exception).__name__}: {exception}",
            "fail_airflow_run_id": dag_run_id,
        }

        update_log_process(
            ld_id=ld_id,
            asat_dt=asat_dt,
            process=process,
            update_values=update_values,
        )

        logger.info(
            f"Update logging for failed status successfully with data: "
            f"{str(update_values)}"
        )
    except Exception as err:
        formatted_exception: str = "".join(
            traceback.format_exception(
                type(err),
                value=err,
                tb=err.__traceback__,
            )
        ).strip()
        with internal_filename.open(mode="w") as f:
            json.dump(
                {
                    "error": str(err),
                    "error_trace": formatted_exception,
                    "error_type": str(type(err)),
                    "context": dict(context),
                },
                f,
                indent=4,
                default=str,
            )


def stream_failure_callback(context: Context):
    """Stream failure callback that use on any stream dags for catching failure
    task. This function will update the stream logging table.
    """
    from airflow.models.taskinstance import TaskInstance

    internal_filename = internal / (
        f"stream_failure_callback_"
        f"{get_dt_now(only_date=False):%Y%m%d%H%M%S}_{uuid.uuid4()}.json"
    )
    ti: TaskInstance = context["ti"]

    try:
        stream: Stream = Stream.model_validate(
            ti.xcom_pull(task_ids="get_config")
        )
        stream_id: int = ti.xcom_pull(task_ids="start_stream", key="stream_id")
        asat_dt: datetime = ti.xcom_pull(task_ids="start_stream", key="asat_dt")

        logger.info(
            f"Start stream failure callback function with strem_id: "
            f"{stream_id}, asat_dt: {asat_dt:%Y-%m-%d %H:%M:%S}"
        )

        # NOTE: Get exception detail.
        exception = context.get("exception")
        formatted_exception: str = get_formatted_error(exception)
        logger.info(f"Getting error: {formatted_exception}")

        prefix: str = ""
        if ti.operator == "TriggerDagRunOperator":
            prefix = f" with {ti.task_id}"

        update_values: dict[str, str] = {
            "st": "FAILED",
            "rmrk": f"{type(exception).__name__}: {exception}{prefix}",
        }

        rmrk: str | None = check_log_stream_remark(
            strem_id=stream_id,
            asat_dt=asat_dt,
            stream=stream,
        )
        if rmrk:
            logger.info(f"Stream already logging on database with {rmrk}")
            # update_values["rmrk"] = f"{rmrk}||{update_values['rmrk']}"

        # NOTE: Update the stream logging table to FAILED status.
        update_log_stream(
            strem_id=stream_id,
            asat_dt=asat_dt,
            stream=stream,
            update_values=update_values,
        )
        logger.info(
            f"Update logging for failed status successfully with data: "
            f"{str(update_values)}"
        )
        # with internal_filename.open(mode="w") as f:
        #     json.dump(
        #         {"context": dict(context)},
        #         f,
        #         indent=4,
        #         default=str,
        #     )
    except Exception as err:
        formatted_exception: str = "".join(
            traceback.format_exception(
                type(err),
                value=err,
                tb=err.__traceback__,
            )
        ).strip()
        with internal_filename.open(mode="w") as f:
            json.dump(
                {
                    "error": str(err),
                    "error_trace": formatted_exception,
                    "error_type": str(type(err)),
                    "context": dict(context),
                },
                f,
                indent=4,
                default=str,
            )


def main_stream_failure_callback(context: Context):
    from airflow.models.taskinstance import TaskInstance

    internal_filename = internal / (
        f"stream_failure_callback_"
        f"{get_dt_now(only_date=False):%Y%m%d%H%M%S}_sendmail_"
        f"{uuid.uuid4()}.json"
    )
    reason = context["reason"]
    if reason == "task_failure":
        ti: TaskInstance = context["task_instance"]
        # stream: Stream = Stream.model_validate(ti.xcom_pull(task_ids="get_config"))
        stream_id: int = ti.xcom_pull(task_ids="start_stream", key="stream_id")
        asat_dt: datetime = ti.xcom_pull(task_ids="start_stream", key="asat_dt")
        prcs_logs = get_process_log_from_stream(
            strem_id=stream_id, asat_dt=asat_dt
        )
        with internal_filename.open(mode="w") as f:
            json.dump(
                {
                    "context": dict(context),
                    "prcs_logs": [p.model_dump() for p in prcs_logs],
                },
                f,
                indent=4,
                default=str,
            )
    else:
        with internal_filename.open(mode="w") as f:
            json.dump(
                {"context": dict(context)},
                f,
                indent=4,
                default=str,
            )


@provide_session
def cleanup_xcom_callback(context: Context, session=None):
    from airflow.models import XCom

    dag_id = context["ti"]["dag_id"]
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()
