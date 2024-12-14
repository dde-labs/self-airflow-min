from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

import pendulum as pm
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.models import Param
from airflow.models.taskinstance import TaskInstance

from plugins.callback import (
    main_stream_failure_callback,
    stream_failure_callback,
)
from plugins.metadata.schemas import Stream, StreamWatermark
from plugins.metadata.services import (
    create_log_stream,
    finish_log_stream,
    get_watermark,
    open_watermark,
    shift_watermark,
)
from plugins.operators.trigger import TriggerProcessOperator
from plugins.utils import get_dt_now

logger = logging.getLogger("airflow.task")
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    # NOTE: Should not set retry.
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    "on_failure_callback": stream_failure_callback,
}


@dag(
    dag_id="S_AD_D",
    start_date=pm.datetime(2024, 8, 8),
    schedule=None,
    catchup=False,
    tags=["stream"],
    params={
        "mode": Param(
            default="N",
            enum=["N", "F", "R"],
            type="string",
            section="Important Params",
            description="Enter your stream running mode.",
        ),
    },
    default_args=default_args,
    on_failure_callback=main_stream_failure_callback,
    render_template_as_native_obj=True,
)
def s_ad_d():

    # NOTE: Start Read Config data template from yaml file.
    @task(task_id="get_config")
    def get_stream_config(**context) -> dict[str, Any]:
        """Get Stream Config data from yaml file template."""
        stream = Stream.from_conf(name=context["dag_run"].dag_id)
        return stream.model_dump()

    @task(task_id="start_stream")
    def start_stream(stream: dict[str, Any], **context) -> dict[str, Any]:
        """Generate asat_date value that calculate from logic from stream
        setting"""
        stream: Stream = Stream.model_validate(obj=stream)

        # FIXME: Remove auto create stream watermark if it already to deploy
        #   next environment.
        stream_watermark: StreamWatermark = get_watermark(
            stream, auto_create=True
        )
        if (
            mode := context["params"]["mode"]
        ) == "N" and not stream_watermark.strem_f:
            raise AirflowException(
                "Stream still open, please waiting retry process to "
                "execute this task again or adjust it manually"
            )

        if mode == "N" and (
            (get_dt_now() - timedelta(days=stream.stlmt_dt))
            < stream_watermark.asat_dt
        ):
            raise AirflowFailException(
                "Cannot run in normal mode, because as at date is > "
                "current date."
            )

        if mode != "R":
            open_watermark(stream=stream)

        # NOTE: Start create logging for this stream loading ID.
        stream_log = create_log_stream(
            airflow_run_id=context["run_id"],
            stream=stream,
            watermark=stream_watermark,
        )

        # NOTE: Final results that will use on this stream DAG XCom.
        return {
            "asat_dt": stream_watermark.asat_dt,
            "nxt_asat_dt": stream_watermark.nxt_asat_dt,
            "calc_asat_dt": stream_watermark.calc_asat_dt,
            "prv_calc_asat_dt": stream_watermark.prv_calc_asat_dt,
            "stream_id": stream_log.strem_id,
        }

    get_config_task = get_stream_config()
    get_asat_date_task = start_stream(get_config_task)

    # ADJUST: Add main process tasks after start stream.
    # >>>

    @task_group(group_id="PG_AD_FILE_GROUPS")
    def group_files():
        """Group of processes for file system."""
        onedrive_xlsx_01 = TriggerProcessOperator("P_AD_ONEDRIVE_XLSX_01_D")
        onedrive_json_01 = TriggerProcessOperator("P_AD_GCS_JSON_01_D")
        onedrive_csv_01 = TriggerProcessOperator("P_AD_S3_CSV_01_D")

        [onedrive_xlsx_01, onedrive_csv_01] >> onedrive_json_01

    api_01 = TriggerProcessOperator("P_AD_API_01_D")
    gbq_table_01 = TriggerProcessOperator("P_AD_GBQ_TABLE_01_D")

    (get_asat_date_task >> group_files() >> api_01 >> gbq_table_01)
    # ADJUST: <<<

    @task(task_id="end_stream")
    def end_stream(
        task_instance: TaskInstance | None = None, **context
    ) -> dict[str, Any]:
        mode: str = context["params"]["mode"]
        stream: Stream = Stream.model_validate(
            obj=task_instance.xcom_pull(task_ids="get_config")
        )
        old_watermark: StreamWatermark = get_watermark(stream)

        # NOTE: Update stream flag to True for all mode and shift asat_dt if
        #   running mode be Normal.
        stream_watermark = shift_watermark(
            stream=stream, watermark=old_watermark, mode=mode
        )
        logger.info("Close stream successful")

        # NOTE: Start finish stream logging.
        stream_id: int = task_instance.xcom_pull(
            task_ids="start_stream", key="stream_id"
        )
        asat_dt: datetime = task_instance.xcom_pull(
            task_ids="start_stream", key="asat_dt"
        )
        finish_log_stream(
            strem_id=stream_id,
            asat_dt=asat_dt,
            stream=stream,
        )
        logger.info("Finish logging stream")
        return {
            "mode": mode,
            "asat_dt": stream_watermark.asat_dt,
            "nxt_asat_dt": stream_watermark.nxt_asat_dt,
            "calc_asat_dt": stream_watermark.calc_asat_dt,
            "prv_calc_asat_dt": stream_watermark.prv_calc_asat_dt,
            "stream_id": stream_id,
        }

    # ADJUST: Add end_stream task on the final down stream of this dag.
    # >>>
    gbq_table_01 >> end_stream()
    # ADJUST: <<<


s_ad_d()
