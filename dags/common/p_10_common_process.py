from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

import pendulum as pm
from airflow.configuration import conf
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator, DagRun, Param, XCom
from airflow.models.taskinstance import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.helpers import chain
from zoneinfo import ZoneInfo

from plugins.callback import process_failure_callback
from plugins.metadata.schemas import Process, ProcessLog, Stream
from plugins.metadata.services import (
    check_process_deps,
    create_log_process,
    finish_log_process,
)

logger = logging.getLogger("airflow.task")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    # NOTE: Should not set retry.
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    "on_failure_callback": process_failure_callback,
}


@dag(
    dag_id="10_PROCESS_COMMON",
    start_date=pm.datetime(2024, 8, 8),
    schedule=None,
    catchup=False,
    # NOTE: UI params
    description="Common Process DAG",
    tags=["process", "common"],
    # NOTE: Other params
    params={
        "process_name": Param(
            type="string",
            section="Important Params",
            description="Enter your process name.",
        ),
        "asat_dt": Param(
            default=str(pm.now(tz="Asia/Bangkok") - timedelta(days=1)),
            type="string",
            format="date-time",
            section="Important Params",
            description="Enter the asat date that want to filter date.",
        ),
        "mode": Param(
            default="N",
            enum=["N", "F", "R"],
            type="string",
            section="Important Params",
            description="Enter your stream running mode.",
        ),
        "stream_id": Param(
            default=-999,
            type="integer",
            section="Important Params",
            description="Stream running ID that create after starting process.",
        ),
    },
    default_args=default_args,
    render_template_as_native_obj=True,
)
def process_common():

    @task(task_id="get_config")
    def get_process_conf(**context) -> dict[str, Any]:
        """Loading process config from `.yaml` file."""
        process = Process.from_conf(name=context["params"]["process_name"])
        return process.model_dump()

    @task(task_id="check_dependency")
    def check_deps(process: dict[str, Any], **context) -> None:
        """Checking process dependency allow current asat_dt able to run without
        leaking

        :raise AirflowFailException: If it has dependency process does not run
            or it run with fail status.
        """
        core_tz: str = conf.get("core", "default_timezone")
        dag_run: DagRun = context["dag_run"]
        logger.info(
            f"Getting stream data from xcom with dag_id: "
            f"{dag_run.run_id.split('___')[-2]}, "
            f"run_id: {dag_run.run_id.split('___')[0]}"
        )
        stream: Stream = Stream.model_validate(
            XCom.get_one(
                run_id=dag_run.run_id.split("___")[0],
                dag_id=dag_run.run_id.split("___")[-2],
                task_id="get_config",
            )
        )
        process: Process = Process.model_validate(obj=process)

        asat_dt: datetime = datetime.fromisoformat(
            context["params"]["asat_dt"]
        ).replace(tzinfo=ZoneInfo(core_tz))
        logger.info(
            f"Check process dependency with asat_dt: "
            f"{asat_dt:%Y-%m-%d %H:%M:%S} "
            f"(params: {context['params']['asat_dt']})"
        )
        deps: list[ProcessLog] = check_process_deps(
            stream=stream,
            process=process,
            asat_dt=asat_dt,
        )
        if len(deps) > 0:
            err: str = ",".join(
                f"{d.prcs_nm} ({d.asat_dt:%Y-%m-%d}, {d.st}, {d.rmrk})"
                for d in deps
            )
            raise AirflowFailException(
                f"Process dependency does not allow this process able to run: "
                f"{err}"
            )
        logger.info("Process able to run with healthy dependency process.")

    @task(task_id="start_process", multiple_outputs=True)
    def start_process(process: dict[str, Any], **context) -> dict[str, Any]:
        core_tz: str = conf.get("core", "default_timezone")
        process: Process = Process.model_validate(obj=process)
        asat_dt: datetime = datetime.fromisoformat(
            context["params"]["asat_dt"]
        ).replace(tzinfo=ZoneInfo(core_tz))

        logger.info(
            f"Start process with asat_dt: {asat_dt:%Y-%m-%d %H:%M:%S} "
            f"(params: {context['params']['asat_dt']})"
        )
        process_log: ProcessLog = create_log_process(
            airflow_run_id=context["run_id"],
            asat_dt=asat_dt,
            strem_id=context["params"]["stream_id"],
            process=process,
        )
        return {
            "process_obj": process.model_dump(),
            "process_id": process_log.ld_id,
            "asat_dt": asat_dt,
        }

    @task.branch(trigger_rule="all_success")
    def switch_load_type(
        task_instance: TaskInstance | None = None,
    ) -> list[str]:
        """Switch process for an incoming process loading type."""
        process_type: int = (
            task_instance.xcom_pull(task_ids="get_config", key="prcs_typ") or 99
        )
        if process_type not in (
            1,
            2,
            3,
        ):
            return ["trigger-default"]
        return [f"trigger-process-{process_type}"]

    switch_process_load_task = switch_load_type()

    chain(
        start_process(get_process_task := get_process_conf()),
        check_deps(get_process_task),
        switch_process_load_task,
    )

    @task(task_id="end_process", trigger_rule="none_failed_min_one_success")
    def end_process(
        task_instance: TaskInstance | None = None, **context
    ) -> dict[str, Any]:
        ld_id: int = task_instance.xcom_pull(
            task_ids="start_process", key="process_id"
        )
        process: Process = Process.model_validate(
            obj=task_instance.xcom_pull(
                task_ids="start_process", key="process_obj"
            )
        )
        asat_dt: datetime = task_instance.xcom_pull(
            task_ids="start_process", key="asat_dt"
        )

        # NOTE: getting process results from the dynamic trigger dags.
        trigger_dags = task_instance.xcom_pull(task_ids="switch_load_type")
        if len(trigger_dags) > 1:
            raise ValueError

        suffix_dag_id = trigger_dags[0].split("-")[-1]
        logger.info(
            f"Trigger dags: {trigger_dags} with dag ID: "
            f"20_PROCESS_TYPE{suffix_dag_id}"
        )

        return_result: dict[str, Any] = XCom.get_one(
            run_id=context["run_id"],
            dag_id=f"20_PROCESS_TYPE{suffix_dag_id}",
            task_id="return_result",
        )
        logger.info(str(return_result))

        finish_log_process(
            ld_id=ld_id,
            asat_dt=asat_dt,
            process=process,
            update_values=return_result,
        )
        return return_result

    end_process_task = end_process()

    for dag_id in (
        1,
        2,
        3,
        99,
    ):
        if dag_id == 99:
            t_label: str = "DEFAULT"
            t: BaseOperator = EmptyOperator(task_id="trigger-default")
        else:
            t_label: str = f"TYPE {dag_id}"
            t: BaseOperator = TriggerDagRunOperator(
                task_id=f"trigger-process-{dag_id}",
                trigger_dag_id=f"20_PROCESS_TYPE{dag_id}",
                # NOTE: Use parent running ID that passing from stream dag.
                trigger_run_id="{{ run_id }}",
                wait_for_completion=True,
                deferrable=False,
                reset_dag_run=True,
                execution_date="{{ execution_date }}",
                conf={
                    # NOTE: pass process object to each process type.
                    "process": (
                        "{{ task_instance.xcom_pull"
                        "('start_process', key='process_obj') }}"
                    ),
                    "asat_dt": "{{ params['asat_dt'] }}",
                },
            )

        chain(
            switch_process_load_task,
            Label(t_label),
            t,
            end_process_task,
        )


process_common()
