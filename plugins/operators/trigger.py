from __future__ import annotations

from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def get_trigger_process(name: str) -> TriggerDagRunOperator:
    """Return generated trigger process common dag run operator.

    :param name: A process name that want to trigger to the common process dag.
    """
    return TriggerDagRunOperator(
        task_id=name,
        trigger_dag_id="10_PROCESS_COMMON",
        # NOTE: Run ID should concat with stream dag ID and process name.
        trigger_run_id=f"{{{{ run_id }}}}___{{{{ dag_run.dag_id }}}}___{name}",
        wait_for_completion=True,
        deferrable=False,
        reset_dag_run=True,
        conf={
            "process_name": name,
            "asat_dt": (
                "{{ task_instance.xcom_pull('start_stream')['asat_dt'] | ts }}"
            ),
            "mode": "{{ params['mode'] }}",
            "stream_id": (
                "{{ task_instance.xcom_pull('start_stream')['stream_id'] }}"
            ),
        },
    )


# NOTE: Create new function name.
TriggerProcessOperator = get_trigger_process
