from pathlib import Path
from datetime import UTC, datetime, timedelta

from airflow.models import DagRun

import pendulum as pm
from airflow.decorators import dag
from airflow.models import Param
from airflow.utils.helpers import chain

from plugins.core.gateway import process_gateway
from plugins.utils.common import read_process, read_deployment


current_dir: Path = Path(__file__).parent
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


processes: dict[str, type[DagRun]] = {}
for dag_id, config in (
    read_process(file=current_dir / f'../conf/{name}.yaml')
    for name in (
        read_deployment(current_dir / '../conf/deployment.yaml').processes
    )
):
    if dag_id is None or config.id == 'EMPTY':
        continue

    @dag(
        # NOTE: Basic params
        dag_id=dag_id,
        start_date=pm.datetime(2024, 7, 31),
        schedule=None,
        catchup=False,
        # NOTE: UI params
        description=f"Generated process DAG: {dag_id}",
        tags=["process", "auto-gen"],
        # NOTE: Other params
        params={
            "asat_dt": Param(
                default=str(datetime.now(tz=UTC) - timedelta(days=90)),
                type="string",
                format="date-time",
                section="Important Params",
                description="Enter your override logical date that you want.",
            ),
        },
        default_args=default_args,
    )
    def process_common():
        tasks = process_gateway(process=config)
        chain(*tasks)


    processes[dag_id] = process_common()
