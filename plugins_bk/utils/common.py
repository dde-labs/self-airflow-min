from pathlib import Path
from typing import Any, Optional

import yaml
from yaml import CSafeLoader

from plugins_bk.models import Stream, Batch, Deployment, Process


def read_deployment(file: str) -> Deployment:
    """Read a deployment data from file template."""
    deploy_filename: Path = Path(file)
    if not deploy_filename.exists():
        return Deployment()

    with deploy_filename.open(mode='r', encoding='utf-8') as f:
        deploy_data: dict[str, Any] = yaml.load(f, CSafeLoader)
    return Deployment.model_validate(obj=deploy_data)


def read_process(file: str) -> tuple[Optional[str], Process]:
    process_filename: Path = Path(file)
    if not process_filename.exists():
        return None, Process(
            id='EMPTY',
            type=1,
            source={'table': 'EMPTY'},
            target={'table': 'EMPTY'},
        )

    with process_filename.open(mode='r', encoding='utf-8') as f:
        process_data: dict[str, Any] = yaml.load(f, CSafeLoader)
    return process_data['process_id'], Process.model_validate(obj=process_data)


def read_stream(file: str) -> tuple[Optional[str], Stream]:
    """Read a stream data from a file template."""
    stream_filename: Path = Path(file)
    if not stream_filename.exists():
        return None, Stream(id='EMPTY')

    with stream_filename.open(mode='r', encoding='utf-8') as f:
        stream_data: dict[str, Any] = yaml.load(f, CSafeLoader)
    return stream_data["stream_id"], Stream.model_validate(stream_data)


def read_batch(file: str) -> tuple[Optional[str], Batch]:
    """Read a batch data from a file template."""
    batch_filename: Path = Path(file)
    if not batch_filename.exists():
        return None, Batch(batch_id='EMPTY')

    with batch_filename.open(mode='r', encoding='utf-8') as f:
        batch_data: dict[str, Any] = yaml.load(f, CSafeLoader)
    return batch_data["batch_id"], Batch.model_validate(batch_data)


if __name__ == '__main__':
    current_dir = Path(__file__).parent
    dag_id, config = read_stream(
        file=current_dir / '../../dags/conf/s_ad_d.yaml'
    )
    first_groups = config.process_groups[0]
    print(first_groups)
    print(first_groups.priorities())

    dag_id, config = read_process(
        file=current_dir / '../../dags/conf/p_ad_process_1_1.yaml'
    )
    print(config)
