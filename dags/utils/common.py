from pathlib import Path
from typing import Any

import yaml
from yaml import CSafeLoader


def read_stream(name: str) -> dict[str, Any]:
    stream_filename: Path = Path(f'../conf/{name}.yaml')
    if not stream_filename.exists():
        return {}
    with stream_filename.open(mode='r', encoding='utf-8') as f:
        stream_data: dict[str, Any] = yaml.load(f, CSafeLoader)
    return stream_data
