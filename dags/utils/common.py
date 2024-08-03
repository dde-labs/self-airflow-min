from pathlib import Path
from typing import Any, Optional

import yaml
from yaml import CSafeLoader


def read_stream(file: str) -> tuple[Optional[str], dict[str, Any]]:
    stream_filename: Path = Path(file)
    if not stream_filename.exists():
        return None, {}

    with stream_filename.open(mode='r', encoding='utf-8') as f:
        stream_data: dict[str, Any] = yaml.load(f, CSafeLoader)
    return stream_data["stream_id"], stream_data


if __name__ == '__main__':
    current_dir = Path(__file__).parent
    dag_id, config = read_stream(file=current_dir / '../conf/s_ad_d.yaml')
    first_groups = config['process_groups'][0]['processes']
    print(first_groups)
    values = set(map(lambda x: x.get('priority', 99), first_groups))
    print(values)

    new_first_groups = [
        [y for y in first_groups if y.get('priority', 99) == x] for x in values
    ]
    print(new_first_groups)
