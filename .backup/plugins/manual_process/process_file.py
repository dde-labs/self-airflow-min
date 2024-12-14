"""
The file will use for generate the Process function from a config template
"""
import logging
from typing import Any


from plugins_bk.models import Source, Target


def process_file(
    source: Source,
    target: Target,
    extra: dict[str, Any],
):
    logging.info("Start process type 1")
    logging.info(f"... Loading data from {source} to {target}")
    logging.info(f"... Extra params: {extra}")
