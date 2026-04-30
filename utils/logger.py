import logging
from pathlib import Path

_LOG_FILE = Path(__file__).resolve().parents[1] / 'log' / 'logs.log'


def configure_logging() -> None:
    logging.basicConfig(
        filename=_LOG_FILE,
        level=logging.INFO,
        format='%(asctime)s %(levelname)-8s %(name)s  %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
