import logging
import sys
import time

LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)-24s | %(message)s"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
THIRD_PARTY_LOGGERS = ("py4j", "cassandra")


def _formatter():
    formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)
    formatter.converter = time.gmtime
    return formatter


def log_to_file(log_file, mode="a"):
    project_logger = logging.getLogger("src")
    for handler in [h for h in project_logger.handlers if isinstance(h, logging.FileHandler)]:
        project_logger.removeHandler(handler)
        handler.close()
    log_file.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(log_file, mode=mode, encoding="utf-8")
    handler.setFormatter(_formatter())
    project_logger.addHandler(handler)


def configure_logging(log_file, level="INFO", mode="a"):
    project_logger = logging.getLogger("src")
    project_logger.handlers.clear()
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(_formatter())
    project_logger.addHandler(console)
    log_to_file(log_file, mode)
    project_logger.setLevel(level)
    project_logger.propagate = False

    for name in THIRD_PARTY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
