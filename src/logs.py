import logging
import sys
import time

LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)-24s | %(message)s"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
THIRD_PARTY_LOGGERS = ("py4j", "cassandra")


def configure_logging(log_file, level="INFO"):
    formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)
    formatter.converter = time.gmtime

    log_file.parent.mkdir(parents=True, exist_ok=True)
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
    ]

    project_logger = logging.getLogger("src")
    project_logger.handlers.clear()
    for handler in handlers:
        handler.setFormatter(formatter)
        project_logger.addHandler(handler)
    project_logger.setLevel(level)
    project_logger.propagate = False

    for name in THIRD_PARTY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
