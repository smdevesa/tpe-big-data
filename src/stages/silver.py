import logging

log = logging.getLogger(__name__)


def run(record):
    log.warning("etapa pendiente de implementación (spec 03)")
    record.extra["pending"] = "spec 03"
