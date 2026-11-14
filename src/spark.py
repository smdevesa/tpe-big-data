import logging
import os
import sys
from functools import lru_cache

from src import config

log = logging.getLogger(__name__)

LOG4J_CONFIG = config.ROOT / "infra" / "log4j2.properties"


@lru_cache(maxsize=1)
def get_spark():
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.appName("cloud-provider-analytics")
        .master(config.SPARK_MASTER)
        .config("spark.driver.extraJavaOptions", f"-Dlog4j2.configurationFile={LOG4J_CONFIG.as_uri()}")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.debug.maxToStringFields", "200")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(config.SPARK_SHUFFLE_PARTITIONS))
        .getOrCreate()
    )
    log.info("SparkSession %s iniciada (master=%s)", spark.version, config.SPARK_MASTER)
    return spark
