import logging
import os
import sys
from functools import lru_cache

from src import config

log = logging.getLogger(__name__)

SHUFFLE_PARTITIONS = 8


@lru_cache(maxsize=1)
def get_spark():
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.appName("cloud-provider-analytics")
        .master(config.SPARK_MASTER)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession %s iniciada (master=%s)", spark.version, config.SPARK_MASTER)
    return spark
