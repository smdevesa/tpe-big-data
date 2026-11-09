import logging
from functools import lru_cache

from src import config

log = logging.getLogger(__name__)

# Debe acompañar a la versión de PySpark de requirements.txt (Spark 3.5, Scala 2.12).
CASSANDRA_CONNECTOR = "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"
SHUFFLE_PARTITIONS = 8


@lru_cache(maxsize=1)
def get_spark():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("cloud-provider-analytics")
        .master(config.SPARK_MASTER)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
        .config("spark.jars.packages", CASSANDRA_CONNECTOR)
        .config("spark.cassandra.connection.host", config.CASSANDRA_HOST)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession %s iniciada (master=%s)", spark.version, config.SPARK_MASTER)
    return spark
