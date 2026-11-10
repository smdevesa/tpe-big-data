from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.bronze.sources import INGEST_DATE

CUSTOMER_COLUMNS = ("org_name", "industry", "plan_tier", "is_enterprise", "hq_region")


def latest_snapshot(df: DataFrame) -> DataFrame:
    latest = df.agg(F.max(INGEST_DATE)).first()[0]
    return df.filter(F.col(INGEST_DATE) == F.lit(latest))


def conform_resources(bronze: DataFrame) -> DataFrame:
    return latest_snapshot(bronze).withColumn("tags", F.from_json("tags_json", "array<string>"))
