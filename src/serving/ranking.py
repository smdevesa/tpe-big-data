from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

WINDOW_DAYS = 14


def top_services(mart: DataFrame, window_days: int = WINDOW_DAYS) -> DataFrame:
    latest = mart.agg(F.max("usage_date").alias("_latest"))
    contributions = mart.select(
        "org_id",
        "service",
        "daily_cost_usd",
        F.explode(F.sequence("usage_date", F.date_add("usage_date", window_days - 1))).alias("as_of_date"),
    )
    ranking = Window.partitionBy("org_id", "as_of_date").orderBy(F.desc("cost_14d_usd"), "service")
    return (
        contributions.crossJoin(latest)
        .filter(F.col("as_of_date") <= F.col("_latest"))
        .groupBy("org_id", "as_of_date", "service")
        .agg(
            F.round(F.sum("daily_cost_usd"), 4).alias("cost_14d_usd"),
            F.count("*").cast("int").alias("days_with_usage"),
        )
        .withColumn("window_start", F.date_sub("as_of_date", window_days - 1))
        .withColumn("rank", F.row_number().over(ranking))
    )
