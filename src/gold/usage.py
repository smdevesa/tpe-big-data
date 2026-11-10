from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

GRAIN = ("usage_date", "org_id", "service")
ORG_ATTRIBUTES = ("org_name", "plan_tier")
SUMMED_FEATURES = ("requests", "cpu_hours", "storage_gb_hours", "genai_tokens", "carbon_kg")
DECIMALS = 4

ANOMALY_THRESHOLD = 3.5
MAD_SCALE = 0.6745
MIN_SERIES_DAYS = 7


def daily_usage(silver: DataFrame) -> DataFrame:
    return (
        silver.withColumnRenamed("event_date", "usage_date")
        .groupBy(*GRAIN, *ORG_ATTRIBUTES)
        .agg(
            F.round(F.sum("cost_usd_increment"), DECIMALS).alias("daily_cost_usd"),
            *[F.round(F.sum(feature), DECIMALS).alias(feature) for feature in SUMMED_FEATURES],
            F.count("*").alias("events"),
            F.count_if("value_missing").alias("events_missing_value"),
            F.count_if("cost_anomaly").alias("negative_cost_events"),
        )
    )


def flag_cost_anomalies(daily: DataFrame) -> DataFrame:
    series = Window.partitionBy("org_id", "service")
    cost = F.col("daily_cost_usd")
    median, mad = F.col("_median"), F.col("_mad")
    has_reference = (F.col("series_days") >= MIN_SERIES_DAYS) & (mad > 0)

    return (
        daily.withColumn("series_days", F.count("*").over(series))
        .withColumn("_median", F.percentile(cost, 0.5).over(series))
        .withColumn("_mad", F.percentile(F.abs(cost - median), 0.5).over(series))
        .withColumn("cost_robust_z", F.when(has_reference, F.round(F.lit(MAD_SCALE) * (cost - median) / mad, DECIMALS)))
        .withColumn("is_cost_anomaly", F.coalesce(F.abs("cost_robust_z") > ANOMALY_THRESHOLD, F.lit(False)))
        .withColumn("cost_median_usd", F.round(median, DECIMALS))
        .withColumn("cost_mad_usd", F.round(mad, DECIMALS))
        .drop("_median", "_mad")
    )


def build_mart(silver: DataFrame, processed_at) -> DataFrame:
    return flag_cost_anomalies(daily_usage(silver)).withColumn("processed_at", F.lit(processed_at).cast("timestamp"))
