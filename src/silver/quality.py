from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

UNIT_BY_METRIC = {"requests": "count", "cpu_hours": "hours", "storage_gb_hours": "gb_hours"}
COST_ANOMALY_THRESHOLD = -0.01
CATEGORICAL = ("service", "region", "metric", "unit")

REJECTION_REASONS = (
    "missing_event_id",
    "duplicate_event_id",
    "malformed_event",
    "invalid_timestamp",
    "missing_org_id",
    "missing_service",
    "missing_unit",
)
COST_BELOW_THRESHOLD = "cost_below_threshold"


def _rejection_conditions():
    return dict(zip(REJECTION_REASONS, (
        F.col("event_id").isNull(),
        F.col("_event_copy") > 1,
        F.col("corrupt_record").isNotNull(),
        F.col("event_ts").isNull(),
        F.col("org_id").isNull(),
        F.col("service").isNull(),
        F.col("value").isNotNull() & F.col("unit").isNull(),
    )))


def _normalized(column: str) -> Column:
    trimmed = F.lower(F.trim(F.col(column)))
    return F.when(trimmed != "", trimmed)


def _inferred_unit() -> Column:
    mapping = F.create_map(*[F.lit(item) for pair in UNIT_BY_METRIC.items() for item in pair])
    return F.coalesce(F.col("unit"), mapping[F.col("metric")])


def apply_quality(events: DataFrame) -> DataFrame:
    copies = Window.partitionBy("event_id").orderBy("ingest_ts", "source_file")
    reasons = F.filter(
        F.array(*[F.when(condition, F.lit(reason)) for reason, condition in _rejection_conditions().items()]),
        lambda reason: reason.isNotNull(),
    )
    return (
        events.withColumns({column: _normalized(column) for column in CATEGORICAL})
        .withColumn("unit_missing_with_value", F.col("unit").isNull() & F.col("value").isNotNull())
        .withColumn("unit_inferred", F.col("unit").isNull() & _inferred_unit().isNotNull())
        .withColumn("unit", _inferred_unit())
        .withColumn("_event_copy", F.when(F.col("event_id").isNotNull(), F.row_number().over(copies)))
        .withColumn("reasons", reasons)
        .withColumn("reason", F.get("reasons", 0))
        .withColumn("rejected", F.size("reasons") > 0)
        .withColumn("cost_anomaly", F.coalesce(F.col("cost_usd_increment") < COST_ANOMALY_THRESHOLD, F.lit(False)))
        .withColumn("value_missing", F.col("value").isNull())
        .drop("_event_copy")
    )
