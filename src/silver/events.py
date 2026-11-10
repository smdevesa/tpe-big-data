from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.silver.masters import CUSTOMER_COLUMNS
from src.silver.quality import COST_BELOW_THRESHOLD, apply_quality

METRIC_FEATURES = ("requests", "cpu_hours", "storage_gb_hours")
FLAGS = ("cost_anomaly", "value_missing", "unit_inferred", "before_resource_created", "unknown_schema_version")
SILVER_COLUMNS = (
    "event_id", "event_ts", "event_date", "org_id", *CUSTOMER_COLUMNS,
    "resource_id", "resource_state", "resource_created_at", "service", "region",
    "metric", "value", "unit", "cost_usd_increment", *METRIC_FEATURES, "genai_tokens", "carbon_kg",
    "schema_version", *FLAGS, "source_file", "ingest_ts",
)


@dataclass
class SilverResult:
    events: DataFrame
    rejected: DataFrame
    flagged: DataFrame
    checked: DataFrame


def add_features(events: DataFrame) -> DataFrame:
    return events.withColumns({
        metric: F.when(F.col("metric") == metric, F.col("value")) for metric in METRIC_FEATURES
    })


def enrich(events: DataFrame, customers: DataFrame, resources: DataFrame) -> DataFrame:
    orgs = customers.select("org_id", *CUSTOMER_COLUMNS, F.lit(True).alias("_org_found"))
    resource_info = resources.select(
        "resource_id",
        F.col("state").alias("resource_state"),
        F.col("created_at").alias("resource_created_at"),
        F.lit(True).alias("_resource_found"),
    )
    return (
        events.join(F.broadcast(orgs), "org_id", "left")
        .join(F.broadcast(resource_info), "resource_id", "left")
        .withColumn(
            "before_resource_created",
            F.coalesce(F.col("event_date") < F.col("resource_created_at"), F.lit(False)),
        )
    )


def _quarantine(df, action, reason):
    return df.select(
        F.lit("usage_events").alias("source"),
        F.lit(action).alias("action"),
        reason.alias("reason"),
        "reasons",
        F.col("_raw").alias("raw"),
        "source_file",
        "ingest_ts",
    )


def to_silver(bronze: DataFrame, customers: DataFrame, resources: DataFrame) -> SilverResult:
    original = bronze.withColumn("_raw", F.to_json(F.struct(*bronze.columns)))
    checked = enrich(add_features(apply_quality(original)), customers, resources)
    valid = checked.filter(~F.col("rejected"))
    flagged = valid.filter("cost_anomaly").withColumn("reasons", F.array(F.lit(COST_BELOW_THRESHOLD)))
    return SilverResult(
        events=valid.select(*SILVER_COLUMNS),
        rejected=_quarantine(checked.filter("rejected"), "rejected", F.col("reason")),
        flagged=_quarantine(flagged, "flagged", F.lit(COST_BELOW_THRESHOLD)),
        checked=checked,
    )
