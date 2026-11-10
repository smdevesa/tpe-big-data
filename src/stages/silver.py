import json
import logging

from pyspark.sql import functions as F

from src import config
from src.bronze.sources import INGEST_DATE
from src.silver.events import FLAGS, METRIC_FEATURES, to_silver
from src.silver.masters import conform_resources, latest_snapshot
from src.silver.quality import REJECTION_REASONS
from src.spark import get_spark

log = logging.getLogger(__name__)

SAMPLE_ROWS = 5
FEATURES = (*METRIC_FEATURES, "genai_tokens", "carbon_kg")
SAMPLE_COLUMNS = ("event_id", "event_date", "org_id", "service", "metric", "value", "unit", "cost_usd_increment")


def _as_json(df, limit=None):
    if limit:
        df = df.limit(limit)
    return [json.loads(row[0]) for row in df.select(F.to_json(F.struct("*"))).collect()]


def _write(df, path, partition_by=None):
    writer = df.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.parquet(str(path))


def _rule_counts(checked):
    contains = {reason: F.count_if(F.array_contains("reasons", reason)) for reason in REJECTION_REASONS}
    row = checked.agg(
        *[count.alias(reason) for reason, count in contains.items()],
        F.count_if("unit_missing_with_value").alias("unit_detected"),
        F.count_if(F.col("unit_missing_with_value") & ~F.array_contains("reasons", "missing_unit")).alias("unit_resolved"),
        F.count_if(F.col("_org_found").isNull()).alias("org_unmatched"),
        F.count_if(F.col("_resource_found").isNull()).alias("resource_unmatched"),
    ).first()
    return (
        {reason: row[reason] for reason in REJECTION_REASONS},
        {"detected": row["unit_detected"], "resolved": row["unit_resolved"]},
        {"org_id": row["org_unmatched"], "resource_id": row["resource_unmatched"]},
    )


def _flag_counts(events):
    row = events.agg(*[F.count_if(flag).alias(flag) for flag in FLAGS]).first()
    return {flag: row[flag] for flag in FLAGS}


def _feature_profile(events):
    row = events.agg(
        *[F.count(feature).alias(f"{feature}_rows") for feature in FEATURES],
        *[F.sum(feature).alias(f"{feature}_sum") for feature in FEATURES],
    ).first()
    return {
        feature: {"rows": row[f"{feature}_rows"], "sum": round(float(row[f"{feature}_sum"] or 0), 4)}
        for feature in FEATURES
    }


def _example(events):
    example = config.EVIDENCE_EXAMPLE
    rows = events.filter(
        (F.col("org_id") == example["org_id"])
        & (F.col("event_date") == F.lit(example["date"]).cast("date"))
        & (F.col("service") == example["service"])
    ).orderBy("event_ts", "event_id")
    totals = rows.agg(
        F.count("*").alias("events"),
        F.round(F.sum("cost_usd_increment"), 4).alias("cost_usd"),
        F.sum("requests").alias("requests"),
        F.count_if("value_missing").alias("value_missing"),
    ).first().asDict()
    return {"key": config.EVIDENCE_EXAMPLE, "totals": totals, "events": _as_json(rows.select(*SAMPLE_COLUMNS, "value_missing"))}


def _samples(result, rules):
    rejected = {
        reason: _as_json(result.rejected.filter(F.array_contains("reasons", reason)), SAMPLE_ROWS)
        for reason, count in rules.items() if count
    }
    flagged = _as_json(result.flagged, SAMPLE_ROWS)
    by_flag = {flag: _as_json(result.events.filter(flag).select(*SAMPLE_COLUMNS), SAMPLE_ROWS) for flag in FLAGS}
    return {"rejected": rejected, "flagged": flagged}, by_flag


def run(record):
    spark = get_spark()
    customers = latest_snapshot(spark.read.parquet(str(config.BRONZE / "customers_orgs")))
    resources = conform_resources(spark.read.parquet(str(config.BRONZE / "resources")))
    bronze = spark.read.parquet(str(config.BRONZE / "usage_events"))

    result = to_silver(bronze, customers, resources)
    result.checked.cache()

    bronze_rows = result.checked.count()
    silver_rows = result.events.count()
    rejected_rows = result.rejected.count()
    flagged_rows = result.flagged.count()
    if bronze_rows != silver_rows + rejected_rows:
        raise RuntimeError(f"usage_events: {bronze_rows} en Bronze != {silver_rows} Silver + {rejected_rows} rechazados")

    events_path = config.SILVER / "usage_events"
    quarantine_path = config.QUARANTINE / "silver_usage_events"
    _write(result.events.repartition("event_date"), events_path, "event_date")
    _write(result.rejected.unionByName(result.flagged).coalesce(1), quarantine_path)
    _write(customers, config.SILVER / "customers_orgs")
    _write(resources, config.SILVER / "resources")

    rules, unit, unmatched = _rule_counts(result.checked)
    flags = _flag_counts(result.events)
    quarantine_samples, flag_samples = _samples(result, rules)
    example = _example(result.events)
    masters = {
        name: {"rows": df.count(), INGEST_DATE: str(df.first()[INGEST_DATE])}
        for name, df in (("customers_orgs", customers), ("resources", resources))
    }

    record.input(config.BRONZE / "usage_events", bronze_rows)
    record.output(events_path, silver_rows)
    record.output(quarantine_path, rejected_rows + flagged_rows)
    for name, info in masters.items():
        record.output(config.SILVER / name, info["rows"])

    log.info(
        "usage_events: %d bronze -> %d silver | %d rechazados | %d flagged | unit nula con value: %d detectados, %d resueltos",
        bronze_rows, silver_rows, rejected_rows, flagged_rows, unit["detected"], unit["resolved"],
    )
    log.info("flags: %s", ", ".join(f"{flag}={count}" for flag, count in flags.items()))
    if rejected_rows:
        log.warning("motivos de rechazo: %s", {reason: count for reason, count in rules.items() if count})
    if flags["unknown_schema_version"]:
        log.warning(
            "%d eventos con schema_version desconocida: el esquema explícito no conserva campos nuevos",
            flags["unknown_schema_version"],
        )
    if any(unmatched.values()):
        log.warning("eventos sin maestro: %s", unmatched)
    log.info(
        "ejemplo %s: %d eventos, %.4f USD, %s requests, %d sin value",
        tuple(config.EVIDENCE_EXAMPLE.values()), example["totals"]["events"], example["totals"]["cost_usd"] or 0,
        example["totals"]["requests"], example["totals"]["value_missing"],
    )
    result.checked.unpersist()

    record.extra.update({
        "bronze_rows": bronze_rows,
        "silver_rows": silver_rows,
        "rejected_rows": rejected_rows,
        "flagged_rows": flagged_rows,
        "rules": rules,
        "unit_missing_with_value": unit,
        "flags": flags,
        "features": _feature_profile(result.events),
        "join_unmatched": unmatched,
        "masters": masters,
        "example": example,
        "quarantine_samples": quarantine_samples,
        "flag_samples": flag_samples,
    })
