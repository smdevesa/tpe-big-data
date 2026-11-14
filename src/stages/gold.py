import json
import logging
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src import config
from src.gold.usage import ANOMALY_THRESHOLD, DECIMALS, GRAIN, build_mart
from src.spark import get_spark

log = logging.getLogger(__name__)

MART = "org_daily_usage_by_service"
TOP_ANOMALIES = 10
SAMPLE_ROWS = 3
ANOMALY_COLUMNS = (*GRAIN, "daily_cost_usd", "cost_median_usd", "cost_mad_usd", "cost_robust_z", "events")


def _as_json(df):
    return [json.loads(row[0]) for row in df.select(F.to_json(F.struct("*"))).collect()]


def _check_grain(mart):
    repeated = mart.groupBy(*GRAIN).count().filter("count > 1").count()
    if repeated:
        raise RuntimeError(f"{MART}: {repeated} claves {GRAIN} repetidas")


def _reconcile(silver, mart, rows):
    source = silver.agg(
        F.sum("cost_usd_increment").alias("cost_usd"), F.sum("requests").alias("requests"), F.count("*").alias("events")
    ).first()
    target = mart.agg(
        F.sum("daily_cost_usd").alias("cost_usd"), F.sum("requests").alias("requests"), F.sum("events").alias("events")
    ).first()
    rounding_tolerance = rows * 0.5 * 10 ** -DECIMALS
    checks = {
        "cost_usd": abs(source["cost_usd"] - target["cost_usd"]) <= rounding_tolerance,
        "requests": abs(source["requests"] - target["requests"]) <= rounding_tolerance,
        "events": source["events"] == target["events"],
    }
    report = {
        name: {"silver": round(source[name], 2), "gold": round(target[name], 2), "ok": ok}
        for name, ok in checks.items()
    }
    if not all(checks.values()):
        raise RuntimeError(f"{MART}: no concilia con Silver {report}")
    return report


def _anomalies(mart):
    row = mart.agg(
        F.count_if("is_cost_anomaly").alias("total"),
        F.count_if(F.col("is_cost_anomaly") & (F.col("cost_robust_z") > 0)).alias("high"),
        F.count_if(F.col("is_cost_anomaly") & (F.col("cost_robust_z") < 0)).alias("low"),
        F.count_distinct(F.when(F.col("cost_robust_z").isNull(), F.struct("org_id", "service"))).alias("series_without_reference"),
    ).first().asDict()
    top = mart.filter("is_cost_anomaly").orderBy(F.desc(F.abs("cost_robust_z"))).limit(TOP_ANOMALIES)
    return {**row, "threshold": ANOMALY_THRESHOLD}, _as_json(top.select(*ANOMALY_COLUMNS))


def _series_profile(mart):
    days = mart.groupBy("org_id", "service").count()
    row = days.agg(
        F.count("*").alias("series"),
        F.min("count").alias("min"),
        F.percentile("count", 0.5).alias("median"),
        F.max("count").alias("max"),
    ).first()
    return row["series"], {"min": row["min"], "median": row["median"], "max": row["max"]}


def _example(mart):
    example = config.EVIDENCE_EXAMPLE
    rows = mart.filter(
        (F.col("org_id") == example["org_id"])
        & (F.col("usage_date") == F.lit(example["date"]).cast("date"))
        & (F.col("service") == example["service"])
    )
    return _as_json(rows.drop("processed_at"))


def run(record):
    spark = get_spark()
    silver_path = config.SILVER / "usage_events"
    mart_path = config.GOLD / MART
    processed_at = datetime.now(timezone.utc).replace(microsecond=0)

    silver = spark.read.parquet(str(silver_path))
    mart = build_mart(silver, processed_at).cache()
    rows = mart.count()

    _check_grain(mart)
    reconciliation = _reconcile(silver, mart, rows)
    mart.repartition("usage_date").write.mode("overwrite").partitionBy("usage_date").parquet(str(mart_path))

    series, days_per_series = _series_profile(mart)
    anomalies, top = _anomalies(mart)
    example = _example(mart)
    orgs = mart.select("org_id").distinct().count()
    sample = _as_json(mart.orderBy(*GRAIN).limit(SAMPLE_ROWS).drop("processed_at"))
    mart.unpersist()

    record.input(silver_path)
    record.output(mart_path, rows)

    log.info(
        "%s: %d filas (%d orgs, %d series de %s-%s días) | concilia con Silver: %.2f USD, %.0f requests, %d eventos",
        MART, rows, orgs, series, days_per_series["min"], days_per_series["max"],
        reconciliation["cost_usd"]["gold"], reconciliation["requests"]["gold"], reconciliation["events"]["gold"],
    )
    log.info(
        "anomalías de costo (|z| > %.1f): %d días (%d altos, %d bajos), %d series sin referencia",
        ANOMALY_THRESHOLD, anomalies["total"], anomalies["high"], anomalies["low"], anomalies["series_without_reference"],
    )
    if example:
        log.info(
            "ejemplo %s: %.4f USD, %s requests, %d eventos, %d sin value",
            tuple(config.EVIDENCE_EXAMPLE.values()), example[0]["daily_cost_usd"], example[0].get("requests"),
            example[0]["events"], example[0]["events_missing_value"],
        )
    else:
        log.warning("ejemplo %s: no existe en el mart", tuple(config.EVIDENCE_EXAMPLE.values()))

    record.extra.update({
        "rows": rows,
        "orgs": orgs,
        "series": series,
        "days_per_series": days_per_series,
        "processed_at": processed_at.isoformat(),
        "reconciliation": reconciliation,
        "anomalies": anomalies,
        "top_anomalies": top,
        "example": example,
        "sample": sample,
    })
