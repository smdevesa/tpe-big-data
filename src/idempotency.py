from pyspark.sql import functions as F

from src import config
from src.bronze.sources import BATCH_SOURCES, INGEST_DATE

CASSANDRA_TABLES = ("usage_by_org_service_day", "top_services_14d")
GOLD_KEY = ("usage_date", "org_id", "service")


def _read(spark, path):
    return spark.read.parquet(str(path))


def _event_measures(df, with_cost=False):
    aggregates = [
        F.count("*").alias("rows"),
        F.count("event_id").alias("rows_with_event_id"),
        F.count_distinct("event_id").alias("distinct_event_id"),
    ]
    if with_cost:
        aggregates.append(F.round(F.sum("cost_usd_increment"), 2).alias("cost_usd"))
    return df.agg(*aggregates).first().asDict()


def _master_rows(spark, source):
    df = _read(spark, config.BRONZE / source.name)
    if source.partition_by == INGEST_DATE:
        latest = df.agg(F.max(INGEST_DATE)).first()[0]
        df = df.filter(F.col(INGEST_DATE) == F.lit(latest))
    return {"rows": df.count()}


def snapshot(spark, session):
    quarantine = _read(spark, config.QUARANTINE / "silver_usage_events")
    gold = _read(spark, config.GOLD / "org_daily_usage_by_service")
    measures = {f"bronze.{source.name}": _master_rows(spark, source) for source in BATCH_SOURCES}
    measures["bronze.usage_events"] = _event_measures(_read(spark, config.BRONZE / "usage_events"))
    measures["silver.usage_events"] = _event_measures(_read(spark, config.SILVER / "usage_events"), with_cost=True)
    measures["quarantine.silver_usage_events"] = quarantine.agg(
        F.count_if(F.col("action") == "rejected").alias("rejected"),
        F.count_if(F.col("action") == "flagged").alias("flagged"),
    ).first().asDict()
    measures["gold.org_daily_usage_by_service"] = gold.agg(
        F.count("*").alias("rows"),
        F.count_distinct(*GOLD_KEY).alias("distinct_keys"),
        F.round(F.sum("daily_cost_usd"), 2).alias("cost_usd"),
        F.round(F.sum("requests"), 2).alias("requests"),
    ).first().asDict()
    for table in CASSANDRA_TABLES:
        count = session.execute(f"SELECT COUNT(*) FROM {config.CASSANDRA_KEYSPACE}.{table}").one()[0]
        measures[f"cassandra.{table}"] = {"rows": count}
    return measures


def _unique_keys(measures):
    checks = {}
    for zone, values in measures.items():
        if "distinct_event_id" in values:
            checks[zone] = values["distinct_event_id"] == values["rows_with_event_id"]
        elif "distinct_keys" in values:
            checks[zone] = values["distinct_keys"] == values["rows"]
    return checks


def compare(first, second, second_run_stream_rows):
    differences = [
        {"zone": zone, "measure": measure, "first": value, "second": second.get(zone, {}).get(measure)}
        for zone, values in first.items()
        for measure, value in values.items()
        if second.get(zone, {}).get(measure) != value
    ]
    unique = {"first": _unique_keys(first), "second": _unique_keys(second)}
    checks = {
        "same_measures": not differences,
        "unique_keys": all(unique["first"].values()) and all(unique["second"].values()),
        "stream_read_nothing_new": second_run_stream_rows == 0,
    }
    return {
        "result": "ok" if all(checks.values()) else "failed",
        "checks": checks,
        "differences": differences,
        "unique_keys": unique,
        "second_run_stream_rows": second_run_stream_rows,
        "first": first,
        "second": second,
    }
