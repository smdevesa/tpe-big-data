from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.bronze.transforms import CORRUPT_RECORD

FIELDS = (
    "event_id", "timestamp", "org_id", "resource_id", "service", "region", "metric", "value", "unit",
    "cost_usd_increment", "schema_version", "carbon_kg", "genai_tokens",
)
NUMERIC_TYPES = {
    "value": "double",
    "cost_usd_increment": "double",
    "schema_version": "int",
    "carbon_kg": "double",
    "genai_tokens": "bigint",
}
BRONZE_COLUMNS = (
    "event_id", "event_ts", "event_date", "org_id", "resource_id", "service", "region", "metric",
    "value", "value_raw", "unit", "cost_usd_increment", "schema_version", "carbon_kg", "genai_tokens",
    "cast_errors", "corrupt_record", "source_file", "ingest_ts",
)


@dataclass
class LateArrivals:
    rows: int
    max_lateness_s: int | None


def landing_schema():
    fields = [T.StructField(name, T.StringType()) for name in FIELDS]
    return T.StructType(fields + [T.StructField(CORRUPT_RECORD, T.StringType())])


def _failed(column, parsed):
    return F.when(F.col(column).isNotNull() & parsed.isNull(), F.lit(column))


def parse_events(raw: DataFrame) -> DataFrame:
    event_ts = F.try_to_timestamp("timestamp")
    failures = [_failed("timestamp", event_ts)] + [
        _failed(column, F.col(column).try_cast(target)) for column, target in NUMERIC_TYPES.items()
    ]
    record = F.coalesce(F.col(CORRUPT_RECORD), F.to_json(F.struct(*FIELDS)))
    dedupe_key = F.coalesce(
        F.col("event_id"),
        F.concat(F.lit("sin_id:"), F.sha2(F.concat_ws("|", F.col("source_file"), record), 256)),
    )

    return (
        raw.withColumn("value_raw", F.col("value"))
        .withColumn("cast_errors", F.filter(F.array(*failures), lambda column: column.isNotNull()))
        .withColumn("dedupe_key", dedupe_key)
        .withColumn("event_ts", event_ts)
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumns({column: F.col(column).try_cast(target) for column, target in NUMERIC_TYPES.items()})
        .withColumn("corrupt_record", F.col(CORRUPT_RECORD))
        .withColumn("ingest_ts", F.current_timestamp())
        .select(*BRONZE_COLUMNS, "dedupe_key")
    )


def deduplicate(events: DataFrame, watermark: str) -> DataFrame:
    return (
        events.withWatermark("event_ts", watermark)
        .dropDuplicatesWithinWatermark(["dedupe_key"])
        .drop("dedupe_key")
    )


def count_late_arrivals(events: DataFrame) -> LateArrivals:
    batches = events.groupBy("ingest_ts").agg(F.max("event_ts").alias("batch_max"))
    earlier = batches.select(F.col("ingest_ts").alias("earlier_ts"), F.col("batch_max").alias("earlier_max"))
    previous_max = (
        batches.join(earlier, F.col("earlier_ts") < F.col("ingest_ts"))
        .groupBy("ingest_ts")
        .agg(F.max("earlier_max").alias("previous_max"))
    )
    late = (
        events.join(previous_max, "ingest_ts")
        .filter(F.col("event_ts") < F.col("previous_max"))
        .select((F.unix_timestamp("previous_max") - F.unix_timestamp("event_ts")).alias("lateness_s"))
    )
    result = late.agg(F.count("*").alias("rows"), F.max("lateness_s").alias("max_lateness_s")).first()
    return LateArrivals(rows=result["rows"], max_lateness_s=result["max_lateness_s"])
