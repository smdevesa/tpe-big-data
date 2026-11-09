import json
import logging
import shutil
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src import config
from src.bronze.sources import BATCH_SOURCES, INGEST_DATE
from src.bronze.transforms import CORRUPT_RECORD, landing_relative_path, landing_schema, to_bronze
from src.spark import get_spark

log = logging.getLogger(__name__)

SAMPLE_ROWS = 3


def _read_landing(spark, source):
    path = config.LANDING / source.file_name
    header = spark.read.option("header", True).csv(str(path)).columns
    if tuple(header) != source.columns:
        raise ValueError(f"{source.file_name}: encabezado {header} no coincide con {list(source.columns)}")

    return (
        spark.read.schema(landing_schema(source))
        .option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD)
        .csv(str(path))
        .withColumn("source_file", landing_relative_path(F.col("_metadata.file_path"), str(config.LANDING.resolve())))
        .cache()
    )


def _write_partitioned(df, path, partition_by):
    (
        df.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy(partition_by)
        .parquet(str(path))
    )


def _write_quarantine(df, rows, path, ingest_date):
    if rows:
        _write_partitioned(df, path, INGEST_DATE)
        return
    stale = path / f"{INGEST_DATE}={ingest_date.isoformat()}"
    if stale.exists():
        shutil.rmtree(stale)


def _partitions(path):
    if not path.exists():
        return []
    return sorted(child.name for child in path.iterdir() if child.is_dir() and "=" in child.name)


def _ingest(spark, source, ingest_ts, record):
    raw = _read_landing(spark, source)
    result = to_bronze(raw, source, ingest_ts)
    bronze_path = config.BRONZE / source.name
    quarantine_path = config.QUARANTINE / f"bronze_{source.name}"

    landing_rows = raw.count()
    bronze_rows = result.bronze.count()
    quarantine_rows = result.quarantine.count()
    exact_duplicates = result.exact_duplicates.count()
    if landing_rows != bronze_rows + quarantine_rows + exact_duplicates:
        raise RuntimeError(
            f"{source.name}: {landing_rows} filas en Landing != {bronze_rows} Bronze + "
            f"{quarantine_rows} quarantine + {exact_duplicates} duplicados exactos"
        )

    _write_partitioned(result.bronze, bronze_path, source.partition_by)
    _write_quarantine(result.quarantine, quarantine_rows, quarantine_path, ingest_ts.date())

    record.input(config.LANDING / source.file_name, landing_rows)
    record.output(bronze_path, bronze_rows)
    record.output(quarantine_path, quarantine_rows)

    by_reason = {row["reason"]: row["count"] for row in result.quarantine.groupBy("reason").count().collect()}
    if quarantine_rows:
        log.warning("%s: %d filas a quarantine %s", source.name, quarantine_rows, by_reason)
    sample = result.bronze.orderBy(source.key).limit(SAMPLE_ROWS).select(F.to_json(F.struct("*"))).collect()
    raw.unpersist()

    return {
        "landing_rows": landing_rows,
        "bronze_rows": bronze_rows,
        "quarantine_rows": quarantine_rows,
        "exact_duplicates": exact_duplicates,
        "quarantine_by_reason": by_reason,
        "partitions": _partitions(bronze_path),
        "schema": result.bronze.schema.simpleString(),
        "sample": [json.loads(row[0]) for row in sample],
    }


def run(record):
    spark = get_spark()
    ingest_ts = datetime.now(timezone.utc).replace(microsecond=0)
    record.extra["ingest_ts"] = ingest_ts.isoformat()
    for source in BATCH_SOURCES:
        record.extra[source.name] = _ingest(spark, source, ingest_ts, record)
