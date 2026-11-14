import json
import logging
from dataclasses import dataclass

from pyspark.sql import functions as F

from src import config
from src.bronze.events import count_late_arrivals, deduplicate, landing_schema, parse_events
from src.bronze.transforms import CORRUPT_RECORD, landing_relative_path
from src.spark import get_spark

log = logging.getLogger(__name__)

SOURCE_DIR = "usage_events_stream"
TABLE = "usage_events"
SAMPLE_ROWS = 3


@dataclass
class StreamRun:
    micro_batches: int
    input_rows: int
    duplicates_dropped: int
    dropped_by_watermark: int

    @property
    def written_rows(self):
        return self.input_rows - self.duplicates_dropped - self.dropped_by_watermark


def run_stream(spark, source_dir, output_dir, checkpoint_dir, watermark, max_files_per_trigger):
    raw = (
        spark.readStream.schema(landing_schema())
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD)
        .json(str(source_dir))
        .withColumn("source_file", landing_relative_path(F.col("_metadata.file_path"), str(config.LANDING.resolve())))
    )
    query = (
        deduplicate(parse_events(raw), watermark)
        .repartition("event_date")
        .writeStream.format("parquet")
        .partitionBy("event_date")
        .option("checkpointLocation", str(checkpoint_dir))
        .trigger(availableNow=True)
        .start(str(output_dir))
    )
    query.awaitTermination()

    progress = query.recentProgress
    operators = [operator for update in progress for operator in update.stateOperators]
    return StreamRun(
        micro_batches=sum(1 for update in progress if update.numInputRows),
        input_rows=sum(update.numInputRows for update in progress),
        duplicates_dropped=sum(operator.customMetrics.get("numDroppedDuplicateRows", 0) for operator in operators),
        dropped_by_watermark=sum(operator.numRowsDroppedByWatermark for operator in operators),
    )


def _duration(seconds):
    if seconds is None:
        return None
    minutes = seconds // 60
    return f"{minutes // 1440} d {minutes % 1440 // 60} h {minutes % 60} min"


def _counts_by(df, column):
    return {str(row[0]): row["count"] for row in df.groupBy(column).count().orderBy(column).collect()}


def _bronze_profile(bronze):
    stats = bronze.agg(
        F.count("*").alias("rows"),
        F.count("corrupt_record").alias("corrupt_rows"),
        F.count_if(F.col("event_id").isNull()).alias("null_event_id"),
        F.count_if(F.col("event_ts").isNull()).alias("null_event_ts"),
    ).first()
    return {
        "bronze_rows": stats["rows"],
        "schema_versions": _counts_by(bronze, "schema_version"),
        "cast_errors": _counts_by(bronze.select(F.explode("cast_errors").alias("column")), "column"),
        "corrupt_rows": stats["corrupt_rows"],
        "null_event_id": stats["null_event_id"],
        "null_event_ts": stats["null_event_ts"],
    }


def run(record):
    spark = get_spark()
    source_dir = config.LANDING / SOURCE_DIR
    bronze_path = config.BRONZE / TABLE
    checkpoint_dir = config.CHECKPOINTS / f"bronze_{TABLE}"
    files = len(list(source_dir.glob("*.jsonl")))

    stream = run_stream(
        spark, source_dir, bronze_path, checkpoint_dir,
        config.STREAM_WATERMARK, config.STREAM_MAX_FILES_PER_TRIGGER,
    )
    if stream.dropped_by_watermark:
        log.warning(
            "%s: %d eventos descartados por llegar después del watermark (%s)",
            TABLE, stream.dropped_by_watermark, config.STREAM_WATERMARK,
        )

    bronze = spark.read.parquet(str(bronze_path)).cache()
    profile = _bronze_profile(bronze)
    late = count_late_arrivals(bronze)
    late_pct = round(100 * late.rows / profile["bronze_rows"], 1) if profile["bronze_rows"] else 0.0
    sample = bronze.orderBy("event_id").limit(SAMPLE_ROWS).select(F.to_json(F.struct("*"))).collect()
    bronze.unpersist()

    record.input(source_dir, stream.input_rows)
    record.output(bronze_path, profile["bronze_rows"])
    files_written = record.outputs[-1].get("files", 0)
    record.output(checkpoint_dir)
    partitions = sorted(child.name for child in bronze_path.iterdir() if child.name.startswith("event_date="))

    log.info(
        "%s: %d archivos, %d micro-lotes, %d leídos -> %d escritos (Bronze total %d) | %d duplicados | "
        "%d descartados por watermark | %d fuera de orden (%.1f%%, atraso máximo %s)",
        TABLE, files, stream.micro_batches, stream.input_rows, stream.written_rows, profile["bronze_rows"],
        stream.duplicates_dropped, stream.dropped_by_watermark, late.rows, late_pct, _duration(late.max_lateness_s),
    )

    record.extra.update({
        "files": files,
        "watermark": config.STREAM_WATERMARK,
        "max_files_per_trigger": config.STREAM_MAX_FILES_PER_TRIGGER,
        "micro_batches": stream.micro_batches,
        "input_rows": stream.input_rows,
        "written_rows": stream.written_rows,
        "duplicates_dropped": stream.duplicates_dropped,
        "dropped_by_watermark": stream.dropped_by_watermark,
        **profile,
        "late_arrivals": late.rows,
        "late_arrivals_pct": late_pct,
        "max_lateness": _duration(late.max_lateness_s),
        "partitions": len(partitions),
        "files_written": files_written,
        "sample": [json.loads(row[0]) for row in sample],
    })
