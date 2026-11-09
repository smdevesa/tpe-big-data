from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.bronze.sources import INGEST_DATE

CORRUPT_RECORD = "_corrupt_record"
EXACT_DUPLICATE = "exact_duplicate"
DUPLICATE_KEY = "duplicate_key"
MALFORMED_ROW = "malformed_row"


@dataclass
class BronzeResult:
    bronze: DataFrame
    quarantine: DataFrame
    exact_duplicates: DataFrame


def landing_schema(source):
    fields = [T.StructField(column, T.StringType()) for column in source.columns]
    return T.StructType(fields + [T.StructField(CORRUPT_RECORD, T.StringType())])


def landing_relative_path(file_path: Column, landing_dir: str) -> Column:
    local_path = F.regexp_replace(file_path, "^file:/+", "/")
    return F.replace(local_path, F.lit(landing_dir.rstrip("/") + "/"), F.lit("landing/"))


def _cast_errors(types):
    failures = [
        F.when(F.col(column).isNotNull() & F.col(column).try_cast(target).isNull(), F.lit(column))
        for column, target in types.items()
    ]
    return F.filter(F.array(*failures), lambda column: column.isNotNull())


def _rejection_reason(source):
    reason = F.when(F.col(CORRUPT_RECORD).isNotNull(), F.lit(MALFORMED_ROW))
    for column in source.required:
        reason = reason.when(F.col(column).isNull(), F.lit(f"missing:{column}"))
    return reason.when(
        F.size("_cast_errors") > 0, F.concat(F.lit("cast_error:"), F.array_join("_cast_errors", ","))
    )


def to_bronze(raw: DataFrame, source, ingest_ts) -> BronzeResult:
    if CORRUPT_RECORD not in raw.columns:
        raw = raw.withColumn(CORRUPT_RECORD, F.lit(None).cast("string"))

    is_valid = F.col("_reason").isNull()
    same_row = Window.partitionBy("raw").orderBy("source_file")
    same_key = Window.partitionBy(is_valid, source.key).orderBy("raw")

    tagged = (
        raw.withColumn("raw", F.coalesce(F.col(CORRUPT_RECORD), F.to_json(F.struct(*source.columns))))
        .withColumn("_cast_errors", _cast_errors(source.types))
        .withColumn("_reason", _rejection_reason(source))
        .withColumns({column: F.col(column).try_cast(target) for column, target in source.types.items()})
        .withColumn("ingest_ts", F.lit(ingest_ts).cast("timestamp"))
        .withColumn(INGEST_DATE, F.to_date("ingest_ts"))
        .withColumn(
            "_fate",
            F.when(~is_valid, F.col("_reason"))
            .when(F.row_number().over(same_row) > 1, F.lit(EXACT_DUPLICATE))
            .when(F.row_number().over(same_key) > 1, F.lit(DUPLICATE_KEY)),
        )
    )

    bronze_columns = [*source.columns, "source_file", "ingest_ts"]
    if source.partition_by == INGEST_DATE:
        bronze_columns.append(INGEST_DATE)

    return BronzeResult(
        bronze=tagged.filter(F.col("_fate").isNull()).select(*bronze_columns),
        quarantine=tagged.filter(F.col("_fate").isNotNull() & (F.col("_fate") != EXACT_DUPLICATE)).select(
            F.lit(source.name).alias("source"),
            F.col("_fate").alias("reason"),
            "raw",
            "source_file",
            "ingest_ts",
            INGEST_DATE,
        ),
        exact_duplicates=tagged.filter(F.col("_fate") == EXACT_DUPLICATE).select("raw", "source_file"),
    )
