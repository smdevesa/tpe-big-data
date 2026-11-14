import json
import os
from datetime import date

import pytest
from pyspark.sql import functions as F

from src.bronze.events import FIELDS, count_late_arrivals, landing_schema, parse_events
from src.stages.bronze_stream import run_stream


def event(event_id="evt_1", timestamp="2025-08-01T12:00:00Z", value="10", schema_version="2", **fields):
    base = {"event_id": event_id, "timestamp": timestamp, "metric": "requests", "value": value,
            "cost_usd_increment": "0.5", "schema_version": schema_version}
    return {**base, **fields}


@pytest.fixture
def events_df(spark):
    def build(*events, corrupt=None):
        rows = [tuple(e.get(field) for field in FIELDS) + (None,) for e in events]
        if corrupt:
            rows.append((None,) * len(FIELDS) + (corrupt,))
        return spark.createDataFrame(rows, landing_schema()).withColumn("source_file", F.lit("landing/x.jsonl"))
    return build


def by_id(df):
    return {row["event_id"]: row for row in df.collect()}


def test_value_is_cast_with_original_text_kept(events_df):
    rows = by_id(parse_events(events_df(event("a", value="118.0"), event("b", value="7.5"), event("c", value="abc"))))

    assert rows["a"]["value"] == 118.0
    assert rows["b"]["value_raw"] == "7.5"
    assert rows["c"]["value"] is None
    assert rows["c"]["value_raw"] == "abc"
    assert rows["c"]["cast_errors"] == ["value"]
    assert rows["a"]["cast_errors"] == []


def test_missing_value_is_not_a_cast_error(events_df):
    row = parse_events(events_df(event(value=None))).first()

    assert row["value"] is None
    assert row["cast_errors"] == []


def test_invalid_timestamp_keeps_event_without_date(events_df):
    row = parse_events(events_df(event(timestamp="no-es-fecha"))).first()

    assert row["event_ts"] is None
    assert row["event_date"] is None
    assert row["cast_errors"] == ["timestamp"]


def test_event_date_comes_from_utc_timestamp(events_df):
    row = parse_events(events_df(event(timestamp="2025-08-01T23:30:00Z"))).first()

    assert row["event_date"] == date(2025, 8, 1)


def test_v1_fields_absent_stay_null_not_zero(events_df):
    rows = by_id(parse_events(events_df(
        event("v1", schema_version="1"),
        event("v2", schema_version="2", carbon_kg="0", genai_tokens="1500"),
    )))

    assert rows["v1"]["carbon_kg"] is None
    assert rows["v1"]["genai_tokens"] is None
    assert rows["v2"]["carbon_kg"] == 0.0
    assert rows["v2"]["genai_tokens"] == 1500


def test_late_arrivals_are_counted_against_previous_micro_batches(spark):
    rows = [
        ("2026-11-09 10:00:00", "2025-08-01 10:00:00"),
        ("2026-11-09 10:00:00", "2025-08-01 12:00:00"),
        ("2026-11-09 10:01:00", "2025-08-01 11:00:00"),
        ("2026-11-09 10:01:00", "2025-08-01 13:00:00"),
        ("2026-11-09 10:02:00", "2025-08-01 09:00:00"),
    ]
    events = spark.createDataFrame(rows, "ingest_ts string, event_ts string").select(
        F.to_timestamp("ingest_ts").alias("ingest_ts"), F.to_timestamp("event_ts").alias("event_ts")
    )

    late = count_late_arrivals(events)

    assert late.rows == 2
    assert late.max_lateness_s == 4 * 3600


def write_jsonl(directory, name, events, order, extra_lines=()):
    path = directory / name
    lines = [json.dumps(e) for e in events] + list(extra_lines)
    path.write_text("\n".join(lines) + "\n")
    os.utime(path, (order * 1_000, order * 1_000))


@pytest.fixture
def landing(tmp_path):
    directory = tmp_path / "landing"
    directory.mkdir()
    return directory


@pytest.fixture
def three_micro_batches(landing):
    write_jsonl(landing, "part_0.jsonl", [event("on_time", "2025-08-01T12:00:00Z")], order=1)
    write_jsonl(landing, "part_1.jsonl", [
        event("second", "2025-08-01T12:10:00Z"),
        event("on_time", "2025-08-01T12:00:00Z"),
    ], order=2)
    write_jsonl(landing, "part_2.jsonl", [
        event("too_late", "2025-08-01T09:00:00Z"),
        event("within_tolerance", "2025-08-01T11:30:00Z"),
    ], order=3)
    return landing


def stream(spark, tmp_path, source, watermark):
    return run_stream(spark, source, tmp_path / "bronze", tmp_path / "checkpoint", watermark, max_files_per_trigger=1)


def written(spark, tmp_path):
    return spark.read.parquet(str(tmp_path / "bronze"))


def written_ids(spark, tmp_path):
    return sorted(row["event_id"] for row in written(spark, tmp_path).collect())


def test_watermark_drops_and_counts_events_older_than_tolerance(spark, tmp_path, three_micro_batches):
    run = stream(spark, tmp_path, three_micro_batches, "1 hour")

    assert run.micro_batches == 3
    assert run.input_rows == 5
    assert run.dropped_by_watermark == 1
    assert run.duplicates_dropped == 1
    assert run.written_rows == 3
    assert written_ids(spark, tmp_path) == ["on_time", "second", "within_tolerance"]


def test_wide_watermark_keeps_late_events(spark, tmp_path, three_micro_batches):
    run = stream(spark, tmp_path, three_micro_batches, "1 day")

    assert run.dropped_by_watermark == 0
    assert written_ids(spark, tmp_path) == ["on_time", "second", "too_late", "within_tolerance"]


def test_rerun_with_checkpoint_reads_nothing_new(spark, tmp_path, three_micro_batches):
    stream(spark, tmp_path, three_micro_batches, "1 day")

    rerun = stream(spark, tmp_path, three_micro_batches, "1 day")

    assert rerun.input_rows == 0
    assert written(spark, tmp_path).count() == 4


def test_repeated_event_id_is_kept_once_but_events_without_id_are_not_collapsed(spark, tmp_path, landing):
    write_jsonl(landing, "part_0.jsonl", [
        event("a"), event("a"), event(None, value="1"), event(None, value="2"),
    ], order=1, extra_lines=["{roto"])

    run = stream(spark, tmp_path, landing, "1 day")

    events = written(spark, tmp_path)
    assert run.duplicates_dropped == 1
    assert events.count() == 4
    assert events.filter(F.col("event_id") == "a").count() == 1
    assert events.filter(F.col("corrupt_record") == "{roto").count() == 1
