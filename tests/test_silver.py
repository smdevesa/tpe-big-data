from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from src.silver.events import to_silver
from src.silver.masters import conform_resources, latest_snapshot

BRONZE_SCHEMA = (
    "event_id string, event_ts timestamp, event_date date, org_id string, resource_id string, "
    "service string, region string, metric string, value double, value_raw string, unit string, "
    "cost_usd_increment double, schema_version int, carbon_kg double, genai_tokens bigint, "
    "cast_errors array<string>, corrupt_record string, source_file string, ingest_ts timestamp"
)
INGEST_TS = datetime(2026, 11, 10, 12, 0)


def event(event_id="evt_1", ts="2025-08-17 10:00:00", org_id="org_1", resource_id="res_1", service="networking",
          metric="requests", value=10.0, unit="count", cost=1.0, schema_version=2, carbon_kg=0.5,
          genai_tokens=None, corrupt=None, ingest_ts=INGEST_TS, source_file="landing/a.jsonl"):
    event_ts = datetime.fromisoformat(ts) if ts else None
    return {
        "event_id": event_id, "event_ts": event_ts, "event_date": event_ts.date() if event_ts else None,
        "org_id": org_id, "resource_id": resource_id, "service": service, "region": "sa-east",
        "metric": metric, "value": value, "value_raw": None if value is None else str(value), "unit": unit,
        "cost_usd_increment": cost, "schema_version": schema_version, "carbon_kg": carbon_kg,
        "genai_tokens": genai_tokens, "cast_errors": [], "corrupt_record": corrupt,
        "source_file": source_file, "ingest_ts": ingest_ts,
    }


@pytest.fixture
def masters(spark):
    customers = spark.createDataFrame(
        [("org_1", "Nova Tech", "Media", "pro", True, "sa-east", date(2026, 11, 10))],
        "org_id string, org_name string, industry string, plan_tier string, is_enterprise boolean, "
        "hq_region string, ingest_date date",
    )
    resources = spark.createDataFrame(
        [("res_1", "org_1", "networking", "sa-east", date(2025, 8, 1), "running", '["env:prod"]', date(2026, 11, 10))],
        "resource_id string, org_id string, service string, region string, created_at date, state string, "
        "tags_json string, ingest_date date",
    )
    return customers, conform_resources(resources)


@pytest.fixture
def silver(spark, masters):
    def build(*events):
        bronze = spark.createDataFrame([tuple(e.values()) for e in events], BRONZE_SCHEMA)
        return to_silver(bronze, *masters)
    return build


def rejected_reasons(result):
    return sorted(row["reason"] for row in result.rejected.collect())


@pytest.mark.parametrize("bad_event, reason", [
    (event(event_id=None), "missing_event_id"),
    (event(ts=None), "invalid_timestamp"),
    (event(org_id=None), "missing_org_id"),
    (event(service=None), "missing_service"),
    (event(metric="gpu_hours", unit=None), "missing_unit"),
])
def test_each_rule_rejects_with_its_reason(silver, bad_event, reason):
    result = silver(bad_event, event("evt_ok"))

    assert [row["event_id"] for row in result.events.collect()] == ["evt_ok"]
    assert rejected_reasons(result) == [reason]
    assert result.rejected.first()["action"] == "rejected"


def test_corrupt_event_lists_every_failed_rule(silver):
    corrupt = event(event_id=None, ts=None, org_id=None, service=None, metric=None, value=None, unit=None,
                    cost=None, corrupt="{roto")
    row = silver(corrupt).rejected.first()

    assert row["reason"] == "missing_event_id"
    assert set(row["reasons"]) == {
        "missing_event_id", "malformed_event", "invalid_timestamp", "missing_org_id", "missing_service",
    }
    assert '"corrupt_record":"{roto"' in row["raw"]


def test_duplicate_event_id_keeps_earliest_ingested_copy(silver):
    first = event(value=1.0, ingest_ts=datetime(2026, 11, 10, 12, 0))
    later = event(value=2.0, ingest_ts=datetime(2026, 11, 10, 12, 5))
    result = silver(later, first)

    assert [row["value"] for row in result.events.collect()] == [1.0]
    assert rejected_reasons(result) == ["duplicate_event_id"]


def test_missing_unit_is_inferred_from_metric(silver):
    row = silver(event(metric="cpu_hours", unit=None, value=1.5)).events.first()

    assert row["unit"] == "hours"
    assert row["unit_inferred"] is True


def test_known_unit_is_not_marked_as_inferred(silver):
    assert silver(event()).events.first()["unit_inferred"] is False


@pytest.mark.parametrize("cost, anomaly", [(-0.02, True), (-0.01, False), (0.0, False), (317.43, False)])
def test_cost_anomaly_threshold(silver, cost, anomaly):
    result = silver(event(cost=cost))

    row = result.events.first()
    assert row["cost_anomaly"] is anomaly
    assert row["cost_usd_increment"] == cost
    assert result.flagged.count() == int(anomaly)


def test_anomalous_cost_stays_in_silver_and_is_copied_as_flagged(silver):
    result = silver(event(cost=-154.46))

    assert result.events.count() == 1
    assert result.rejected.count() == 0
    flagged = result.flagged.first()
    assert (flagged["action"], flagged["reason"]) == ("flagged", "cost_below_threshold")


def test_missing_value_is_kept_without_imputing_zero(silver):
    row = silver(event(value=None, cost=0.9561)).events.first()

    assert row["value_missing"] is True
    assert row["value"] is None
    assert row["requests"] is None
    assert row["cost_usd_increment"] == 0.9561


def test_metric_features_only_where_the_metric_applies(silver):
    rows = {row["metric"]: row for row in silver(
        event("a", metric="requests", value=131.0),
        event("b", metric="cpu_hours", value=1.3656, unit="hours"),
        event("c", metric="storage_gb_hours", value=10.5558, unit="gb_hours"),
    ).events.collect()}

    assert (rows["requests"]["requests"], rows["requests"]["cpu_hours"]) == (131.0, None)
    assert (rows["cpu_hours"]["cpu_hours"], rows["cpu_hours"]["requests"]) == (1.3656, None)
    assert rows["storage_gb_hours"]["storage_gb_hours"] == 10.5558


def test_v1_events_keep_v2_features_null(silver):
    row = silver(event(schema_version=1, carbon_kg=None, genai_tokens=None)).events.first()

    assert row["carbon_kg"] is None
    assert row["genai_tokens"] is None


def test_event_before_resource_creation_is_flagged_not_rejected(silver):
    result = silver(event("before", ts="2025-07-15 10:00:00"), event("after", ts="2025-08-02 10:00:00"))

    flags = {row["event_id"]: row["before_resource_created"] for row in result.events.collect()}
    assert flags == {"before": True, "after": False}
    assert result.rejected.count() == 0


@pytest.mark.parametrize("version, unknown", [(1, False), (2, False), (3, True), (None, True)])
def test_unknown_schema_version_is_flagged_and_kept(silver, version, unknown):
    result = silver(event(schema_version=version))

    assert result.events.first()["unknown_schema_version"] is unknown
    assert result.rejected.count() == 0


def test_events_are_enriched_with_masters(silver):
    row = silver(event()).events.first()

    assert (row["org_name"], row["plan_tier"], row["is_enterprise"]) == ("Nova Tech", "pro", True)
    assert (row["resource_state"], row["resource_created_at"]) == ("running", date(2025, 8, 1))


def test_unknown_org_is_kept_without_enrichment(silver):
    row = silver(event(org_id="org_unknown")).events.first()

    assert row["org_id"] == "org_unknown"
    assert row["org_name"] is None


def test_latest_snapshot_takes_last_ingest_date(spark):
    snapshots = spark.createDataFrame(
        [("org_1", "standard", date(2026, 11, 9)), ("org_1", "pro", date(2026, 11, 10))],
        "org_id string, plan_tier string, ingest_date date",
    )

    assert [row["plan_tier"] for row in latest_snapshot(snapshots).collect()] == ["pro"]


def test_resource_tags_are_parsed(masters):
    _, resources = masters
    assert resources.first()["tags"] == ["env:prod"]


def test_e1_example_keeps_all_six_events(silver):
    rows = [
        ("requests", None, "count", 0.9561),
        ("cpu_hours", 1.3656, "hours", 0.0134),
        ("storage_gb_hours", 10.5558, "gb_hours", 0.1253),
        ("requests", 131.0, "count", 1.2812),
        ("requests", 110.0, "count", 1.1302),
        ("requests", 135.0, "count", 1.3298),
    ]
    events = [
        event(f"evt_{i}", org_id="org_1", metric=metric, value=value, unit=unit, cost=cost)
        for i, (metric, value, unit, cost) in enumerate(rows)
    ]
    result = silver(*events)

    totals = result.events.agg(
        F.count("*").alias("events"),
        F.round(F.sum("cost_usd_increment"), 4).alias("cost"),
        F.sum("requests").alias("requests"),
        F.count_if("value_missing").alias("value_missing"),
    ).first()
    assert (totals["events"], totals["cost"], totals["requests"], totals["value_missing"]) == (6, 4.836, 376.0, 1)
    assert result.rejected.count() == 0
