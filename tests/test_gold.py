from datetime import date, datetime, timedelta

import pytest

from src.gold.usage import build_mart, daily_usage, flag_cost_anomalies

SILVER_SCHEMA = (
    "event_date date, org_id string, org_name string, plan_tier string, service string, "
    "cost_usd_increment double, requests double, cpu_hours double, storage_gb_hours double, "
    "genai_tokens bigint, carbon_kg double, value_missing boolean, cost_anomaly boolean"
)
DAY = date(2025, 8, 17)


def event(day=DAY, org_id="org_1", service="networking", cost=1.0, requests=None, cpu_hours=None,
          storage_gb_hours=None, genai_tokens=None, carbon_kg=None, value_missing=False):
    return (day, org_id, "Nova Tech", "pro", service, cost, requests, cpu_hours, storage_gb_hours,
            genai_tokens, carbon_kg, value_missing, cost < -0.01)


@pytest.fixture
def silver(spark):
    return lambda *events: spark.createDataFrame(list(events), SILVER_SCHEMA)


def rows_by(df, *keys):
    return {tuple(row[key] for key in keys): row for row in df.collect()}


def test_e1_example_aggregates_to_the_reported_totals(silver):
    events = silver(
        event(cost=0.9561, value_missing=True),
        event(cost=0.0134, cpu_hours=1.3656),
        event(cost=0.1253, storage_gb_hours=10.5558),
        event(cost=1.2812, requests=131.0),
        event(cost=1.1302, requests=110.0),
        event(cost=1.3298, requests=135.0),
    )

    row = daily_usage(events).first()

    assert row["daily_cost_usd"] == 4.836
    assert row["requests"] == 376.0
    assert row["events"] == 6
    assert row["events_missing_value"] == 1


def test_each_org_service_and_day_is_its_own_row(silver):
    events = silver(
        event(cost=1.0), event(cost=2.0),
        event(service="compute", cost=5.0),
        event(day=DAY + timedelta(days=1), cost=7.0),
    )

    rows = rows_by(daily_usage(events), "usage_date", "service")

    assert len(rows) == 3
    assert rows[(DAY, "networking")]["daily_cost_usd"] == 3.0
    assert rows[(DAY, "compute")]["daily_cost_usd"] == 5.0
    assert rows[(DAY + timedelta(days=1), "networking")]["daily_cost_usd"] == 7.0


def test_metrics_without_data_stay_null_not_zero(silver):
    row = daily_usage(silver(event(requests=10.0), event(requests=5.0))).first()

    assert row["carbon_kg"] is None
    assert row["genai_tokens"] is None
    assert row["cpu_hours"] is None


def test_reported_zero_is_kept_as_zero(silver):
    row = daily_usage(silver(event(carbon_kg=0.0), event(carbon_kg=None))).first()

    assert row["carbon_kg"] == 0.0


def test_negative_cost_events_are_counted_and_summed(silver):
    row = daily_usage(silver(event(cost=-3.5), event(cost=-0.005), event(cost=2.0))).first()

    assert row["negative_cost_events"] == 1
    assert row["daily_cost_usd"] == -1.505


def series(silver, costs, service="compute"):
    start = date(2025, 7, 1)
    return daily_usage(silver(*[event(day=start + timedelta(days=i), service=service, cost=float(c)) for i, c in enumerate(costs)]))


def flagged_costs(df):
    return sorted(row["daily_cost_usd"] for row in df.collect() if row["is_cost_anomaly"])


def test_high_spike_is_flagged_and_normal_days_are_not(silver):
    result = flag_cost_anomalies(series(silver, [10, 11, 9, 10, 12, 10, 11, 9, 10, 95]))

    assert flagged_costs(result) == [95.0]
    spike = next(row for row in result.collect() if row["daily_cost_usd"] == 95.0)
    assert spike["cost_median_usd"] == 10.0
    assert spike["cost_mad_usd"] == 1.0
    assert spike["cost_robust_z"] == pytest.approx(0.6745 * 85, abs=1e-3)


def test_negative_day_is_flagged_as_low_anomaly(silver):
    result = flag_cost_anomalies(series(silver, [10, 11, 9, 10, 12, 10, 11, 9, 10, -40]))

    assert flagged_costs(result) == [-40.0]


def test_series_are_scored_independently(silver):
    spiky = series(silver, [10, 11, 9, 10, 12, 10, 11, 9, 10, 95], service="compute")
    expensive = series(silver, [95, 96, 94, 95, 97, 95, 96, 94, 95, 96], service="genai")

    assert flagged_costs(flag_cost_anomalies(spiky.unionByName(expensive))) == [95.0]


def test_series_without_dispersion_has_no_score(silver):
    result = flag_cost_anomalies(series(silver, [10] * 9 + [95]))

    assert {row["cost_robust_z"] for row in result.collect()} == {None}
    assert flagged_costs(result) == []


def test_short_series_has_no_score(silver):
    result = flag_cost_anomalies(series(silver, [10, 11, 9, 10, 95]))

    assert {row["cost_robust_z"] for row in result.collect()} == {None}


def test_mart_keeps_grain_and_processing_time(silver):
    processed_at = datetime(2026, 11, 10, 12, 0)
    mart = build_mart(silver(event(), event(service="compute")), processed_at)

    assert mart.groupBy("usage_date", "org_id", "service").count().filter("count > 1").count() == 0
    assert {row["processed_at"] for row in mart.collect()} == {processed_at}
