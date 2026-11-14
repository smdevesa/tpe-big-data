import copy

from src.idempotency import compare

STATE = {
    "bronze.users": {"rows": 800},
    "silver.usage_events": {"rows": 43200, "rows_with_event_id": 43200, "distinct_event_id": 43200, "cost_usd": 147433.98},
    "gold.org_daily_usage_by_service": {"rows": 11050, "distinct_keys": 11050, "cost_usd": 147433.98},
    "cassandra.top_services_14d": {"rows": 15599},
}


def test_identical_runs_are_idempotent():
    result = compare(STATE, copy.deepcopy(STATE), second_run_stream_rows=0)

    assert result["result"] == "ok"
    assert result["differences"] == []


def test_different_counts_are_reported():
    second = copy.deepcopy(STATE)
    second["cassandra.top_services_14d"]["rows"] = 31198

    result = compare(STATE, second, second_run_stream_rows=0)

    assert result["result"] == "failed"
    assert result["differences"] == [
        {"zone": "cassandra.top_services_14d", "measure": "rows", "first": 15599, "second": 31198}
    ]


def test_duplicate_keys_fail_even_if_both_runs_match():
    duplicated = copy.deepcopy(STATE)
    duplicated["silver.usage_events"]["rows_with_event_id"] = 43201

    result = compare(duplicated, copy.deepcopy(duplicated), second_run_stream_rows=0)

    assert result["checks"]["same_measures"] is True
    assert result["checks"]["unique_keys"] is False
    assert result["result"] == "failed"


def test_second_run_reading_the_stream_again_fails():
    result = compare(STATE, copy.deepcopy(STATE), second_run_stream_rows=43200)

    assert result["checks"]["stream_read_nothing_new"] is False
    assert result["result"] == "failed"
