from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from src.bronze.sources import BILLING_MONTHLY, USERS
from src.bronze.transforms import DUPLICATE_KEY, landing_schema, to_bronze

INGEST_TS = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
USERS_FILE = "landing/users.csv"


def user(user_id="user_1", active="True", created_at="2025-07-10", last_login=None, email=None):
    return (user_id, "org_1", email or f"{user_id}@example.com", "developer", active, created_at, last_login, None)


@pytest.fixture
def users_df(spark):
    def build(*rows):
        return spark.createDataFrame(list(rows), landing_schema(USERS)).withColumn("source_file", F.lit(USERS_FILE))
    return build


def reasons(result):
    return sorted(row["reason"] for row in result.quarantine.collect())


def test_casts_types_and_adds_lineage(users_df):
    result = to_bronze(users_df(user(last_login="2025-08-01")), USERS, INGEST_TS)

    row = result.bronze.withColumn("ingest_ts", F.col("ingest_ts").cast("string")).collect()[0]
    assert row["active"] is True
    assert row["created_at"] == date(2025, 7, 10)
    assert row["last_login"] == date(2025, 8, 1)
    assert row["source_file"] == USERS_FILE
    assert row["ingest_ts"] == "2026-11-09 12:00:00"
    assert row["ingest_date"] == date(2026, 11, 9)


def test_missing_optional_value_stays_null_in_bronze(users_df):
    result = to_bronze(users_df(user(last_login=None)), USERS, INGEST_TS)

    assert result.bronze.collect()[0]["last_login"] is None
    assert result.quarantine.count() == 0


def test_uncastable_value_goes_to_quarantine_with_original_record(users_df):
    result = to_bronze(users_df(user(created_at="10/07/2025"), user("user_2")), USERS, INGEST_TS)

    assert [row["user_id"] for row in result.bronze.collect()] == ["user_2"]
    quarantined = result.quarantine.collect()[0]
    assert quarantined["reason"] == "cast_error:created_at"
    assert '"created_at":"10/07/2025"' in quarantined["raw"]
    assert quarantined["source"] == "users"


def test_missing_key_goes_to_quarantine(users_df):
    result = to_bronze(users_df(user(user_id=None, email="anon@example.com")), USERS, INGEST_TS)

    assert result.bronze.count() == 0
    assert reasons(result) == ["missing:user_id"]


def test_malformed_row_goes_to_quarantine(spark):
    row = (None,) * len(USERS.columns) + ("user_9,org_1,broken",)
    raw = spark.createDataFrame([row], landing_schema(USERS)).selectExpr("*", f"'{USERS_FILE}' AS source_file")

    assert reasons(to_bronze(raw, USERS, INGEST_TS)) == ["malformed_row"]


def test_exact_duplicates_are_dropped_not_quarantined(users_df):
    result = to_bronze(users_df(user(), user(), user("user_2")), USERS, INGEST_TS)

    assert sorted(row["user_id"] for row in result.bronze.collect()) == ["user_1", "user_2"]
    assert result.exact_duplicates.count() == 1
    assert result.quarantine.count() == 0


def test_conflicting_key_keeps_one_row_deterministically(users_df):
    first, second = user(active="True"), user(active="False")

    for rows in ((first, second), (second, first)):
        result = to_bronze(users_df(*rows), USERS, INGEST_TS)
        assert [row["active"] for row in result.bronze.collect()] == [False]
        assert reasons(result) == [DUPLICATE_KEY]


def test_invalid_copy_does_not_displace_valid_row_with_same_key(users_df):
    result = to_bronze(users_df(user(created_at="not-a-date"), user()), USERS, INGEST_TS)

    assert result.bronze.count() == 1
    assert reasons(result) == ["cast_error:created_at"]


def test_every_landing_row_is_accounted_for(users_df):
    rows = [user(), user(), user(active="False"), user("user_2", created_at="bad"), user(None, email="x"), user("user_3")]
    result = to_bronze(users_df(*rows), USERS, INGEST_TS)

    total = result.bronze.count() + result.quarantine.count() + result.exact_duplicates.count()
    assert total == len(rows)


def test_billing_amounts_are_decimal_and_month_is_required(spark):
    rows = [
        ("inv_1", "org_1", "2025-06-01", "1133.34", None, "238.0", "USD", "0.97684", None),
        ("inv_2", "org_1", None, "10.00", "1.00", "2.10", "ARS", "0.0016", None),
    ]
    raw = spark.createDataFrame(rows, landing_schema(BILLING_MONTHLY)).selectExpr(
        "*", "'landing/billing_monthly.csv' AS source_file"
    )
    result = to_bronze(raw, BILLING_MONTHLY, INGEST_TS)

    invoice = result.bronze.collect()[0]
    assert invoice["month"] == date(2025, 6, 1)
    assert invoice["subtotal"] == Decimal("1133.34")
    assert invoice["credits"] is None
    assert invoice["exchange_rate_to_usd"] == Decimal("0.976840")
    assert "ingest_date" not in result.bronze.columns
    assert reasons(result) == ["missing:month"]
