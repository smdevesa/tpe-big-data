from datetime import date, timedelta

import pytest

from src.serving.ranking import top_services

START = date(2025, 8, 1)


@pytest.fixture
def mart(spark):
    def build(*rows):
        return spark.createDataFrame(
            [(START + timedelta(days=day), org, service, float(cost)) for day, org, service, cost in rows],
            "usage_date date, org_id string, service string, daily_cost_usd double",
        )
    return build


def ranking_at(df, as_of, org="org_1"):
    rows = df.filter(f"org_id = '{org}' AND as_of_date = '{as_of.isoformat()}'").orderBy("rank").collect()
    return [(row["rank"], row["service"], row["cost_14d_usd"]) for row in rows]


def test_a_day_counts_for_the_fourteen_dates_that_include_it(mart):
    result = top_services(mart((0, "org_1", "compute", 5.0), (20, "org_1", "compute", 1.0)))

    as_of_dates = sorted(row["as_of_date"] for row in result.filter("cost_14d_usd = 5.0").collect())
    assert as_of_dates == [START + timedelta(days=offset) for offset in range(14)]


def test_window_sums_only_the_last_fourteen_days(mart):
    result = top_services(mart(
        (0, "org_1", "compute", 100.0),
        (1, "org_1", "compute", 10.0),
        (14, "org_1", "compute", 1.0),
    ))

    assert ranking_at(result, START + timedelta(days=14)) == [(1, "compute", 11.0)]


def test_services_are_ranked_by_cost_with_name_as_tiebreak(mart):
    result = top_services(mart(
        (0, "org_1", "storage", 5.0),
        (0, "org_1", "compute", 9.0),
        (0, "org_1", "analytics", 5.0),
    ))

    assert ranking_at(result, START) == [(1, "compute", 9.0), (2, "analytics", 5.0), (3, "storage", 5.0)]


def test_negative_costs_lower_the_accumulated_cost(mart):
    result = top_services(mart((0, "org_1", "compute", 10.0), (1, "org_1", "compute", -4.0)))

    assert ranking_at(result, START + timedelta(days=1)) == [(1, "compute", 6.0)]


def test_dates_after_the_latest_usage_are_not_published(mart):
    result = top_services(mart((0, "org_1", "compute", 1.0), (3, "org_1", "compute", 1.0)))

    assert max(row["as_of_date"] for row in result.collect()) == START + timedelta(days=3)


def test_each_organization_has_its_own_ranking(mart):
    result = top_services(mart((0, "org_1", "compute", 1.0), (0, "org_2", "genai", 7.0)))

    assert ranking_at(result, START, "org_1") == [(1, "compute", 1.0)]
    assert ranking_at(result, START, "org_2") == [(1, "genai", 7.0)]


def test_window_start_and_days_with_usage(mart):
    row = top_services(mart((0, "org_1", "compute", 1.0), (2, "org_1", "compute", 1.0))).filter(
        f"as_of_date = '{(START + timedelta(days=2)).isoformat()}'"
    ).first()

    assert row["window_start"] == START + timedelta(days=2 - 13)
    assert row["days_with_usage"] == 2
