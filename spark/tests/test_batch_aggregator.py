import pytest

from batch_aggregator import build_aggregation


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test-batch-aggregator")
        .getOrCreate()
    )


def test_daily_distinct_users(spark):
    data = [
        ("init", 1_700_000_000, "user_1", "PT", "ios"),
        ("init", 1_700_000_060, "user_2", "PT", "ios"),
        ("init", 1_700_000_120, "user_1", "PT", "ios"),  # user_1 again — still 1 distinct
    ]
    df = spark.createDataFrame(data, ["event-type", "time", "user-id", "country", "platform"])
    result = build_aggregation(df).collect()

    assert len(result) == 1
    assert result[0]["distinct_users"] == 2


def test_non_init_events_excluded(spark):
    data = [
        ("init",            1_700_000_000, "user_1", "PT", "ios"),
        ("in-app-purchase", 1_700_000_001, "user_2", "PT", "ios"),  # must not be counted
    ]
    df = spark.createDataFrame(data, ["event-type", "time", "user-id", "country", "platform"])
    result = build_aggregation(df).collect()

    assert result[0]["distinct_users"] == 1  # only user_1


def test_groups_by_country_and_platform(spark):
    data = [
        ("init", 1_700_000_000, "user_1", "PT", "ios"),
        ("init", 1_700_000_000, "user_2", "US", "android"),
        ("init", 1_700_000_000, "user_3", "PT", "android"),
    ]
    df = spark.createDataFrame(data, ["event-type", "time", "user-id", "country", "platform"])
    rows = {
        (r["country"], r["platform"]): r["distinct_users"]
        for r in build_aggregation(df).collect()
    }

    assert rows[("PT", "ios")] == 1
    assert rows[("US", "android")] == 1
    assert rows[("PT", "android")] == 1


def test_cross_day_grouping(spark):
    # Same user on two different days should produce two separate rows, not be deduplicated
    data = [
        ("init", 1_700_000_000, "user_1", "PT", "ios"),  # 2023-11-14
        ("init", 1_700_086_400, "user_1", "PT", "ios"),  # 2023-11-15 (+86400s = +1 day)
    ]
    df = spark.createDataFrame(data, ["event-type", "time", "user-id", "country", "platform"])
    result = build_aggregation(df).collect()

    assert len(result) == 2
    assert all(r["distinct_users"] == 1 for r in result)
