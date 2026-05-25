import pytest
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from streaming_aggregator import (
    compute_global_metrics,
    compute_match_count_by_country,
    compute_revenue_by_country,
    update_user_country_cache,
)

_CACHE_VIEW = "user_country_cache"

# Minimal schema that covers fields used by compute_global_metrics
_GLOBAL_METRICS_SCHEMA = StructType([
    StructField("event-type",     StringType(), True),
    StructField("user-id",        StringType(), True),
    StructField("user-a",         StringType(), True),
    StructField("purchase_value", DoubleType(), True),
])


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test-streaming-aggregator")
        .getOrCreate()
    )


@pytest.fixture(autouse=True)
def drop_cache(spark):
    """Ensure the cache temp view does not bleed between tests."""
    yield
    if spark.catalog.tableExists(_CACHE_VIEW):
        spark.catalog.dropTempView(_CACHE_VIEW)


# ---------------------------------------------------------------------------
# compute_global_metrics
# ---------------------------------------------------------------------------

def test_purchase_count(spark):
    data = [
        ("in-app-purchase", "user_1", None, 9.99),
        ("in-app-purchase", "user_2", None, 4.99),
        ("init",            "user_3", None, None),
    ]
    df = spark.createDataFrame(data, _GLOBAL_METRICS_SCHEMA)
    row = compute_global_metrics(spark, df).collect()[0]
    assert row["purchase_count"] == 2


def test_revenue_sum(spark):
    data = [
        ("in-app-purchase", "user_1", None, 9.99),
        ("in-app-purchase", "user_2", None, 0.01),
    ]
    df = spark.createDataFrame(data, _GLOBAL_METRICS_SCHEMA)
    row = compute_global_metrics(spark, df).collect()[0]
    assert abs(row["total_revenue"] - 10.00) < 0.001


def test_distinct_users_across_event_types(spark):
    # user_1 appears in init and in-app-purchase; user_2 is a match user-a
    iap_data = [
        ("in-app-purchase", "user_1", "user_1", 5.0),
        ("init",            "user_1", "user_1", None),
        ("match",           None,     "user_2", None),
    ]
    df = spark.createDataFrame(iap_data, ["event-type", "user-id", "user-a", "purchase_value"])
    row = compute_global_metrics(spark, df).collect()[0]
    assert row["distinct_users"] == 2


def test_empty_purchases_returns_zero_row(spark):
    df = spark.createDataFrame([("init", "user_1", None, None)], _GLOBAL_METRICS_SCHEMA)
    rows = compute_global_metrics(spark, df).collect()
    assert len(rows) == 1
    assert rows[0]["purchase_count"] == 0
    assert rows[0]["total_revenue"] == 0.0


# ---------------------------------------------------------------------------
# compute_revenue_by_country
# ---------------------------------------------------------------------------

def _cache_df(spark, rows):
    return spark.createDataFrame(rows, ["user-id", "country"])


def test_revenue_by_country_correct_values(spark):
    iap = spark.createDataFrame(
        [("in-app-purchase", "user_1", 9.99), ("in-app-purchase", "user_2", 4.99)],
        ["event-type", "user-id", "purchase_value"],
    )
    cache = _cache_df(spark, [("user_1", "Portugal"), ("user_2", "Brazil")])
    result = {r["country"]: r["revenue"] for r in compute_revenue_by_country(iap, cache).collect()}
    assert abs(result["Portugal"] - 9.99) < 0.001
    assert abs(result["Brazil"]   - 4.99) < 0.001


def test_revenue_by_country_aggregates_same_country(spark):
    iap = spark.createDataFrame(
        [("in-app-purchase", "user_1", 5.0), ("in-app-purchase", "user_2", 3.0)],
        ["event-type", "user-id", "purchase_value"],
    )
    cache = _cache_df(spark, [("user_1", "Portugal"), ("user_2", "Portugal")])
    result = compute_revenue_by_country(iap, cache).collect()
    assert len(result) == 1
    assert abs(result[0]["revenue"] - 8.0) < 0.001


def test_revenue_by_country_unknown_for_missing_user(spark):
    iap = spark.createDataFrame(
        [("in-app-purchase", "user_unknown", 3.50)],
        ["event-type", "user-id", "purchase_value"],
    )
    cache = _cache_df(spark, [("user_1", "Portugal")])
    result = {r["country"]: r["revenue"] for r in compute_revenue_by_country(iap, cache).collect()}
    assert "Unknown" in result
    assert abs(result["Unknown"] - 3.50) < 0.001


def test_revenue_by_country_excludes_non_purchase_events(spark):
    data = [
        ("in-app-purchase", "user_1", 5.0),
        ("init",            "user_1", None),
    ]
    df = spark.createDataFrame(data, ["event-type", "user-id", "purchase_value"])
    cache = _cache_df(spark, [("user_1", "Portugal")])
    result = compute_revenue_by_country(df, cache).collect()
    assert len(result) == 1
    assert abs(result[0]["revenue"] - 5.0) < 0.001


# ---------------------------------------------------------------------------
# compute_match_count_by_country
# ---------------------------------------------------------------------------

def test_match_count_by_country_uses_user_a(spark):
    matches = spark.createDataFrame(
        [("match", "user_1", "user_2"), ("match", "user_3", "user_1")],
        ["event-type", "user-a", "user-b"],
    )
    cache = _cache_df(spark, [("user_1", "Portugal"), ("user_3", "Brazil")])
    result = {r["country"]: r["match_count"] for r in compute_match_count_by_country(matches, cache).collect()}
    assert result["Portugal"] == 1
    assert result["Brazil"]   == 1


def test_match_count_by_country_unknown_for_missing_user(spark):
    matches = spark.createDataFrame(
        [("match", "user_unknown", "user_2")],
        ["event-type", "user-a", "user-b"],
    )
    cache = _cache_df(spark, [("user_1", "Portugal")])
    result = {r["country"]: r["match_count"] for r in compute_match_count_by_country(matches, cache).collect()}
    assert result.get("Unknown", 0) == 1


def test_match_count_excludes_non_match_events(spark):
    data = [
        ("match",           "user_1", "user_2"),
        ("in-app-purchase", "user_1", None),
    ]
    df = spark.createDataFrame(data, ["event-type", "user-a", "user-b"])
    cache = _cache_df(spark, [("user_1", "Portugal")])
    result = compute_match_count_by_country(df, cache).collect()
    assert sum(r["match_count"] for r in result) == 1


# ---------------------------------------------------------------------------
# update_user_country_cache
# ---------------------------------------------------------------------------

def test_cache_creates_view_from_init_events(spark):
    data = [("init", "user_1", "Portugal"), ("init", "user_2", "Brazil")]
    df = spark.createDataFrame(data, ["event-type", "user-id", "country"])
    update_user_country_cache(spark, df)
    assert spark.catalog.tableExists(_CACHE_VIEW)
    rows = {r["user-id"]: r["country"] for r in spark.table(_CACHE_VIEW).collect()}
    assert rows["user_1"] == "Portugal"
    assert rows["user_2"] == "Brazil"


def test_cache_ignores_non_init_events(spark):
    data = [
        ("in-app-purchase", "user_1", None),
        ("init",            "user_2", "Germany"),
    ]
    df = spark.createDataFrame(data, ["event-type", "user-id", "country"])
    update_user_country_cache(spark, df)
    rows = spark.table(_CACHE_VIEW).collect()
    assert len(rows) == 1
    assert rows[0]["user-id"] == "user_2"


def test_cache_accumulates_across_calls(spark):
    batch1 = spark.createDataFrame(
        [("init", "user_1", "Portugal")], ["event-type", "user-id", "country"]
    )
    batch2 = spark.createDataFrame(
        [("init", "user_2", "Spain")], ["event-type", "user-id", "country"]
    )
    update_user_country_cache(spark, batch1)
    update_user_country_cache(spark, batch2)
    rows = {r["user-id"]: r["country"] for r in spark.table(_CACHE_VIEW).collect()}
    assert rows["user_1"] == "Portugal"
    assert rows["user_2"] == "Spain"


def test_cache_deduplicates_latest_country_wins(spark):
    batch1 = spark.createDataFrame(
        [("init", "user_1", "Portugal")], ["event-type", "user-id", "country"]
    )
    batch2 = spark.createDataFrame(
        [("init", "user_1", "Germany")], ["event-type", "user-id", "country"]
    )
    update_user_country_cache(spark, batch1)
    update_user_country_cache(spark, batch2)
    rows = spark.table(_CACHE_VIEW).collect()
    assert len(rows) == 1
    assert rows[0]["country"] == "Germany"
