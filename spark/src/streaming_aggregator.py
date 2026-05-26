import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    from_json,
    lit,
    row_number,
    sum as sum_,
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(level=logging.INFO)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CLEAN_TOPIC = os.getenv("KAFKA_TOPIC", "game-events-clean")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/tmp/streaming_checkpoint")

WIDE_SCHEMA = StructType([
    StructField("event-type",     StringType(),  True),
    StructField("time",           LongType(),    True),
    StructField("user-id",        StringType(),  True),
    StructField("country",        StringType(),  True),
    StructField("platform",       StringType(),  True),
    StructField("user-a",         StringType(),  True),
    StructField("user-b",         StringType(),  True),
    StructField("winner",         StringType(),  True),
    StructField("game-tier",      LongType(),    True),
    StructField("duration",       LongType(),    True),
    StructField("purchase_value", DoubleType(),  True),
    StructField("product-id",     StringType(),  True),
])

_CACHE_VIEW = "user_country_cache"
_CACHE_SCHEMA = StructType([
    StructField("user-id",  StringType(), True),
    StructField("country",  StringType(), True),
])


def update_user_country_cache(spark: SparkSession, batch_df: DataFrame) -> None:
    """Accumulate user-id → country mappings from init events into a temp view."""
    new_mappings = (
        batch_df
        .filter(col("event-type") == "init")
        .select(col("user-id"), col("country"))
        .dropna(subset=["user-id", "country"])
    )

    if spark.catalog.tableExists(_CACHE_VIEW):
        existing = spark.table(_CACHE_VIEW)
        # Row-number window: within each user-id, source=1 (new) ranks above source=2 (existing)
        # so the latest-seen country deterministically wins regardless of partition layout
        w = Window.partitionBy("user-id").orderBy("source")
        merged = (
            new_mappings.withColumn("source", lit(1))
            .union(existing.withColumn("source", lit(2)))
            .withColumn("rn", row_number().over(w))
            .filter(col("rn") == 1)
            .drop("source", "rn")
        )
    else:
        merged = new_mappings

    merged.createOrReplaceTempView(_CACHE_VIEW)


def compute_global_metrics(spark: SparkSession, batch_df: DataFrame) -> DataFrame:
    """
    Returns a single-row DataFrame:
      purchase_count (long), total_revenue (double), distinct_users (long).
    distinct_users counts across all event types (init, in-app-purchase, match user-a).
    """
    iap = batch_df.filter(col("event-type") == "in-app-purchase")

    if iap.isEmpty():
        purchase_metrics = spark.createDataFrame(
            [(0, 0.0)], ["purchase_count", "total_revenue"]
        )
    else:
        purchase_metrics = iap.agg(
            count("*").alias("purchase_count"),
            sum_("purchase_value").alias("total_revenue"),
        )

    all_users = (
        batch_df
        .filter(col("event-type").isin("init", "in-app-purchase"))
        .select(col("user-id").alias("uid"))
        .union(
            batch_df
            .filter(col("event-type") == "match")
            .select(col("user-a").alias("uid"))
        )
        .dropna(subset=["uid"])
    )
    distinct_users = all_users.select(countDistinct("uid").alias("distinct_users"))

    return purchase_metrics.crossJoin(distinct_users)


def compute_revenue_by_country(batch_df: DataFrame, cache_df: DataFrame) -> DataFrame:
    """Sum of purchase_value grouped by country (enriched from init cache)."""
    purchases = (
        batch_df
        .filter(col("event-type") == "in-app-purchase")
        .select(col("user-id"), col("purchase_value"))
    )
    return (
        purchases
        .join(cache_df, on="user-id", how="left")
        .fillna("Unknown", subset=["country"])
        .groupBy("country")
        .agg(sum_("purchase_value").alias("revenue"))
        .orderBy("country")
    )


def compute_match_count_by_country(batch_df: DataFrame, cache_df: DataFrame) -> DataFrame:
    """Count of match events grouped by country of user-a (enriched from init cache)."""
    matches = (
        batch_df
        .filter(col("event-type") == "match")
        .select(col("user-a").alias("user-id"))
    )
    return (
        matches
        .join(cache_df, on="user-id", how="left")
        .fillna("Unknown", subset=["country"])
        .groupBy("country")
        .agg(count("*").alias("match_count"))
        .orderBy("country")
    )


def process_batch(spark: SparkSession, batch_df: DataFrame, batch_id: int) -> None:
    update_user_country_cache(spark, batch_df)
    cache = (
        spark.table(_CACHE_VIEW)
        if spark.catalog.tableExists(_CACHE_VIEW)
        else spark.createDataFrame([], _CACHE_SCHEMA)
    )

    print(f"\n{'='*60}")
    print(f"  Batch {batch_id} — per-minute aggregations")
    print(f"{'='*60}")

    print("\n>> Global metrics (purchases / revenue / distinct users):")
    compute_global_metrics(spark, batch_df).show(truncate=False)

    print(">> Revenue by country:")
    compute_revenue_by_country(batch_df, cache).show(truncate=False)

    print(">> Matches by country:")
    compute_match_count_by_country(batch_df, cache).show(truncate=False)


def run(spark: SparkSession) -> None:
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", CLEAN_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        raw.select(from_json(col("value").cast("string"), WIDE_SCHEMA).alias("d"))
           .select("d.*")
    )

    query = (
        parsed.writeStream
        .foreachBatch(lambda df, bid: process_batch(spark, df, bid))
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="1 minute")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("PerMinuteStreamingAggregator")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    run(spark)
