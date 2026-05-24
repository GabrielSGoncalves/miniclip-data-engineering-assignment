import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, countDistinct, from_json, from_unixtime, to_date
from pyspark.sql.types import LongType, StringType, StructField, StructType

logging.basicConfig(level=logging.INFO)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "game-events-raw")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", None)  # optional: write results as CSV here

# Only init-event fields are needed for the daily-user aggregation
INIT_SCHEMA = StructType([
    StructField("event-type", StringType()),
    StructField("time", LongType()),
    StructField("user-id", StringType()),
    StructField("country", StringType()),
    StructField("platform", StringType()),
])


def build_aggregation(df: DataFrame) -> DataFrame:
    """
    Pure transformation: daily distinct users grouped by country and platform.
    Accepts any DataFrame shaped like INIT_SCHEMA — no Kafka dependency.
    """
    return (
        df.filter(col("event-type") == "init")
          .withColumn("date", to_date(from_unixtime(col("time"))))
          .groupBy("date", "country", "platform")
          .agg(countDistinct(col("user-id")).alias("distinct_users"))
          .orderBy("date", "country", "platform")
    )


def run(spark: SparkSession) -> DataFrame:
    raw = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Kafka delivers messages as binary; cast value to string then parse JSON
    parsed = (
        raw.select(from_json(col("value").cast("string"), INIT_SCHEMA).alias("d"))
           .select("d.*")
    )

    result = build_aggregation(parsed)
    result.show(100, truncate=False)

    if OUTPUT_PATH:
        result.coalesce(1).write.mode("overwrite").option("header", True).csv(OUTPUT_PATH)

    return result


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("DailyUserAggregator")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    run(spark)
