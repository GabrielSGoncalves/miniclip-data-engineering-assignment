import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime, to_timestamp
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOTSTRAP        = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CLEAN_TOPIC      = os.getenv("KAFKA_TOPIC",             "game-events-clean")
CHECKPOINT_PATH  = os.getenv("CHECKPOINT_PATH",         "/tmp/iceberg_checkpoint")
CATALOG_URI      = os.getenv("ICEBERG_CATALOG_URI",     "http://iceberg-rest:8181")
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",          "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",        "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",        "minioadmin")

ICEBERG_TABLE = "iceberg.db.game_events_clean"

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


def transform_for_iceberg(df: DataFrame) -> DataFrame:
    """
    Project and rename all columns in one select to match the Iceberg table schema.
    event_time is placed last so Iceberg field-ID order matches DataFrame column order.
    """
    return df.select(
        col("event-type").alias("event_type"),
        col("user-id").alias("user_id"),
        col("country"),
        col("platform"),
        col("user-a").alias("user_a"),
        col("user-b").alias("user_b"),
        col("winner"),
        col("game-tier").alias("game_tier"),
        col("duration"),
        col("purchase_value"),
        col("product-id").alias("product_id"),
        to_timestamp(from_unixtime(col("time"))).alias("event_time"),
    )


def ensure_table_exists(spark: SparkSession) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.db")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE} (
            event_type     STRING,
            user_id        STRING,
            country        STRING,
            platform       STRING,
            user_a         STRING,
            user_b         STRING,
            winner         STRING,
            game_tier      BIGINT,
            duration       BIGINT,
            purchase_value DOUBLE,
            product_id     STRING,
            event_time     TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (event_type, days(event_time))
    """)
    logger.info("Table %s is ready.", ICEBERG_TABLE)


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("IcebergLoader")
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
                "org.apache.iceberg:iceberg-aws-bundle:1.5.0",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
            ]),
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.iceberg",                        "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type",                   "rest")
        .config("spark.sql.catalog.iceberg.uri",                    CATALOG_URI)
        .config("spark.sql.catalog.iceberg.io-impl",                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg.warehouse",              "s3://warehouse/")
        .config("spark.sql.catalog.iceberg.s3.endpoint",            MINIO_ENDPOINT)
        .config("spark.sql.catalog.iceberg.s3.access-key-id",       MINIO_ACCESS_KEY)
        .config("spark.sql.catalog.iceberg.s3.secret-access-key",   MINIO_SECRET_KEY)
        .config("spark.sql.catalog.iceberg.s3.path-style-access",   "true")
        .getOrCreate()
    )


def run(spark: SparkSession) -> None:
    ensure_table_exists(spark)

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", CLEAN_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        raw
        .select(from_json(col("value").cast("string"), WIDE_SCHEMA).alias("d"))
        .select("d.*")
    )

    transformed = transform_for_iceberg(parsed)

    query = (
        transformed.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="30 seconds")
        .toTable(ICEBERG_TABLE)
    )
    query.awaitTermination()


if __name__ == "__main__":
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    run(spark)
