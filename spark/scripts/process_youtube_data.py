"""
================================================================================
process_youtube_data.py — Apache Spark Processing Script (PySpark)
================================================================================
Purpose:
  Consume raw YouTube channel stat records from Kafka and apply
  distributed transformations using Apache Spark Structured Streaming.

Current Status:
  SCAFFOLD / BOILERPLATE — ready for production implementation.
  The structure, schema definitions, and transformation stubs are complete.
  Connect to Kafka and un-comment the streaming read to activate.

Architecture:
  Kafka Topic (youtube_raw_data)
      │
      ▼
  Spark Structured Streaming (readStream)
      │
      ├── Parse JSON schema
      ├── Compute engagement metrics
      ├── Deduplicate by channel_id + watermark
      │
      └── Sink → PostgreSQL (foreachBatch) / Parquet / Delta Lake

To Submit This Job:
  spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    /opt/spark/scripts/process_youtube_data.py

Environment Variables:
  KAFKA_BOOTSTRAP_SERVERS — Kafka broker address (default: kafka:9092)
  KAFKA_TOPIC             — Source topic (default: youtube_raw_data)
  SPARK_CHECKPOINT_DIR    — Checkpoint location for Structured Streaming
  POSTGRES_*              — Target database connection params
================================================================================
"""

from __future__ import annotations

import logging
import os
from typing import Any

# PySpark imports — available when running via spark-submit
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("spark.youtube_processor")

# ===========================================================================
# Configuration
# ===========================================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "youtube_raw_data")
CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints/youtube")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "youtube_pipeline")
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_TABLE = "channel_stats_spark"

POSTGRES_JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# ===========================================================================
# Schema — must match the JSON records produced by extractor.py
# ===========================================================================
CHANNEL_STATS_SCHEMA = StructType([
    StructField("channel_id",          StringType(),    nullable=False),
    StructField("channel_title",       StringType(),    nullable=True),
    StructField("channel_description", StringType(),    nullable=True),
    StructField("published_at",        StringType(),    nullable=True),
    StructField("country",             StringType(),    nullable=True),
    StructField("total_views",         LongType(),      nullable=True),
    StructField("subscriber_count",    LongType(),      nullable=True),
    StructField("video_count",         LongType(),      nullable=True),
    StructField("processed_at",        StringType(),    nullable=True),
])


# ===========================================================================
# Spark Session Factory
# ===========================================================================
def create_spark_session() -> SparkSession:
    """
    Build and return a SparkSession configured for:
    - Kafka Structured Streaming (requires spark-sql-kafka package)
    - PostgreSQL JDBC write

    Note: The spark-sql-kafka package must be available via:
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
    """
    spark = (
        SparkSession.builder.appName("YouTubeChannelProcessor")
        .master(os.getenv("SPARK_MASTER_URL", "local[*]"))
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        # Avoid writing _SUCCESS files on JDBC writes
        .config("spark.sql.legacy.allowHashOnMapType", "true")
        # Serialisation settings
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession created: %s", spark.version)
    return spark


# ===========================================================================
# Transformations
# ===========================================================================
def parse_kafka_messages(raw_df: DataFrame) -> DataFrame:
    """
    Deserialise Kafka value bytes → JSON → typed columns using the
    pre-defined CHANNEL_STATS_SCHEMA.

    Args:
        raw_df: Raw DataFrame from Kafka readStream with a binary `value` col.

    Returns:
        DataFrame with typed columns matching CHANNEL_STATS_SCHEMA.
    """
    return (
        raw_df
        .select(
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(
                F.col("value").cast(StringType()),
                CHANNEL_STATS_SCHEMA,
            ).alias("data"),
        )
        .select("kafka_timestamp", "data.*")
        .withColumn(
            "processed_at",
            F.to_timestamp("processed_at"),
        )
        .withColumn(
            "published_at",
            F.to_timestamp("published_at"),
        )
    )


def compute_engagement_metrics(df: DataFrame) -> DataFrame:
    """
    Compute derived engagement metrics from raw channel stats.

    Metrics Added:
    - avg_views_per_video : total_views / video_count (guarded against div/0)
    - views_per_subscriber: total_views / subscriber_count
    - engagement_ratio    : subscriber_count / total_views (as a percentage)
    - size_tier           : Categorical label based on subscriber count

    Args:
        df: Parsed channel stats DataFrame.

    Returns:
        DataFrame with additional metric columns.
    """
    return (
        df
        # Average views per uploaded video
        .withColumn(
            "avg_views_per_video",
            F.when(
                F.col("video_count") > 0,
                F.col("total_views") / F.col("video_count"),
            ).otherwise(F.lit(0)),
        )
        # Views per subscriber
        .withColumn(
            "views_per_subscriber",
            F.when(
                F.col("subscriber_count") > 0,
                F.col("total_views") / F.col("subscriber_count"),
            ).otherwise(F.lit(0)),
        )
        # Engagement ratio: what fraction of viewers subscribe
        .withColumn(
            "engagement_ratio",
            F.when(
                F.col("total_views") > 0,
                (F.col("subscriber_count") / F.col("total_views")) * 100,
            ).otherwise(F.lit(0)),
        )
        # Tier classification based on subscriber count
        .withColumn(
            "size_tier",
            F.when(F.col("subscriber_count") >= 1_000_000, "Mega (1M+)")
            .when(F.col("subscriber_count") >= 100_000, "Large (100K–1M)")
            .when(F.col("subscriber_count") >= 10_000, "Mid (10K–100K)")
            .when(F.col("subscriber_count") >= 1_000, "Small (1K–10K)")
            .otherwise("Micro (<1K)"),
        )
    )


def deduplicate(df: DataFrame, watermark_delay: str = "10 minutes") -> DataFrame:
    """
    Apply a watermark and drop duplicates within the watermark window.
    This handles late-arriving Kafka records gracefully.

    Args:
        df: DataFrame with a `processed_at` TimestampType column.
        watermark_delay: Tolerated late-arrival window.

    Returns:
        Deduplicated DataFrame.
    """
    return (
        df
        .withWatermark("processed_at", watermark_delay)
        .dropDuplicates(["channel_id"])
    )


# ===========================================================================
# Sink — Write to PostgreSQL via JDBC (foreachBatch)
# ===========================================================================
def write_to_postgres(batch_df: DataFrame, batch_id: int) -> None:
    """
    foreachBatch sink: upsert each micro-batch into PostgreSQL.

    Note: PySpark's JDBC writer uses INSERT only. For true UPSERT semantics,
    use a staging table + MERGE/INSERT ON CONFLICT pattern, or switch to
    Delta Lake / Apache Iceberg for ACID guarantees at scale.

    Args:
        batch_df: Micro-batch DataFrame.
        batch_id: Monotonically increasing batch identifier.
    """
    if batch_df.isEmpty():
        log.info("Batch %d is empty — skipping", batch_id)
        return

    count = batch_df.count()
    log.info("Writing batch %d with %d records to PostgreSQL", batch_id, count)

    (
        batch_df
        .write.format("jdbc")
        .option("url", POSTGRES_JDBC_URL)
        .option("dbtable", POSTGRES_TABLE)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )
    log.info("Batch %d committed to PostgreSQL", batch_id)


# ===========================================================================
# Main — Streaming Pipeline
# ===========================================================================
def main() -> None:
    spark = create_spark_session()

    # -------------------------------------------------------------------------
    # READ: Kafka Structured Streaming source
    # Un-comment and configure when Kafka is running and the topic is populated.
    # -------------------------------------------------------------------------
    log.info("Reading from Kafka topic: %s at %s", KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS)

    raw_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        # Consumer group for offset management
        .option("kafka.group.id", "spark-youtube-consumer")
        # Maximum records per micro-batch trigger
        .option("maxOffsetsPerTrigger", 1000)
        .load()
    )

    # -------------------------------------------------------------------------
    # TRANSFORM: Parse → Compute Metrics → Deduplicate
    # -------------------------------------------------------------------------
    parsed_df = parse_kafka_messages(raw_stream_df)
    metrics_df = compute_engagement_metrics(parsed_df)
    final_df = deduplicate(metrics_df)

    # -------------------------------------------------------------------------
    # WRITE: PostgreSQL via foreachBatch
    # -------------------------------------------------------------------------
    query = (
        final_df.writeStream
        .foreachBatch(write_to_postgres)
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="30 seconds")
        .start()
    )

    log.info("Streaming query started — awaiting termination…")
    query.awaitTermination()


# ===========================================================================
# Batch Processing Mode (alternative to streaming for initial load)
# ===========================================================================
def run_batch_from_postgres(spark: SparkSession) -> None:
    """
    Batch mode: read channel_stats from PostgreSQL, compute metrics,
    and write enriched results back to a separate analytics table.

    Useful for backfill or scheduled batch enrichment jobs.
    """
    log.info("Running batch enrichment from PostgreSQL")

    df = (
        spark.read.format("jdbc")
        .option("url", POSTGRES_JDBC_URL)
        .option("dbtable", "channel_stats")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    enriched_df = compute_engagement_metrics(df)

    # Write enriched data back to a separate table
    (
        enriched_df
        .write.format("jdbc")
        .option("url", POSTGRES_JDBC_URL)
        .option("dbtable", "channel_stats_enriched")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )
    log.info("Batch enrichment complete")


if __name__ == "__main__":
    mode = os.getenv("SPARK_RUN_MODE", "streaming")
    if mode == "batch":
        _spark = create_spark_session()
        run_batch_from_postgres(_spark)
    else:
        main()
