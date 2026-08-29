from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Day 4 + Day 5 utilities
from spark.utils.avro_deserializer import deserialize_by_schema_id
from spark.utils.validator import validate
from spark.utils.dlq_writer import write_to_dlq, merchants, get_merchant_registry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("payments.cdc")

# Constants – match Days 1-4
KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "payments.public.transactions"
CHECKPOINT = "s3a://payments-lake/checkpoints/payments-cdc"
TABLE_PATH = "s3a://payments-lake/transactions"
LEDGER_PATH = "s3a://payments-lake/_batch_ledger"
APP_ID = "payments_cdc_v1"


# Spark session builder (preserves the working Day-2/4 package set)
def build_spark(app_name: str = "payments-cdc") -> SparkSession:
    packages = ",".join([
        "io.delta:delta-spark_2.12:3.1.0",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "org.apache.spark:spark-avro_2.12:3.5.1",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.postgresql:postgresql:42.7.3",
    ])

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config("spark.jars.packages", packages)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
    )
    return builder.getOrCreate()


# Ledger helpers (Safeguard #1 – commit flag + exact-match)
def already_committed(spark: SparkSession, batch_id: int) -> bool:
    if not DeltaTable.isDeltaTable(spark, LEDGER_PATH):
        logger.info("Ledger absent – treating batch_id=%s as new", batch_id)
        return False

    ledger = spark.read.format("delta").load(LEDGER_PATH)
    return (
        ledger
        .filter((F.col("app_id") == APP_ID) & (F.col("batch_id") == batch_id))
        .limit(1)
        .count() > 0
    )


def commit_ledger(spark: SparkSession, batch_id: int, commit: bool = True) -> None:
    if not commit:
        logger.info("commit=False → skipping ledger write for batch_id=%s", batch_id)
        return

    df = spark.createDataFrame(
        [(APP_ID, int(batch_id), datetime.now(timezone.utc))],
        schema="app_id STRING, batch_id BIGINT, committed_at TIMESTAMP",
    )
    (
        df.write
          .format("delta")
          .mode("append")
          .save(LEDGER_PATH)
    )
    logger.info("Ledger committed batch_id=%s", batch_id)


# Deduplication by latest LSN (deterministic when offset is available)
def dedup_latest(df: DataFrame) -> DataFrame:
    order = [F.col("lsn").desc()]
    if "offset" in df.columns:
        order.append(F.col("offset").desc())
    else:
        logger.warning(
            "dedup_latest: no 'offset' column – equal-LSN ties resolve "
            "NON-DETERMINISTICALLY. Add col('offset') to the deserializer "
            "projection in avro_deserializer.py to close this gap."
        )

    w = Window.partitionBy("transaction_id").orderBy(*order)
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


# Soft-delete MERGE with LSN monotonicity guard (exactly-once mechanism #1)
def merge_batch(spark: SparkSession, staged: DataFrame) -> int:
    n = staged.count()
    if n == 0:
        return 0

    delta_table = DeltaTable.forPath(spark, TABLE_PATH)
    (
        delta_table.alias("target")
        .merge(
            staged.alias("staged"),
            "target.transaction_id = staged.transaction_id",
        )
        .whenMatchedUpdateAll(condition="staged.lsn > target.lsn")
        .whenNotMatchedInsertAll()   # tombstones included – see policy note above
        .execute()
    )
    logger.info("MERGE completed – %d staged rows", n)
    return n


# Core batch processor (Day 5 gate order + single persist)
def process_batch(
    batch_df: DataFrame,
    batch_id: int,
    commit: bool = True,
) -> None:
    spark = batch_df.sparkSession

    # 1. Ledger skip
    if already_committed(spark, batch_id):
        logger.info("batch_id=%s already committed – skipping", batch_id)
        return

    # Persist BEFORE any action. The lineage includes a Kafka read, Confluent
    # wire-format stripping, and schema-ID-dispatched Avro decode. Without
    # this, every count / write below re-executes all of it.
    batch_df.persist()
    try:
        n_in = batch_df.count()          # single scan of the batch
        if n_in == 0:
            # Reachable when a micro-batch contains ONLY Kafka tombstone
            # records (tombstones.on.delete=true): non-empty at the wrapper,
            # empty after the deserializer's .filter(col("e").isNotNull())
            # drops the null-valued messages. Commit so the batch_id is
            # recorded; there is nothing to merge.
            logger.info("batch_id=%s empty after deserialize – nothing to do", batch_id)
            commit_ledger(spark, batch_id, commit=commit)
            return

        # 2. 🔴 Synchronous validation gate (before anything touches Delta)
        valid, invalid = validate(batch_df, merchants())
        valid.persist()
        invalid.persist()
        try:
            # 3. Quarantine
            n_dlq = write_to_dlq(invalid)     # returns count; no extra action

            # 4. Dedup by latest LSN
            staged = dedup_latest(valid)

            # 5. Soft-delete MERGE (LSN guard + tombstone policy)
            n_merged = merge_batch(spark, staged)

            # 6. Ledger commit (respects commit flag)
            commit_ledger(spark, batch_id, commit=commit)

            logger.info(
                "batch_id=%s finished – in=%d merged=%d dlq=%d",
                batch_id, n_in, n_merged, n_dlq,
            )
        finally:
            valid.unpersist()
            invalid.unpersist()
    finally:
        batch_df.unpersist()


# foreachBatch wrapper (schema-id path is the only production path – Day 4)
def foreach_batch_wrapper(raw_batch: DataFrame, batch_id: int) -> None:
    raw_batch.persist()
    try:
        # count() rather than rdd.isEmpty(): once persisted, count() reuses the
        # cached blocks, while rdd.isEmpty() forces a conversion to RDD.
        if raw_batch.count() == 0:
            logger.info("batch_id=%s empty (no Kafka records) – skipping", batch_id)
            return

        # Day-4 production default – schema-id aware deserializer
        flat = deserialize_by_schema_id(raw_batch)
        process_batch(flat, batch_id, commit=True)
    finally:
        raw_batch.unpersist()


# Streaming entry point
def _path_exists(spark: SparkSession, path_str: str) -> bool:
    """Hadoop-FS existence check (works for s3a:// against MinIO)."""
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path = spark._jvm.org.apache.hadoop.fs.Path(path_str)
    return path.getFileSystem(hadoop_conf).exists(path)


def run_stream(starting_offsets: str = "earliest") -> None:
    spark = build_spark()

    # Force merchant cache load at startup (fail-closed: an empty set would
    # reject 100% of traffic, so MerchantRegistry raises on first-load failure)
    reg = get_merchant_registry()
    logger.info("Startup – loaded %d merchants", len(reg.merchant_ids))
    logger.info("[stream] using deserialize_by_schema_id (schema-evolution path)")

    # Explicit checkpoint existence check. Removes the Day-4 §7.1 ambiguity:
    # the log now states unambiguously whether this run resumed or started
    # fresh, so "was this a restart?" can never again be a question that costs
    # an afternoon.
    resuming = _path_exists(spark, CHECKPOINT)
    logger.info(
        "[stream] checkpoint %s at %s – %s",
        "EXISTS" if resuming else "ABSENT",
        CHECKPOINT,
        "resuming from checkpoint; startingOffsets is IGNORED"
        if resuming
        else f"FRESH START; startingOffsets={starting_offsets} APPLIES",
    )

    raw = (
        spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
             .option("subscribe", TOPIC)
             .option("startingOffsets", starting_offsets)
             .option("failOnDataLoss", "false")
             .load()
    )

    query = (
        raw.writeStream
           .foreachBatch(foreach_batch_wrapper)
           .option("checkpointLocation", CHECKPOINT)
           .trigger(processingTime="10 seconds")
           .start()
    )

    logger.info("[stream] Streaming query started. Awaiting termination...")
    query.awaitTermination()


# CLI
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Payments CDC Structured Streaming job (Day 5 gate)"
    )
    parser.add_argument(
        "--starting-offsets",
        default="earliest",
        choices=["earliest", "latest"],
        help=(
            "Only applies on a FRESH checkpoint; otherwise the checkpoint wins. "
            "Default 'earliest' because silently skipping a backlog is "
            "unacceptable for an audit ledger. Use 'latest' only per "
            "runbooks/checkpoint_corruption.md."
        ),
    )
    args = parser.parse_args()
    run_stream(starting_offsets=args.starting_offsets)


if __name__ == "__main__":
    main()