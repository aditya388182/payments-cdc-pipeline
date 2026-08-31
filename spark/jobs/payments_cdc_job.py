"""spark/jobs/payments_cdc_job.py

ONE definition of every function. Day 5 correctness + Day 6 metrics.

Safeguards:
1. process_batch(..., commit=True) — replay uses commit=False
2. delete flatten lives in avro_deserializer.coalesce(after, before)
3. validator delete exemption lives in validator.py
4. parity is_delete=false lives in parity_checker.py

Order: ledger-skip → validate → DLQ → dedup → MERGE → ledger-commit → metrics
"""
from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from spark.utils.avro_deserializer import deserialize_by_schema_id
from spark.utils.validator import validate
from spark.utils.dlq_writer import write_to_dlq, merchants, get_merchant_registry
from spark.utils.metrics_listener import extract_processed_offsets, push_metrics

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("payments.cdc")

KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "payments.public.transactions"
CHECKPOINT = "s3a://payments-lake/checkpoints/payments-cdc"
TABLE_PATH = "s3a://payments-lake/transactions"
LEDGER_PATH = "s3a://payments-lake/_batch_ledger"
APP_ID = "payments_cdc_v1"

_LAST_COMMIT_TS: float = time.time()
_LAST_PROCESSED_OFFSETS: Dict[str, int] = {}
_QUERY = None


def build_spark(app_name: str = "payments-cdc") -> SparkSession:
    import pyspark

    spark_ver = pyspark.__version__
    packages = ",".join(
        [
            "io.delta:delta-spark_2.12:3.1.0",
            f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_ver}",
            f"org.apache.spark:spark-avro_2.12:{spark_ver}",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            "org.postgresql:postgresql:42.7.3",
        ]
    )
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.jars.packages", packages)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
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
        .config("spark.sql.streaming.stopTimeout", "60000")
    )
    return builder.getOrCreate()


def already_committed(spark: SparkSession, batch_id: int) -> bool:
    if not DeltaTable.isDeltaTable(spark, LEDGER_PATH):
        logger.info("Ledger absent – treating batch_id=%s as new", batch_id)
        return False
    return (
        spark.read.format("delta")
        .load(LEDGER_PATH)
        .filter((F.col("app_id") == APP_ID) & (F.col("batch_id") == batch_id))
        .limit(1)
        .count()
        > 0
    )


def commit_ledger(spark: SparkSession, batch_id: int, commit: bool = True) -> bool:
    global _LAST_COMMIT_TS
    if not commit:
        logger.info("commit=False → skipping ledger write for batch_id=%s", batch_id)
        return False
    df = spark.createDataFrame(
        [(APP_ID, int(batch_id), datetime.now(timezone.utc))],
        schema="app_id STRING, batch_id BIGINT, committed_at TIMESTAMP",
    )
    df.write.format("delta").mode("append").save(LEDGER_PATH)
    _LAST_COMMIT_TS = time.time()
    logger.info("COMMITTED batch %s @ %s", batch_id, datetime.now(timezone.utc).isoformat())
    return True


def dedup_latest(df: DataFrame) -> DataFrame:
    order = [F.col("lsn").desc()]
    if "offset" in df.columns:
        order.append(F.col("offset").desc())
    else:
        logger.warning(
            "dedup_latest: no 'offset' column – equal-LSN ties are non-deterministic"
        )
    w = Window.partitionBy("transaction_id").orderBy(*order)
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def merge_batch(spark: SparkSession, staged: DataFrame) -> int:
    n = staged.count()
    if n == 0:
        return 0
    delta_table = DeltaTable.forPath(spark, TABLE_PATH)
    (
        delta_table.alias("target")
        .merge(staged.alias("staged"), "target.transaction_id = staged.transaction_id")
        .whenMatchedUpdateAll(condition="staged.lsn > target.lsn")
        .whenNotMatchedInsertAll()
        .execute()
    )
    logger.info("MERGE completed – %d staged rows", n)
    return n


def _emit_metrics(
    batch_id: int,
    n_in: int,
    n_merged: int,
    n_dlq: int,
    batch_seconds: float,
) -> None:
    push_metrics(
        batch_id=int(batch_id),
        n_in=int(n_in),
        n_merged=int(n_merged),
        n_dlq=int(n_dlq),
        batch_seconds=float(batch_seconds),
        last_commit_ts=_LAST_COMMIT_TS,
        processed_offsets=dict(_LAST_PROCESSED_OFFSETS),
    )


def process_batch(batch_df: DataFrame, batch_id: int, commit: bool = True) -> None:
    spark = batch_df.sparkSession
    t0 = time.time()

    if already_committed(spark, batch_id):
        logger.info("[skip] batch %s already committed – skipping", batch_id)
        return

    batch_df.persist()
    try:
        n_in = batch_df.count()
        if n_in == 0:
            logger.info("batch_id=%s empty after deserialize – nothing to merge", batch_id)
            commit_ledger(spark, batch_id, commit=commit)
            _emit_metrics(batch_id, 0, 0, 0, time.time() - t0)
            return

        valid, invalid = validate(batch_df, merchants())
        valid.persist()
        invalid.persist()
        try:
            n_dlq = write_to_dlq(invalid)
            staged = dedup_latest(valid)
            n_merged = merge_batch(spark, staged)
            commit_ledger(spark, batch_id, commit=commit)
            _emit_metrics(batch_id, n_in, n_merged, n_dlq, time.time() - t0)
            logger.info(
                "batch_id=%s finished – in=%d merged=%d dlq=%d",
                batch_id,
                n_in,
                n_merged,
                n_dlq,
            )
        finally:
            valid.unpersist()
            invalid.unpersist()
    finally:
        batch_df.unpersist()


def foreach_batch_wrapper(raw_batch: DataFrame, batch_id: int) -> None:
    raw_batch.persist()
    try:
        if raw_batch.count() == 0:
            logger.info("batch_id=%s empty (no Kafka records) – skipping", batch_id)
            return
        flat = deserialize_by_schema_id(raw_batch)
        process_batch(flat, batch_id, commit=True)
    finally:
        raw_batch.unpersist()


def _path_exists(spark: SparkSession, path_str: str) -> bool:
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path = spark._jvm.org.apache.hadoop.fs.Path(path_str)
    return path.getFileSystem(hadoop_conf).exists(path)


def run_stream(starting_offsets: str = "earliest") -> None:
    global _QUERY, _LAST_PROCESSED_OFFSETS
    spark = build_spark()

    reg = get_merchant_registry()
    logger.info("Startup – loaded %d merchants", len(reg.merchant_ids))
    logger.info("[stream] using deserialize_by_schema_id (schema-evolution path)")

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
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )
    _QUERY = (
        raw.writeStream.foreachBatch(foreach_batch_wrapper)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="10 seconds")
        .start()
    )
    logger.info("[stream] Streaming query started. Awaiting termination...")
    try:
        while _QUERY.isActive:
            try:
                _LAST_PROCESSED_OFFSETS = extract_processed_offsets(_QUERY.lastProgress)
            except Exception as exc:
                logger.warning("lastProgress read failed (%s)", exc.__class__.__name__)
            time.sleep(2.0)
        _QUERY.awaitTermination()
    finally:
        try:
            _QUERY.stop()
        except Exception:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Payments CDC Structured Streaming job")
    parser.add_argument(
        "--starting-offsets",
        default="earliest",
        choices=["earliest", "latest"],
        help=(
            "Only applies on a FRESH checkpoint; otherwise the checkpoint wins. "
            "Default earliest. Use latest only per runbooks/checkpoint_corruption.md."
        ),
    )
    args = parser.parse_args()

    def handle_sigterm(signum, _frame):
        logger.info("SIGTERM – query.stop() so the in-flight batch finishes")
        if _QUERY is not None:
            _QUERY.stop()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)
    try:
        run_stream(starting_offsets=args.starting_offsets)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt – query.stop()")
        if _QUERY is not None:
            _QUERY.stop()


if __name__ == "__main__":
    main()