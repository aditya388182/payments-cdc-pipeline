#!/usr/bin/env python3

from __future__ import annotations

import argparse
import signal
import sys
import time
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Local imports
from spark.utils.avro_deserializer import (
    deserialize,
    deserialize_by_schema_id,
    latest_schema,
)
from spark.utils.metrics_listener import (
    push_metrics,
    extract_processed_offsets,
)

# Constants
TABLE_PATH = "s3a://payments-lake/transactions"
LEDGER_PATH = "s3a://payments-lake/_batch_ledger"
CHECKPOINT_PATH = "s3a://payments-lake/checkpoints/payments-cdc"
APP_ID = "payments_cdc_v1"
KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "payments.public.transactions"


def build_spark(app_name: str = "payments_cdc_v1") -> SparkSession:
    packages = ",".join(
        [
            "io.delta:delta-spark_2.12:3.1.0",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            "org.apache.spark:spark-avro_2.12:3.5.1",
            "org.apache.hadoop:hadoop-aws:3.3.4",
        ]
    )

    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.jars.packages", packages)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # AQE + reasonable defaults for local
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


# Deduplication
def dedup_latest(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("transaction_id").orderBy(F.col("lsn").desc())

    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# MERGE with LSN monotonicity guard
def merge_batch(spark: SparkSession, staged: DataFrame) -> int:
    if staged.rdd.isEmpty():
        print("[merge] empty batch – skipping")
        return 0

    delta_table = DeltaTable.forPath(spark, TABLE_PATH)

    (
        delta_table.alias("target")
        .merge(
            staged.alias("staged"),
            "target.transaction_id = staged.transaction_id",
        )
        .whenMatchedDelete(
            condition="staged.lsn > target.lsn AND staged.is_delete = true"
        )
        .whenMatchedUpdateAll(
            condition="staged.lsn > target.lsn AND staged.is_delete = false"
        )
        .whenNotMatchedInsertAll(
            condition="staged.is_delete = false"
        )
        .execute()
    )

    print("[merge] MERGE completed")

    return staged.count()


# Batch Ledger (exactly-once)
def already_committed(spark: SparkSession, batch_id: int) -> bool:
    """Return True if this batch_id (or higher) has already been committed."""
    try:
        ledger = spark.read.format("delta").load(LEDGER_PATH)

        row = (
            ledger.filter(F.col("app_id") == APP_ID)
            .agg(F.max("batch_id").alias("max_batch"))
            .collect()[0]
        )

        max_batch = row["max_batch"]

        if max_batch is None:
            return False

        return batch_id <= max_batch

    except Exception:
        # Ledger table does not exist till now, next day
        return False


def commit_ledger(spark: SparkSession, batch_id: int) -> bool:
    (
        spark.createDataFrame(
            [(APP_ID, batch_id)],
            ["app_id", "batch_id"],
        )
        .withColumn("committed_at", F.current_timestamp())
        .write.format("delta")
        .mode("append")
        .save(LEDGER_PATH)
    )

    print(f"[ledger] committed batch_id={batch_id}")

    return True


# Process batch
def process_batch(
    df: DataFrame,
    batch_id: int,
    commit: bool = True,
) -> None:
    """
    Core micro-batch logic.

    commit=True  > normal streaming path (write to ledger)
    commit=False > used by replay_offsets.py so that a high fake batch_id
                   cannot poison the ledger and permanently skip future batches.
    """

    spark = df.sparkSession

    if commit and already_committed(spark, batch_id):
        print(f"[skip] batch {batch_id} already committed – skipping")
        return

    try:
        t0 = time.time()

        # 1. Incoming rows
        n_in = df.count()
        print(f"[batch {batch_id}] incoming rows = {n_in}")

        # 2. Deduplicate
        staged = dedup_latest(df)
        n_staged = staged.count()

        print(f"[batch {batch_id}] after dedup = {n_staged}")

        # 3. MERGE
        n_merged = merge_batch(spark, staged)

        # There is currently no DLQ path in this job.
        # Keep the metric at zero until DLQ processing is implemented.
        n_dlq = 0

        # 4. Commit Ledger
        # Crucial: only push metrics if this succeeds!
        if commit:
            committed = commit_ledger(spark, batch_id)
        else:
            committed = False
            print(
                f"[batch {batch_id}] commit=False → ledger NOT updated"
            )

        batch_seconds = time.time() - t0

        # 5. Push Observability Metrics
        if committed:
            # Pull the latest commit timestamp to send to Prometheus.
            last_commit_df = (
                spark.read.format("delta")
                .load(LEDGER_PATH)
                .filter(F.col("app_id") == APP_ID)
                .orderBy(F.col("batch_id").desc())
                .limit(1)
                .collect()
            )

            if last_commit_df:
                last_ts = (
                    last_commit_df[0]["committed_at"].timestamp()
                )
            else:
                last_ts = time.time()

            # Processed offsets aren't available until the query
            # progresses, so push an empty dict here.
            # The sidecar handles lag separately.
            push_metrics(
                batch_id=batch_id,
                n_in=n_in,
                n_merged=n_merged,
                n_dlq=n_dlq,
                batch_seconds=batch_seconds,
                last_commit_ts=last_ts,
                processed_offsets={},
            )

        print(
            f"Batch {batch_id} complete. "
            f"In={n_in}, "
            f"Merged={n_merged}, "
            f"DLQ={n_dlq}, "
            f"Committed={committed}"
        )

    except Exception as e:
        print(f"Batch {batch_id} failed: {e}")
        raise


# Streaming entrypoint
def run_streaming(
    spark: SparkSession,
    starting_offsets: str = "earliest",
    max_offsets_per_trigger: int = 5000,
    use_schema_id_path: bool = False,
):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
        .option("failOnDataLoss", "false")
        .load()
    )

    if use_schema_id_path:
        print(
            "[stream] using deserialize_by_schema_id "
            "(schema-evolution path)"
        )
        parsed = deserialize_by_schema_id(raw)
    else:
        print("[stream] using single-schema deserialize path")
        schema_json = latest_schema()
        parsed = deserialize(raw, schema_json)

    # Select only the columns needed for the MERGE
    final_cols = [
        "transaction_id",
        "merchant_id",
        "amount_minor",
        "currency",
        "status",
        "event_type",
        "created_at",
        "updated_at",
        "lsn",
        "source_ts",
        "is_delete",
    ]

    # Include channel if it exists in the parsed schema.
    if "channel" in parsed.columns:
        final_cols.insert(4, "channel")

    staged = parsed.select(*final_cols)

    query = (
        staged.writeStream.foreachBatch(
            lambda df, batch_id: process_batch(
                df,
                batch_id,
                commit=True,
            )
        )
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("[stream] Streaming query started. Awaiting termination...")

    return query


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Payments CDC Streaming Job"
    )

    parser.add_argument(
        "--starting-offsets",
        default="earliest",
        help="earliest | latest | JSON offsets",
    )

    parser.add_argument(
        "--max-offsets",
        type=int,
        default=5000,
        help="maxOffsetsPerTrigger",
    )

    parser.add_argument(
        "--schema-id-path",
        action="store_true",
        help="Use per-message schema-id deserialization (Day-4 path)",
    )

    args = parser.parse_args()

    spark = build_spark()

    query = run_streaming(
        spark,
        starting_offsets=args.starting_offsets,
        max_offsets_per_trigger=args.max_offsets,
        use_schema_id_path=args.schema_id_path,
    )

    # Graceful shutdown handler
    def handle_sigterm(signum, frame):
        print(
            "Received SIGTERM! Stopping query gracefully..."
        )
        query.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)

    try:
        query.awaitTermination()

    except KeyboardInterrupt:
        print(
            "Keyboard interrupt! Stopping query gracefully..."
        )
        query.stop()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()