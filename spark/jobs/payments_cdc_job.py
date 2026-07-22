#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
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


# MERGE with LSN monotonicity guard + Soft Delete (tombstone) architecture
def merge_batch(spark, staged):
    if staged.rdd.isEmpty():
        print("[merge] empty batch – skipping")
        return

    delta_table = DeltaTable.forPath(spark, TABLE_PATH)
    (
        delta_table.alias("target")
        .merge(
            staged.alias("staged"),
            "target.transaction_id = staged.transaction_id",
        )
        # Soft delete: a delete event (is_delete=true, higher LSN) flips the flag
        # via this same update clause. The row and its LSN are NEVER removed,
        # so the guard always has a target to reject stale replays against.
        .whenMatchedUpdateAll(condition="staged.lsn > target.lsn")
        # Inserts re-enabled — safe now, because deleted keys retain a tombstone row.
        .whenNotMatchedInsertAll(condition="staged.is_delete = false")
        .execute()
    )
    print("[merge] MERGE completed (soft-delete, inserts enabled)")
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
        # Ledger table does not exist yet
        return False


def commit_ledger(spark: SparkSession, batch_id: int) -> None:
    (
        spark.createDataFrame([(APP_ID, batch_id)], ["app_id", "batch_id"])
        .write.format("delta")
        .mode("append")
        .save(LEDGER_PATH)
    )
    print(f"[ledger] committed batch_id={batch_id}")


# process_batch
def process_batch(
    df: DataFrame,
    batch_id: int,
    commit: bool = True,  # ← Safeguard #1: prevent ledger poisoning
) -> None:
    """
    Core micro-batch logic.

    commit=True  → normal streaming path (write to ledger)
    commit=False → used by replay_offsets.py so that a high fake batch_id
                   cannot poison the ledger and permanently skip future batches.
    """
    spark = df.sparkSession

    if commit and already_committed(spark, batch_id):
        print(f"[skip] batch {batch_id} already committed – skipping")
        return

    print(f"[batch {batch_id}] incoming rows = {df.count()}")

    # 1. Deduplicate to latest LSN per key
    staged = dedup_latest(df)
    print(f"[batch {batch_id}] after dedup = {staged.count()}")

    # 2. Guarded MERGE (soft-delete)
    merge_batch(spark, staged)

    # 3. Commit to ledger only when requested
    if commit:
        commit_ledger(spark, batch_id)
    else:
        print(f"[batch {batch_id}] commit=False → ledger NOT updated")


# Streaming entrypoint
def run_streaming(
    spark: SparkSession,
    starting_offsets: str = "earliest",
    max_offsets_per_trigger: int = 5000,
    use_schema_id_path: bool = True,
) -> None:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
        .option("failOnDataLoss", "false")
        .load()
    )

    # only extract the schema_id here (lazy transformation).
    # The actual per-schema decoding happens inside foreachBatch
    # where the DataFrame is a normal batch DataFrame.
    if use_schema_id_path:
        print("[stream] using deserialize_by_schema_id (schema-evolution path)")
        # Just carry the raw Kafka rows forward; decoding is done per micro-batch
        staged_raw = raw
    else:
        print("[stream] using single-schema deserialize path")
        schema_json = latest_schema()
        staged_raw = deserialize(raw, schema_json)

    def foreach_batch_wrapper(df: DataFrame, batch_id: int) -> None:
        if use_schema_id_path:
            # Now it is safe — df is a batch DataFrame
            parsed = deserialize_by_schema_id(df)
        else:
            parsed = df   # already deserialized in the single-schema path

        # Project to the columns the MERGE needs
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
        staged = parsed.select(*final_cols)
        process_batch(staged, batch_id, commit=True)

    query = (
        staged_raw.writeStream.foreachBatch(foreach_batch_wrapper)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start()
    )
    print("[stream] Streaming query started. Awaiting termination...")
    query.awaitTermination()

def main() -> None:
    parser = argparse.ArgumentParser(description="Payments CDC Streaming Job")
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
    try:
        run_streaming(
            spark,
            starting_offsets=args.starting_offsets,
            max_offsets_per_trigger=args.max_offsets,
            use_schema_id_path=args.schema_id_path,
        )
    except KeyboardInterrupt:
        print("\n[stream] Interrupted by user")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()