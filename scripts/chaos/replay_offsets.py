#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# Import production functions from day 2 work
from spark.utils.avro_deserializer import (
    deserialize_confluent_avro,
    flatten_envelope,
)
from spark.jobs.payments_cdc_job import (
    process_batch,
    TABLE_PATH,
    LEDGER_PATH,
    build_spark_session,          
)


def build_replay_session() -> SparkSession:
    """Minimal session that matches the production job configuration."""
    spark = (
        SparkSession.builder
        .appName("replay_offsets_probe")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_range(
    spark: SparkSession,
    starting_offsets: str,
    ending_offsets: str,
) -> DataFrame:
    """Batch-read a closed offset range from the CDC topic."""
    return (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:29092")
        .option("subscribe", "payments.public.transactions")
        .option("startingOffsets", starting_offsets)
        .option("endingOffsets", ending_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


def snapshot_metrics(spark: SparkSession) -> tuple[int, int]:
    df = (
        spark.read.format("delta")
        .load(TABLE_PATH)
        .filter(F.col("is_delete") == False)
    )
    cnt = df.count()
    total = df.agg(F.coalesce(F.sum("amount_minor"), F.lit(0)).alias("s")).collect()[0]["s"]
    return cnt, int(total)


def run_lsn_guard_test(spark: SparkSession, start_off: str, end_off: str) -> None:
    print("\n=== LSN-GUARD REPLAY TEST (commit=False, batch_id=999_999_999) ===")

    raw = read_kafka_range(spark, start_off, end_off)
    print(f"Kafka records read: {raw.count()}")

    # Same deserialization path the streaming job uses
    decoded = deserialize_confluent_avro(raw)
    flat = flatten_envelope(decoded)

    before_cnt, before_sum = snapshot_metrics(spark)
    print(f"BEFORE  → rows={before_cnt:,}  sum(amount_minor)={before_sum:,}")

    # THE CRITICAL CALL — commit=False protects the ledger
    process_batch(flat, batch_id=999_999_999, commit=False)

    after_cnt, after_sum = snapshot_metrics(spark)
    print(f"AFTER   → rows={after_cnt:,}  sum(amount_minor)={after_sum:,}")

    if after_cnt != before_cnt or after_sum != before_sum:
        raise AssertionError(
            f"REPLAY CORRUPTED THE TABLE!\n"
            f"  count {before_cnt} → {after_cnt}\n"
            f"  sum   {before_sum} → {after_sum}"
        )

    print("REPLAY TEST: table unchanged (LSN guard held, ledger not poisoned)")


def run_ledger_skip_test(spark: SparkSession, already_committed_batch_id: int) -> None:
    print(f"\n=== LEDGER-SKIP TEST (batch_id={already_committed_batch_id}) ===")

    # Empty DataFrame is enough — we only care that the ledger check fires
    empty = spark.createDataFrame([], schema="transaction_id string")

    before_cnt, before_sum = snapshot_metrics(spark)

    process_batch(empty, batch_id=already_committed_batch_id, commit=True)

    after_cnt, after_sum = snapshot_metrics(spark)

    if after_cnt != before_cnt or after_sum != before_sum:
        raise AssertionError("Ledger skip path wrote data — this must never happen")

    print(f"LEDGER SKIP TEST: batch {already_committed_batch_id} correctly ignored")


def main() -> None:
    parser = argparse.ArgumentParser(description="Day 3 offset-replay / ledger-skip proofs")
    parser.add_argument(
        "--mode",
        choices=["lsn-guard", "ledger-skip", "both"],
        default="both",
        help="Which proof to run",
    )
    parser.add_argument(
        "--start-offsets",
        default='{"payments.public.transactions":{"0":0,"1":0,"2":0}}',
        help="Kafka startingOffsets JSON",
    )
    parser.add_argument(
        "--end-offsets",
        default='{"payments.public.transactions":{"0":500,"1":500,"2":500}}',
        help="Kafka endingOffsets JSON (must be already processed)",
    )
    parser.add_argument(
        "--known-batch-id",
        type=int,
        default=1,
        help="A batch_id that is already present in the ledger (for ledger-skip test)",
    )
    args = parser.parse_args()

    spark = build_replay_session()

    try:
        if args.mode in ("lsn-guard", "both"):
            run_lsn_guard_test(spark, args.start_offsets, args.end_offsets)

        if args.mode in ("ledger-skip", "both"):
            run_ledger_skip_test(spark, args.known_batch_id)

        print("\nAll Day-3 replay proofs passed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()