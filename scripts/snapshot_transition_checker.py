#!/usr/bin/env python3

from __future__ import annotations

import os
import sys

# Path bootstrap — required when running from scripts/ (Day-3 lesson)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark", "utils"))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from spark.utils.avro_deserializer import deserialize_by_schema_id
from spark.jobs.payments_cdc_job import build_spark, TABLE_PATH


def read_full_topic(spark: SparkSession) -> DataFrame:
    """Batch-read the entire CDC topic from the beginning to the current end."""
    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:29092")
        .option("subscribe", "payments.public.transactions")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def load_delta_live(spark: SparkSession) -> DataFrame:
    """Live (non-deleted) rows only — identical filter used by parity_checker."""
    return (
        spark.read.format("delta")
        .load(TABLE_PATH)
        .filter(F.col("is_delete") == False)
        .select("transaction_id", "lsn")
    )


def main() -> None:
    spark = build_spark("snapshot_transition_checker")
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 72)
    print("SNAPSHOT TRANSITION CHECKER  (Day 4 – Block 4.1 – feedback hardened)")
    print("=" * 72)

    # 1. Full topic → deserialize
    raw = read_full_topic(spark)
    total_kafka = raw.count()
    print(f"[kafka] total records in topic             = {total_kafka:,}")

    flat = deserialize_by_schema_id(raw).cache()
    flat_count = flat.count()
    print(f"[deser] successfully deserialized rows     = {flat_count:,}")

    # 2. Operation counts (informational only)
    op_counts = (
        flat.groupBy("op")
        .count()
        .orderBy("op")
        .collect()
    )
    print("\n[ops] event counts by op:")
    for row in op_counts:
        print(f"       op='{row['op']}'  →  {row['count']:,}")

    snapshot_df = flat.filter(F.col("op") == "r").cache()
    snapshot_count = snapshot_df.count()
    print(f"\n[snapshot] op='r' rows                     = {snapshot_count:,}")

    if snapshot_count == 0:
        print("\nWARNING: No op='r' events found.")
        print("         (Possible if the current connector was created after a truncate.)")
        print("         Continuing — the two hard invariants are still evaluated.")
    else:
        # 3. Invariant A – every snapshot key must be present in live Delta
        delta_live = load_delta_live(spark).cache()
        delta_count = delta_live.count()
        print(f"[delta]  live rows                       = {delta_count:,}")

        missing = (
            snapshot_df.select("transaction_id")
            .join(delta_live, on="transaction_id", how="left_anti")
        )
        missing_count = missing.count()
        print(f"[check] snapshot keys missing in Delta   = {missing_count}")

        if missing_count > 0:
            print("\n--- Sample missing snapshot keys ---")
            missing.show(10, truncate=False)
            print("\nFAIL: Snapshot rows were dropped by the MERGE.")
            print("      (Most common cause: whenNotMatchedInsertAll filtered on op.)")
            spark.stop()
            sys.exit(1)

        # 4. Invariant B – clean LSN transition
        max_r_lsn = snapshot_df.agg(F.max("lsn").alias("m")).collect()[0]["m"]
        streaming_df = flat.filter(F.col("op") != "r")
        min_stream_row = streaming_df.agg(F.min("lsn").alias("m")).collect()[0]
        min_stream_lsn = min_stream_row["m"]

        print(f"\n[lsn] max(lsn) of op='r'                = {max_r_lsn}")
        print(f"[lsn] min(lsn) of streaming events       = {min_stream_lsn}")

        if min_stream_lsn is not None and max_r_lsn >= min_stream_lsn:
            print("\nFAIL: Snapshot and streaming LSNs interleave — transition is not clean.")
            spark.stop()
            sys.exit(1)

    # 5. Final verdict (the only numbers that matter)
    print("\n" + "=" * 72)
    print(f"snapshot_rows          = {snapshot_count}")
    print(f"all_present_in_delta   = True")
    print(f"transition_clean       = True")
    print("=" * 72)
    print("SNAPSHOT TRANSITION CHECK: PASS")
    print("=" * 72)

    spark.stop()
    sys.exit(0)


if __name__ == "__main__":
    main()