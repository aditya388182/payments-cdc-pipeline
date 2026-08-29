#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark() -> SparkSession:
    packages = ",".join(
        [
            "io.delta:delta-spark_2.12:3.1.0",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "org.postgresql:postgresql:42.7.3",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", # Added for DLQ reading
        ]
    )

    spark = (
        SparkSession.builder.appName("parity_checker")
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
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_postgres(spark: SparkSession):
    return (
        spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://localhost:5432/payments")
        .option("dbtable", "transactions")
        .option("user", "payments")
        .option("password", "payments")
        .option("driver", "org.postgresql.Driver")
        .load()
        .select(
            F.col("transaction_id").cast("string").alias("transaction_id"),
            "merchant_id",
            "amount_minor",
            "currency",
            "status",
            "event_type",
        )
    )


def load_delta(spark: SparkSession):
    """Load live (non-deleted) rows only."""
    return (
        spark.read.format("delta")
        .load("s3a://payments-lake/transactions")
        .filter(F.col("is_delete") == False)  # ← Safeguard #4
        .select(
            "transaction_id",
            "merchant_id",
            "amount_minor",
            "currency",
            "status",
            "event_type",
        )
    )


def load_dlq_keys(spark: SparkSession):
    """Load keys from the DLQ topic to identify quarantined records."""
    try:
        dlq_df = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:29092")
            .option("subscribe", "payments.transactions.dlq")
            .option("startingOffsets", "earliest")
            .load()
        )
        return dlq_df.select(F.col("key").cast("string").alias("transaction_id")).distinct()
    except Exception:
        # Topic might be empty or missing yet
        from pyspark.sql.types import StructType, StructField, StringType
        return spark.createDataFrame([], StructType([StructField("transaction_id", StringType(), True)]))


def main() -> None:
    parser = argparse.ArgumentParser(description="Postgres ↔ Delta parity checker")
    parser.add_argument(
        "--tolerate-inflight",
        type=int,
        default=0,
        help="Allow up to N missing_in_delta rows (for rows still in flight)",
    )
    args = parser.parse_args()

    spark = build_spark()

    pg = load_postgres(spark).cache()
    delta = load_delta(spark).cache()
    dlq_keys = load_dlq_keys(spark).cache()

    pg_count = pg.count()
    delta_count = delta.count()

    # Missing in Delta (present in Postgres, absent in Delta)
    missing = pg.join(delta, on="transaction_id", how="left_anti").cache()
    missing_in_delta_total = missing.count()

    # Quarantine Logic (Intersection of missing rows and DLQ keys)
    quarantined = missing.join(dlq_keys, on="transaction_id", how="inner").count()
    unaccounted = missing_in_delta_total - quarantined

    # Extra in Delta (present in Delta, absent in Postgres)
    extra = delta.join(pg, on="transaction_id", how="left_anti")
    extra_count = extra.count()

    # Value mismatches (same key, different columns)
    joined = pg.alias("p").join(delta.alias("d"), on="transaction_id", how="inner")
    mismatches = joined.filter(
        (F.col("p.merchant_id") != F.col("d.merchant_id"))
        | (F.col("p.amount_minor") != F.col("d.amount_minor"))
        | (F.col("p.currency") != F.col("d.currency"))
        | (F.col("p.status") != F.col("d.status"))
        | (F.col("p.event_type") != F.col("d.event_type"))
    )
    mismatch_count = mismatches.count()

    # Duplicate check inside Delta
    dupes = (
        delta.groupBy("transaction_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    # Option B output format
    print(f"[parity] Postgres rows                    = {pg_count}")
    print(f"[parity] Delta live rows                  = {delta_count}")
    print(f"[parity] missing_in_delta_total           = {missing_in_delta_total}")
    print(f"[parity] unaccounted (not in DLQ either)  = {unaccounted}")
    print(f"[parity] quarantined                      = {quarantined}")
    print(f"[parity] extra_in_delta                   = {extra_count}")
    print(f"[parity] value_mismatch                   = {mismatch_count}")
    print(f"[parity] duplicate_keys_in_delta          = {dupes}\n")

    # Oracle Enforcement
    EXPECTED_QUARANTINE_AFTER_INJECTION = 53
    if quarantined not in (0, EXPECTED_QUARANTINE_AFTER_INJECTION):
        print(f"ORACLE FAIL: Expected quarantined to be 0 or {EXPECTED_QUARANTINE_AFTER_INJECTION}, got {quarantined}")
        spark.stop()
        sys.exit(1)

    # Decision
    effective_unaccounted = max(0, unaccounted - args.tolerate_inflight)
    passed = (
        effective_unaccounted == 0
        and extra_count == 0
        and mismatch_count == 0
        and dupes == 0
    )

    if not passed:
        print("\n--- Sample unaccounted (missing) ---")
        missing.join(dlq_keys, on="transaction_id", how="left_anti").show(10, truncate=False)
        print("\n--- Sample extra_in_delta ---")
        extra.show(10, truncate=False)
        print("\n--- Sample value_mismatch ---")
        mismatches.show(10, truncate=False)
        print("\nPARITY: FAIL ❌")
        spark.stop()
        sys.exit(1)
    else:
        print("PARITY: PASS")
        spark.stop()
        sys.exit(0)


if __name__ == "__main__":
    main()