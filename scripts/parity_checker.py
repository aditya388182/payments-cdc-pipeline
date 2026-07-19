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

    pg_count = pg.count()
    delta_count = delta.count()
    print(f"[parity] Postgres rows          = {pg_count}")
    print(f"[parity] Delta live rows        = {delta_count}")

    # Missing in Delta (present in Postgres, absent in Delta)
    missing = pg.join(delta, on="transaction_id", how="left_anti")
    missing_count = missing.count()

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

    print(f"[parity] missing_in_delta       = {missing_count}")
    print(f"[parity] extra_in_delta         = {extra_count}")
    print(f"[parity] value_mismatch         = {mismatch_count}")
    print(f"[parity] duplicate_keys_in_delta= {dupes}")

    # Decision
    effective_missing = max(0, missing_count - args.tolerate_inflight)
    passed = (
        effective_missing == 0
        and extra_count == 0
        and mismatch_count == 0
        and dupes == 0
    )

    if not passed:
        print("\n--- Sample missing_in_delta ---")
        missing.show(10, truncate=False)
        print("\n--- Sample extra_in_delta ---")
        extra.show(10, truncate=False)
        print("\n--- Sample value_mismatch ---")
        mismatches.show(10, truncate=False)

    if passed:
        print("\nPARITY: PASS")
        spark.stop()
        sys.exit(0)
    else:
        print("\nPARITY: FAIL")
        spark.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()