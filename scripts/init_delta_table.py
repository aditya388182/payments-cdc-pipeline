#!/usr/bin/env python3
"""
(idempotent) initialization of the Delta Lake transactions table
with Change Data Feed enabled.
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession


def build_spark() -> SparkSession:
    packages = ",".join(
        [
            "io.delta:delta-spark_2.12:3.1.0",
            "org.apache.hadoop:hadoop-aws:3.3.4",
        ]
    )

    spark = (
        SparkSession.builder.appName("init_delta")
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
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize Delta transactions table")
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop and recreate the table (destructive)",
    )
    args = parser.parse_args()

    spark = build_spark()
    table_path = "s3a://payments-lake/transactions"

    if args.drop:
        print(f"[init] Dropping existing table at {table_path} ...")
        spark.sql(f"DROP TABLE IF EXISTS delta.`{table_path}`")

    print(f"[init] Creating table (IF NOT EXISTS) at {table_path} ...")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS delta.`{table_path}` (
            transaction_id   STRING,
            merchant_id      STRING,
            amount_minor     BIGINT,
            currency         STRING,
            status           STRING,
            event_type       STRING,
            created_at       TIMESTAMP,
            updated_at       TIMESTAMP,
            lsn              BIGINT,
            source_ts        TIMESTAMP,
            is_delete        BOOLEAN
        )
        USING DELTA
        TBLPROPERTIES (
            delta.enableChangeDataFeed = true
        )
        """
    )

    # Verify CDF is enabled
    props = spark.sql(f"SHOW TBLPROPERTIES delta.`{table_path}`").collect()
    cdf_enabled = False
    for row in props:
        if row.key == "delta.enableChangeDataFeed" and row.value.lower() == "true":
            cdf_enabled = True
            break

    print("\n[init] Table properties:")
    for row in props:
        print(f"  {row.key} = {row.value}")

    if not cdf_enabled:
        print("\n[ERROR] delta.enableChangeDataFeed is NOT true. Aborting.", file=sys.stderr)
        sys.exit(1)

    print("\n[init] SUCCESS – Delta table ready with CDF enabled.")
    spark.stop()


if __name__ == "__main__":
    main()