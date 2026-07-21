#!/usr/bin/env python3
"""
scripts/check_phantom.py
Diagnostic: is the MERGE join key (transaction_id) actually matching?
Traces one phantom row (live in Delta, absent in Postgres) to confirm whether
the soft-delete update failed to match — the suspected root cause.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TABLE_PATH = "s3a://payments-lake/transactions"


def build_spark() -> SparkSession:
    packages = ",".join([
        "io.delta:delta-spark_2.12:3.1.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "org.postgresql:postgresql:42.7.3",
    ])
    spark = (
        SparkSession.builder.appName("check_phantom")
        .master("local[2]")
        .config("spark.jars.packages", packages)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- sanity check: confirm the session actually used OUR packages ---
    effective = spark.sparkContext.getConf().get("spark.jars.packages", "<not set>")
    print(f"\n[sanity] effective spark.jars.packages = {effective}")
    if "postgresql" not in effective:
        print("[sanity] WARNING: postgresql driver missing from effective config!")
        print("[sanity] Check for a leftover PYSPARK_SUBMIT_ARGS env var, or run via:")
        print("[sanity]   spark-submit --packages ... scripts/check_phantom.py")

    return spark


def main() -> None:
    spark = build_spark()

    pg = (spark.read.format("jdbc")
          .option("url", "jdbc:postgresql://localhost:5432/payments")
          .option("dbtable", "transactions")
          .option("user", "payments")
          .option("password", "payments")
          .option("driver", "org.postgresql.Driver")
          .load()
          .select(F.col("transaction_id").cast("string").alias("transaction_id")))

    delta_all = spark.read.format("delta").load(TABLE_PATH)

    print("\n===== POSTGRES transaction_id schema =====")
    pg.select("transaction_id").printSchema()
    print("===== DELTA transaction_id schema =====")
    delta_all.select("transaction_id").printSchema()

    live = delta_all.filter(F.col("is_delete") == False)
    print(f"\n[counts] postgres={pg.count()}  delta_live={live.count()}  "
          f"delta_total={delta_all.count()}")

    phantom = (live.join(pg, "transaction_id", "left_anti")
               .select("transaction_id").limit(1).collect())

    if not phantom:
        print("\nNo phantoms found right now — table is clean on this dimension.")
        spark.stop()
        return

    tid = phantom[0]["transaction_id"]
    print(f"\nPHANTOM KEY: {tid!r}  len={len(tid)}")

    print("\n-- this key in Postgres (expect 0 rows if it was deleted upstream): --")
    pg.filter(F.col("transaction_id") == tid).show(truncate=False)

    print("-- ALL Delta rows for this key (watch is_delete, lsn): --")
    (delta_all.filter(F.col("transaction_id") == tid)
     .select("transaction_id", "lsn", "is_delete")
     .orderBy("lsn")
     .show(truncate=False))

    one = spark.createDataFrame([(tid,)], ["transaction_id"])
    matched = delta_all.join(one, "transaction_id").count()
    print(f"[match test] explicit join on this key returned {matched} Delta row(s). "
          f"If 0, the key does NOT compare equal → MERGE key bug confirmed.")

    spark.stop()


if __name__ == "__main__":
    main()