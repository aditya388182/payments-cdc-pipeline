import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, sum as _sum, count as _count

TABLE = "s3a://payments-lake/transactions"


def build_spark(naive: bool) -> SparkSession:
    builder = (
        SparkSession.builder.appName("aggregation_skew_job")
        .master("local[2]")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4") # changed 
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
    )
    if naive:
        builder = builder.config("spark.sql.adaptive.enabled", "false")
    else:
        builder = (
            builder
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        )
    return builder.getOrCreate()


def naive_aggregate(df):
    return df.groupBy("merchant_id").agg(
        _sum("amount_minor").alias("total_minor"),
        _count("*").alias("n"),
    )


def salted_aggregate(df, n_salt: int, seed: int):
    salted = (
        df.withColumn("salt", (rand(seed) * n_salt).cast("int"))
        .groupBy("merchant_id", "salt")
        .agg(_sum("amount_minor").alias("partial"), _count("*").alias("n_partial"))
    )
    final = salted.groupBy("merchant_id").agg(
        _sum("partial").alias("total_minor"),
        _sum("n_partial").alias("n"),
    )
    return final


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["naive", "salted"], required=True)
    ap.add_argument("--starting-version", type=int, default=0)
    ap.add_argument("--n-salt", type=int, default=8)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--hold-seconds", type=int, default=90)
    args = ap.parse_args()

    spark = build_spark(naive=(args.mode == "naive"))

    cdf = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", args.starting_version)
        .load(TABLE)
        .filter(col("_change_type").isin("insert", "update_postimage"))
    )

    t0 = time.time()
    result = naive_aggregate(cdf) if args.mode == "naive" else salted_aggregate(
        cdf, args.n_salt, args.seed
    )

    n_rows = result.count()  # forcing action -- every partition must fully run
    elapsed = time.time() - t0

    print(f"\n[{args.mode}] aggregated {n_rows} merchants in {elapsed:.2f}s\n")
    result.orderBy(col("total_minor").desc()).show(5, truncate=False)

    print(f"Holding driver open for {args.hold_seconds}s -- go screenshot "
          f"http://localhost:4040 now.")
    time.sleep(args.hold_seconds)
    spark.stop()


if __name__ == "__main__":
    main()
