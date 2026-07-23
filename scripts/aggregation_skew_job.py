"""
scripts/aggregation_skew_job.py

Day-4 whale-skew demo consumer for the payments CDC project.

The MERGE path (payments_cdc_job.py) is PK-keyed on transaction_id and is
therefore already uniform -- skew never shows up there. Skew only appears
when you GROUP BY MERCHANT, so this is a separate batch job that reads the
Delta Change Data Feed and rolls up per-merchant totals.

Two modes:
  naive  : AQE off, plain groupBy(merchant_id) -- the whale merchant's key
           lands on a single task, producing the straggler you screenshot
           as 07_skew_before.png.
  salted : AQE on, skewJoin on, two-phase salted aggregation (N=8 buckets)
           -- screenshot as 08_skew_after.png.

Rule: salting belongs ONLY in this aggregation stage. Never salt the
MERGE path in payments_cdc_job.py, and never re-key the Kafka topic to
fix a Spark-side aggregation problem (see runbooks/hot_partition.md).
"""
import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, pmod, hash as _hash, sum as _sum, count as _count,
)

TABLE = "s3a://payments-lake/transactions"


def build_spark(naive: bool) -> SparkSession:
    builder = (
        SparkSession.builder.appName("aggregation_skew_job")
        .master("local[2]")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4")
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
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        )
    return builder.getOrCreate()


def naive_aggregate(df, n_partitions: int = 8):
    # A plain groupBy(...).agg(sum(...)) never shows the whale here: Spark
    # auto-applies map-side partial aggregation for associative functions
    # like sum/count, collapsing each partition's rows down to one tiny
    # partial-sum row BEFORE the shuffle. The reduce-side stage then only
    # ever handles a handful of partial sums (you'll see this as a tiny
    # "Shuffle Read Records" count, e.g. 42) -- flat task durations no
    # matter how skewed the raw data is.
    #
    # An explicit repartition on merchant_id -- matching
    # spark.sql.shuffle.partitions -- forces the real, unmerged shuffle:
    # Spark's planner sees the data is already correctly hash-partitioned
    # and skips inserting a second exchange, so the aggregate runs directly
    # against however many raw rows landed in each partition. The whale's
    # rows all land on ONE task, which now has to actually sum all of them
    # -- that's what produces the real straggler.
    repartitioned = df.repartition(n_partitions, "merchant_id")
    return repartitioned.groupBy("merchant_id").agg(
        _sum("amount_minor").alias("total_minor"),
        _count("*").alias("n"),
    )


def salted_aggregate(df, n_salt: int, seed: int, n_partitions: int = 8):
    # Deterministic hash-based salt rather than rand(): rand() is recomputed
    # after a shuffle changes partition indices, which would silently corrupt
    # the two-phase rollup below. hash(transaction_id) is stable across
    # shuffles and spreads uniformly.
    salted_input = df.withColumn("salt", pmod(_hash(col("transaction_id")), lit(n_salt)))

    # Same repartition trick as naive_aggregate, and for the same reason:
    # without it Spark pre-combines each partition down to tiny partial-sum
    # rows before the shuffle, and this stage looks flat whether or not the
    # salt did anything. Repartitioning on (merchant_id, salt) forces the raw
    # rows across the shuffle -- and because the whale's rows now carry
    # n_salt distinct salt values, they fan out across n_salt separate hash
    # buckets instead of piling onto one task. THAT is what the flat
    # distribution in this stage proves.
    repartitioned = salted_input.repartition(n_partitions, "merchant_id", "salt")

    first = repartitioned.groupBy("merchant_id", "salt").agg(
        _sum("amount_minor").alias("partial"), _count("*").alias("n_partial")
    )
    final = first.groupBy("merchant_id").agg(
        _sum("partial").alias("total_minor"),
        _sum("n_partial").alias("n"),
    )
    return final


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["naive", "salted"], required=True)
    ap.add_argument("--starting-version", type=int, default=0)
    ap.add_argument("--n-salt", type=int, default=8)
    ap.add_argument("--n-partitions", type=int, default=8,
                     help="Must match spark.sql.shuffle.partitions so Spark "
                          "skips a redundant second shuffle after repartition")
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
    result = (
        naive_aggregate(cdf, args.n_partitions)
        if args.mode == "naive"
        else salted_aggregate(cdf, args.n_salt, args.seed, args.n_partitions)
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