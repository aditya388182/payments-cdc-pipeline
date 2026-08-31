from __future__ import annotations
import argparse
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from spark.utils.avro_deserializer import deserialize_by_schema_id
from spark.utils.validator import validate
from spark.utils.dlq_writer import write_to_dlq, merchants, get_merchant_registry
from spark.utils.metrics_listener import push_metrics, extract_processed_offsets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("payments.cdc")

KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "payments.public.transactions"
CHECKPOINT = "s3a://payments-lake/checkpoints/payments-cdc"
TABLE_PATH = "s3a://payments-lake/transactions"
LEDGER_PATH = "s3a://payments-lake/_batch_ledger"
APP_ID = "payments_cdc_v1"

def build_spark(app_name: str = "payments-cdc") -> SparkSession:
    packages = ",".join([
        "io.delta:delta-spark_2.12:3.1.0",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "org.apache.spark:spark-avro_2.12:3.5.1",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.postgresql:postgresql:42.7.3",
    ])
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config("spark.jars.packages", packages)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
    )
    return builder.getOrCreate()

def already_committed(spark: SparkSession, batch_id: int) -> bool:
    if not DeltaTable.isDeltaTable(spark, LEDGER_PATH):
        return False
    ledger = spark.read.format("delta").load(LEDGER_PATH)
    return ledger.filter((F.col("app_id") == APP_ID) & (F.col("batch_id") == batch_id)).count() > 0

def commit_ledger(spark: SparkSession, batch_id: int, commit: bool = True) -> bool:
    if not commit:
        return False
    df = spark.createDataFrame([(APP_ID, int(batch_id), datetime.now(timezone.utc))], schema="app_id STRING, batch_id BIGINT, committed_at TIMESTAMP")
    df.write.format("delta").mode("append").save(LEDGER_PATH)
    return True

def dedup_latest(df: DataFrame) -> DataFrame:
    order = [F.col("lsn").desc()]
    if "offset" in df.columns:
        order.append(F.col("offset").desc())
    w = Window.partitionBy("transaction_id").orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

def merge_batch(spark: SparkSession, staged: DataFrame) -> int:
    n = staged.count()
    if n == 0: return 0
    delta_table = DeltaTable.forPath(spark, TABLE_PATH)
    delta_table.alias("target").merge(staged.alias("staged"), "target.transaction_id = staged.transaction_id") \
        .whenMatchedUpdateAll(condition="staged.lsn > target.lsn") \
        .whenNotMatchedInsertAll().execute()
    return n

def process_batch(batch_df: DataFrame, batch_id: int, commit: bool = True) -> None:
    spark = batch_df.sparkSession
    if already_committed(spark, batch_id):
        return
    batch_df.persist()
    try:
        t0 = time.time()
        n_in = batch_df.count()
        if n_in == 0:
            commit_ledger(spark, batch_id, commit=commit)
            return

        valid, invalid = validate(batch_df, merchants())
        valid.persist()
        invalid.persist()
        try:
            n_dlq = write_to_dlq(invalid)
            staged = dedup_latest(valid)
            n_merged = merge_batch(spark, staged)
            committed = commit_ledger(spark, batch_id, commit=commit)

            batch_seconds = time.time() - t0
            
            if committed:
                last_commit_df = spark.read.format("delta").load(LEDGER_PATH).filter(F.col("app_id") == APP_ID).orderBy(F.col("batch_id").desc()).limit(1).collect()
                last_ts = last_commit_df[0]["committed_at"].timestamp() if last_commit_df else time.time()
                push_metrics(batch_id, n_in, n_merged, n_dlq, batch_seconds, last_ts, {})
                
            logger.info("batch_id=%s finished - in=%d merged=%d dlq=%d", batch_id, n_in, n_merged, n_dlq)
        finally:
            valid.unpersist()
            invalid.unpersist()
    finally:
        batch_df.unpersist()

def foreach_batch_wrapper(raw_batch: DataFrame, batch_id: int) -> None:
    raw_batch.persist()
    try:
        if raw_batch.count() == 0: return
        flat = deserialize_by_schema_id(raw_batch)
        process_batch(flat, batch_id, commit=True)
    finally:
        raw_batch.unpersist()

global_query = None

def run_stream(starting_offsets: str = "earliest") -> None:
    global global_query
    spark = build_spark()
    reg = get_merchant_registry()
    raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP).option("subscribe", TOPIC).option("startingOffsets", starting_offsets).option("failOnDataLoss", "false").load()
    global_query = raw.writeStream.foreachBatch(foreach_batch_wrapper).option("checkpointLocation", CHECKPOINT).trigger(processingTime="10 seconds").start()
    logger.info("Streaming query started. Awaiting termination...")
    global_query.awaitTermination()

def main() -> None:
    parser = argparse.ArgumentParser(description="Payments CDC Streaming job")
    parser.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
    args = parser.parse_args()

    def handle_sigterm(signum, frame):
        logger.info("Received SIGTERM! Stopping query gracefully...")
        if global_query: global_query.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    
    try:
        run_stream(starting_offsets=args.starting_offsets)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt! Stopping query gracefully...")
        if global_query: global_query.stop()

if __name__ == "__main__":
    main()