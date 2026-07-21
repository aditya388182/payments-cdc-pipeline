# scripts/trace_merge.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark", "utils"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from spark.utils.avro_deserializer import deserialize_by_schema_id
from spark.jobs.payments_cdc_job import dedup_latest, merge_batch, TABLE_PATH, build_spark

KEY = "8d1653c8-e07b-4ac7-90d6-fa0a27903217"

spark = build_spark("trace_merge")

# Read the exact range that contains both offset 488 and 1411 on partition 2
# (Added partitions 0 and 1 to prevent Spark's partition assignment crash)
raw = (spark.read.format("kafka")
    .option("kafka.bootstrap.servers","localhost:29092")
    .option("subscribe","payments.public.transactions")
    .option("startingOffsets", '{"payments.public.transactions":{"0":0, "1":0, "2":480}}')
    .option("endingOffsets",   '{"payments.public.transactions":{"0":0, "1":0, "2":1420}}')
    .option("failOnDataLoss","false").load())

flat = deserialize_by_schema_id(raw)

print("=== raw events for key in this range ===")
flat.filter(F.col("transaction_id")==KEY).select("op","lsn","is_delete").orderBy("lsn").show(truncate=False)

staged = dedup_latest(flat)
print("=== after dedup_latest — what survives for this key ===")
staged.filter(F.col("transaction_id")==KEY).select("op","lsn","is_delete").show(truncate=False)

# fresh empty table to isolate: what does MERGE do with just this staged batch?
# (writes into the REAL table — run only if you're about to rebuild anyway)
merge_batch(spark, staged)

print("=== table state for key AFTER merge ===")
(spark.read.format("delta").load(TABLE_PATH)
    .filter(F.col("transaction_id")==KEY)
    .select("op" if "op" in spark.read.format("delta").load(TABLE_PATH).columns else "transaction_id",
            "lsn","is_delete").show(truncate=False))

spark.stop()