# scripts/trace_ordering.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark", "utils"))
from avro_deserializer import deserialize_by_schema_id

KEY = "8d1653c8-e07b-4ac7-90d6-fa0a27903217"

spark = (SparkSession.builder.appName("trace_ordering").master("local[2]")
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.apache.spark:spark-avro_2.12:3.5.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

raw = (spark.read.format("kafka")
    .option("kafka.bootstrap.servers","localhost:29092")
    .option("subscribe","payments.public.transactions")
    .option("startingOffsets","earliest").load())

flat = deserialize_by_schema_id(raw)
print("=== every event for the phantom key, in (partition, offset) order ===")
(flat.filter(F.col("transaction_id") == KEY)
     .select("partition","offset","op","lsn","is_delete")
     .orderBy("partition","offset")
     .show(truncate=False))
spark.stop()