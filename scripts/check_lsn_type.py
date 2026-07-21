# scripts/check_lsn_type.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark", "utils"))
from avro_deserializer import deserialize_by_schema_id

TABLE = "s3a://payments-lake/transactions"

spark = (SparkSession.builder.appName("check_lsn").master("local[2]")
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.apache.spark:spark-avro_2.12:3.5.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint","http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key","minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key","minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access","true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled","false")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

raw = (spark.read.format("kafka")
    .option("kafka.bootstrap.servers","localhost:29092")
    .option("subscribe","payments.public.transactions")
    .option("startingOffsets","earliest").load())

staged = deserialize_by_schema_id(raw)
print("=== STAGED (from Kafka) lsn dtype ===")
staged.select("lsn").printSchema()

target = spark.read.format("delta").load(TABLE)
print("=== TARGET (Delta table) lsn dtype ===")
target.select("lsn").printSchema()

spark.stop()