# scripts/check_split_lifecycle.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark", "utils"))
from avro_deserializer import deserialize_by_schema_id

TABLE_PATH = "s3a://payments-lake/transactions"

spark = (SparkSession.builder.appName("check_split").master("local[2]")
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.apache.spark:spark-avro_2.12:3.5.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint","http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key","minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key","minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access","true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled","false")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

pg = (spark.read.format("jdbc")
      .option("url","jdbc:postgresql://localhost:5432/payments")
      .option("dbtable","transactions").option("user","payments")
      .option("password","payments").option("driver","org.postgresql.Driver")
      .load().select(F.col("transaction_id").cast("string").alias("transaction_id")))

delta_live = spark.read.format("delta").load(TABLE_PATH).filter(F.col("is_delete")==False)
extra = delta_live.join(pg, "transaction_id", "left_anti").select("transaction_id")
print("extra_in_delta count:", extra.count())

tid = extra.limit(1).collect()[0]["transaction_id"]
print("sample extra key:", tid)

# read a WIDE range so we can see this key's full history, well past offset 500
raw = (spark.read.format("kafka")
    .option("kafka.bootstrap.servers","localhost:29092")
    .option("subscribe","payments.public.transactions")
    .option("startingOffsets",'{"payments.public.transactions":{"0":0,"1":0,"2":0}}')
    .option("endingOffsets",  '{"payments.public.transactions":{"0":3000,"1":3000,"2":3000}}')
    .load())
flat = deserialize_by_schema_id(raw)
print(f"=== full history for {tid} across offsets 0-3000 ===")
flat.filter(F.col("transaction_id")==tid).select("partition","offset","op","lsn","is_delete").orderBy("offset").show(truncate=False)

spark.stop()