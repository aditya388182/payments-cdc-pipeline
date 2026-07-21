from pyspark.sql import functions as F

pg = (spark.read.format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/payments")
      .option("dbtable", "transactions").option("user", "payments")
      .option("password", "payments").option("driver", "org.postgresql.Driver")
      .load().select(F.col("transaction_id").cast("string").alias("transaction_id")))

delta_all = spark.read.format("delta").load("s3a://payments-lake/transactions")

# dtypes on both sides — must BOTH be string
pg.select("transaction_id").printSchema()
delta_all.select("transaction_id").printSchema()

# grab one phantom: live in delta, absent in postgres
phantom = (delta_all.filter(F.col("is_delete") == False)
           .join(pg, "transaction_id", "left_anti")
           .select("transaction_id").limit(1).collect())

if phantom:
    tid = phantom[0]["transaction_id"]
    print("PHANTOM KEY:", repr(tid), "len:", len(tid))
    print("-- this key in Postgres (expect 0 rows if it was deleted): --")
    pg.filter(F.col("transaction_id") == tid).show(truncate=False)
    print("-- all Delta rows for this key (look at is_delete + count): --")
    delta_all.filter(F.col("transaction_id") == tid).show(truncate=False)
else:
    print("no phantoms found right now")