import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("dedup-test").getOrCreate()

def apply_dedup(df):
    """Replicates the CDC deduplication window logic"""
    window_spec = Window.partitionBy("transaction_id").orderBy(
        F.col("lsn").desc(), F.col("updated_at").desc()
    )
    return df.withColumn("rn", F.row_number().over(window_spec)) \
             .filter(F.col("rn") == 1).drop("rn")

def test_in_order(spark):
    df = spark.createDataFrame([
        Row(transaction_id="1", lsn=100, updated_at=1000, is_delete=False),
        Row(transaction_id="1", lsn=110, updated_at=1100, is_delete=False)
    ])
    res = apply_dedup(df).collect()
    assert len(res) == 1
    assert res[0].lsn == 110

def test_out_of_order(spark):
    df = spark.createDataFrame([
        Row(transaction_id="2", lsn=120, updated_at=1200, is_delete=False),
        Row(transaction_id="2", lsn=110, updated_at=1100, is_delete=False)
    ])
    res = apply_dedup(df).collect()
    assert len(res) == 1
    assert res[0].lsn == 120

def test_duplicate_lsn_determinism(spark):
    df = spark.createDataFrame([
        Row(transaction_id="3", lsn=130, updated_at=1300, is_delete=False),
        Row(transaction_id="3", lsn=130, updated_at=1305, is_delete=False)
    ])
    res = apply_dedup(df).collect()
    assert len(res) == 1
    assert res[0].updated_at == 1305

def test_delete_before_update_trio(spark):
    df = spark.createDataFrame([
        Row(transaction_id="4", lsn=100, updated_at=1000, is_delete=False), # c@100
        Row(transaction_id="4", lsn=140, updated_at=1400, is_delete=True),  # d@140
        Row(transaction_id="4", lsn=120, updated_at=1200, is_delete=False)  # u@120 (late)
    ])
    res = apply_dedup(df).collect()
    assert len(res) == 1
    assert res[0].lsn == 140
    assert res[0].is_delete == True