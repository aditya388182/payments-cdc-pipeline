from __future__ import annotations

import uuid

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import types as T

from spark.jobs.payments_cdc_job import dedup_latest


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-dedup")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield session
    session.stop()


SCHEMA = T.StructType(
    [
        T.StructField("transaction_id", T.StringType(), False),
        T.StructField("merchant_id", T.StringType(), True),
        T.StructField("amount_minor", T.LongType(), True),
        T.StructField("currency", T.StringType(), True),
        T.StructField("status", T.StringType(), True),
        T.StructField("lsn", T.LongType(), False),
        T.StructField("is_delete", T.BooleanType(), False),
        T.StructField("op", T.StringType(), True),
        T.StructField("offset", T.LongType(), True),
    ]
)


def _tid() -> str:
    return str(uuid.uuid4())


def _rows(spark, records):
    return spark.createDataFrame(records, schema=SCHEMA)


def test_in_order_keeps_highest_lsn(spark):
    tid = _tid()
    df = _rows(spark, [
        Row(tid, "MERCH_001", 100, "USD", "PENDING", 10, False, "c", 1),
        Row(tid, "MERCH_001", 100, "USD", "SETTLED", 20, False, "u", 2),
        Row(tid, "MERCH_001", 100, "USD", "SETTLED", 30, False, "u", 3),
    ])
    out = dedup_latest(df).collect()
    assert len(out) == 1 and out[0]["lsn"] == 30 and out[0]["status"] == "SETTLED"


def test_out_of_order_still_keeps_highest_lsn(spark):
    tid = _tid()
    df = _rows(spark, [
        Row(tid, "MERCH_001", 100, "USD", "SETTLED", 30, False, "u", 3),
        Row(tid, "MERCH_001", 100, "USD", "PENDING", 10, False, "c", 1),
        Row(tid, "MERCH_001", 250, "USD", "SETTLED", 40, False, "u", 4),
        Row(tid, "MERCH_001", 100, "USD", "SETTLED", 20, False, "u", 2),
    ])
    out = dedup_latest(df).collect()
    assert len(out) == 1 and out[0]["lsn"] == 40 and out[0]["amount_minor"] == 250


def test_duplicate_lsn_is_deterministic_one_survivor(spark):
    tid = _tid()
    df = _rows(spark, [
        Row(tid, "MERCH_001", 100, "USD", "PENDING", 50, False, "u", 10),
        Row(tid, "MERCH_001", 999, "EUR", "FAILED", 50, False, "u", 11),
        Row(tid, "MERCH_001", 100, "USD", "PENDING", 50, False, "u", 10),
    ])
    out = dedup_latest(df).collect()
    assert len(out) == 1 and out[0]["lsn"] == 50 and out[0]["offset"] == 11
    assert out[0]["amount_minor"] == dedup_latest(df).collect()[0]["amount_minor"]


def test_independent_keys_do_not_collapse_together(spark):
    a, b = _tid(), _tid()
    df = _rows(spark, [
        Row(a, "MERCH_001", 100, "USD", "PENDING", 1, False, "c", 1),
        Row(b, "MERCH_002", 200, "EUR", "PENDING", 2, False, "c", 2),
        Row(a, "MERCH_001", 100, "USD", "SETTLED", 3, False, "u", 3),
    ])
    out = {r["transaction_id"]: r for r in dedup_latest(df).collect()}
    assert set(out) == {a, b}
    assert out[a]["lsn"] == 3 and out[b]["lsn"] == 2


def test_delete_before_update_trio_lands_on_delete(spark):
    tid = _tid()
    df = _rows(spark, [
        Row(tid, "MERCH_001", 100, "USD", "PENDING", 100, False, "c", 1),
        Row(tid, "MERCH_001", 100, "USD", "PENDING", 140, True, "d", 2),
        Row(tid, "MERCH_001", 150, "USD", "SETTLED", 120, False, "u", 3),
    ])
    out = dedup_latest(df).collect()
    assert len(out) == 1 and out[0]["lsn"] == 140 and out[0]["is_delete"] is True


def test_empty_batch(spark):
    assert dedup_latest(spark.createDataFrame([], schema=SCHEMA)).collect() == []