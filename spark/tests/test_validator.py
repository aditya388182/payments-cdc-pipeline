from __future__ import annotations

import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType
)

from spark.utils.validator import validate

# Fixtures
@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-validator")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount_minor", LongType(), True),
    StructField("currency", StringType(), True),
    StructField("is_delete", BooleanType(), True),
    StructField("lsn", LongType(), True),
])

KNOWN_MERCHANTS = {"MERCH_001", "MERCH_007", "MERCH_017"}


def make_df(spark: SparkSession, rows: list[dict]):
    return spark.createDataFrame([Row(**r) for r in rows], schema=SCHEMA)


# 1. Negative amount boundary
def test_negative_amount(spark: SparkSession):
    df = make_df(spark, [{
        "transaction_id": "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d",
        "merchant_id": "MERCH_001",
        "amount_minor": -1,
        "currency": "USD",
        "is_delete": False,
        "lsn": 100,
    }])
    valid, invalid = validate(df, KNOWN_MERCHANTS)
    assert valid.count() == 0
    assert invalid.count() == 1
    assert "NEGATIVE_AMOUNT" in invalid.collect()[0]["rejection_reason"]


# 2. Zero amount boundary
def test_zero_amount_passes(spark: SparkSession):
    df = make_df(spark, [{
        "transaction_id": "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d",
        "merchant_id": "MERCH_001",
        "amount_minor": 0,
        "currency": "USD",
        "is_delete": False,
        "lsn": 100,
    }])
    valid, invalid = validate(df, KNOWN_MERCHANTS)
    assert valid.count() == 1
    assert invalid.count() == 0


# 3. Invalid currency
def test_invalid_currency(spark: SparkSession):
    df = make_df(spark, [{
        "transaction_id": "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d",
        "merchant_id": "MERCH_001",
        "amount_minor": 1000,
        "currency": "XYZ",
        "is_delete": False,
        "lsn": 101,
    }])
    valid, invalid = validate(df, KNOWN_MERCHANTS)
    assert valid.count() == 0
    assert invalid.count() == 1
    assert "INVALID_CURRENCY" in invalid.collect()[0]["rejection_reason"]


# 4. Valid currency passes
def test_valid_currency_passes(spark: SparkSession):
    df = make_df(spark, [{
        "transaction_id": "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d",
        "merchant_id": "MERCH_001",
        "amount_minor": 1000,
        "currency": "EUR",
        "is_delete": False,
        "lsn": 101,
    }])
    valid, invalid = validate(df, KNOWN_MERCHANTS)
    assert valid.count() == 1
    assert invalid.count() == 0


# 5. Invalid UUID format
def test_invalid_uuid(spark: SparkSession):
    df = make_df(spark, [{
        "transaction_id": "not-a-uuid-at-all",
        "merchant_id": "MERCH_001",
        "amount_minor": 1000,
        "currency": "USD",
        "is_delete": False,
        "lsn": 102,
    }])
    valid, invalid = validate(df, KNOWN_MERCHANTS)
    assert valid.count() == 0
    assert invalid.count() == 1
    assert "INVALID_UUID" in invalid.collect()[0]["rejection_reason"]


# 6. Null UUID
def test_null_uuid_rejected(spark: SparkSession):
    df = make_df(spark, [{
        "transaction_id": None,
        "merchant_id": "MERCH_001",
        "amount_minor": 1000,
        "currency": "USD",
        "is_delete": False,
        "lsn": 102,
    }])
    valid, invalid = validate(df, KNOWN_MERCHANTS)
    assert valid.count() == 0
    assert invalid.count() == 1
    assert "INVALID_UUID" in invalid.collect()[0]["rejection_reason"]


# 7. Unknown merchant
def test_unknown_merchant(spark: SparkSession):
    df = make_df(spark, [{
        "transaction_id": "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d",
        "merchant_id": "GHOST_MERCHANT",
        "amount_minor": 1000,
        "currency": "USD",
        "is_delete": False,
        "lsn": 103,
    }])
    valid, invalid = validate(df, KNOWN_MERCHANTS)
    assert valid.count() == 0
    assert invalid.count() == 1
    assert "UNKNOWN_MERCHANT" in invalid.collect()[0]["rejection_reason"]


# 8. All pass
def test_all_rules_pass(spark: SparkSession):
    df = make_df(spark, [{
        "transaction_id": "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d",
        "merchant_id": "MERCH_001",
        "amount_minor": 2500,
        "currency": "USD",
        "is_delete": False,
        "lsn": 104,
    }])
    valid, invalid = validate(df, KNOWN_MERCHANTS)
    assert valid.count() == 1
    assert invalid.count() == 0


# 9. Fail-closed empty merchant set
def test_empty_merchant_set_raises(spark: SparkSession):
    df = make_df(spark, [{
        "transaction_id": "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d",
        "merchant_id": "MERCH_001",
        "amount_minor": 1000,
        "currency": "USD",
        "is_delete": False,
        "lsn": 100,
    }])
    with pytest.raises(ValueError, match="must not be empty"):
        validate(df, set())


# 10. NULL is_delete coalesce
def test_null_is_delete_treated_as_not_deleted(spark: SparkSession):
    df = make_df(spark, [{
        "transaction_id": "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d",
        "merchant_id": "MERCH_001",
        "amount_minor": -500,
        "currency": "USD",
        "is_delete": None,
        "lsn": 100,
    }])
    valid, invalid = validate(df, KNOWN_MERCHANTS)
    assert valid.count() == 0
    assert invalid.count() == 1
    assert "NEGATIVE_AMOUNT" in invalid.collect()[0]["rejection_reason"]


# 11. Mixed-batch test
def test_mixed_batch(spark: SparkSession):
    rows = [
        {
            "transaction_id": "11111111-1111-4111-8111-111111111111",
            "merchant_id": "MERCH_001",
            "amount_minor": 1000,
            "currency": "USD",
            "is_delete": False,
            "lsn": 200,
        },
        {
            "transaction_id": "22222222-2222-4222-8222-222222222222",
            "merchant_id": "MERCH_001",
            "amount_minor": -100,
            "currency": "USD",
            "is_delete": False,
            "lsn": 201,
        },
        {
            "transaction_id": "33333333-3333-4333-8333-333333333333",
            "merchant_id": "MERCH_001",
            "amount_minor": 1000,
            "currency": "XYZ",
            "is_delete": False,
            "lsn": 202,
        },
        {
            "transaction_id": "44444444-4444-4444-8444-444444444444",
            "merchant_id": "GHOST",
            "amount_minor": 1000,
            "currency": "EUR",
            "is_delete": False,
            "lsn": 203,
        },
    ]
    df = make_df(spark, rows)
    valid, invalid = validate(df, KNOWN_MERCHANTS)

    assert valid.count() + invalid.count() == df.count()
    assert valid.count() == 1
    assert invalid.count() == 3

    reasons = {r["transaction_id"]: r["rejection_reason"] for r in invalid.collect()}
    assert "NEGATIVE_AMOUNT" in reasons["22222222-2222-4222-8222-222222222222"]
    assert "INVALID_CURRENCY" in reasons["33333333-3333-4333-8333-333333333333"]
    assert "UNKNOWN_MERCHANT" in reasons["44444444-4444-4444-8444-444444444444"]


# 12. Delete carve-out
def test_delete_exempt_from_amount_and_currency(spark: SparkSession):
    df = make_df(spark, [{
        "transaction_id": "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d",
        "merchant_id": "MERCH_001",
        "amount_minor": -9999,
        "currency": "XYZ",
        "is_delete": True,
        "lsn": 300,
    }])
    valid, invalid = validate(df, KNOWN_MERCHANTS)
    assert valid.count() == 1
    assert invalid.count() == 0


# 13. Valid delete passes
def test_valid_delete_passes(spark: SparkSession):
    df = make_df(spark, [{
        "transaction_id": "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d",
        "merchant_id": "MERCH_001",
        "amount_minor": 5000,
        "currency": "EUR",
        "is_delete": True,
        "lsn": 300,
    }])
    valid, invalid = validate(df, KNOWN_MERCHANTS)
    assert valid.count() == 1
    assert invalid.count() == 0


# 14. Delete rejects bad uuid
def test_delete_still_rejects_bad_uuid(spark: SparkSession):
    df = make_df(spark, [{
        "transaction_id": "bad-uuid",
        "merchant_id": "MERCH_001",
        "amount_minor": -9999,
        "currency": "XYZ",
        "is_delete": True,
        "lsn": 301,
    }])
    valid, invalid = validate(df, KNOWN_MERCHANTS)
    assert valid.count() == 0
    assert invalid.count() == 1
    assert "INVALID_UUID" in invalid.collect()[0]["rejection_reason"]


# 15. Delete rejects unknown merchant
def test_delete_still_rejects_unknown_merchant(spark: SparkSession):
    df = make_df(spark, [{
        "transaction_id": "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d",
        "merchant_id": "GHOST_MERCHANT",
        "amount_minor": -9999,
        "currency": "XYZ",
        "is_delete": True,
        "lsn": 302,
    }])
    valid, invalid = validate(df, KNOWN_MERCHANTS)
    assert valid.count() == 0
    assert invalid.count() == 1
    assert "UNKNOWN_MERCHANT" in invalid.collect()[0]["rejection_reason"]


# 16. Multiple failures collected
def test_multiple_failures_collected(spark: SparkSession):
    df = make_df(spark, [{
        "transaction_id": "bad-uuid",
        "merchant_id": "GHOST",
        "amount_minor": -100,
        "currency": "XYZ",
        "is_delete": False,
        "lsn": 400,
    }])
    valid, invalid = validate(df, KNOWN_MERCHANTS)
    assert valid.count() == 0
    assert invalid.count() == 1
    reason = invalid.collect()[0]["rejection_reason"]
    assert "NEGATIVE_AMOUNT" in reason
    assert "INVALID_CURRENCY" in reason
    assert "INVALID_UUID" in reason
    assert "UNKNOWN_MERCHANT" in reason


# 17. Output schema contracts
def test_output_schemas(spark: SparkSession):
    rows = [
        {
            "transaction_id": "11111111-1111-4111-8111-111111111111",
            "merchant_id": "MERCH_001",
            "amount_minor": 1000,
            "currency": "USD",
            "is_delete": False,
            "lsn": 200,
        },
        {
            "transaction_id": "22222222-2222-4222-8222-222222222222",
            "merchant_id": "MERCH_001",
            "amount_minor": -100,
            "currency": "USD",
            "is_delete": False,
            "lsn": 201,
        },
    ]
    valid, invalid = validate(make_df(spark, rows), KNOWN_MERCHANTS)

    helpers = {"r_amount", "r_currency", "r_uuid", "r_merchant"}

    assert not helpers & set(valid.columns)
    assert "rejection_reason" not in valid.columns
    assert set(valid.columns) == set(SCHEMA.fieldNames())

    assert not helpers & set(invalid.columns)
    assert "rejection_reason" in invalid.columns