from __future__ import annotations
from typing import Set, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column

# Constants
ALLOWED_CURRENCIES: Set[str] = {
    "USD", "EUR", "GBP", "JPY", "CAD",
    "AUD", "CHF", "SGD", "HKD", "INR",
}

# Strict UUID v4 regex
UUID4_REGEX = r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"

def _rule_amount(col_amount: Column, is_delete: Column) -> Column:
    return F.when((~is_delete) & (col_amount < 0), F.lit("NEGATIVE_AMOUNT"))

def _rule_currency(col_currency: Column, is_delete: Column) -> Column:
    return F.when(
        (~is_delete) & ~col_currency.isin(list(ALLOWED_CURRENCIES)),
        F.lit("INVALID_CURRENCY"),
    )

def _rule_uuid(col_tid: Column) -> Column:
    return F.when(
        col_tid.isNull() | ~col_tid.rlike(UUID4_REGEX),
        F.lit("INVALID_UUID"),
    )

def _rule_merchant(col_merchant: Column, merchant_ids: Set[str]) -> Column:
    return F.when(
        col_merchant.isNull() | ~col_merchant.isin(list(merchant_ids)),
        F.lit("UNKNOWN_MERCHANT"),
    )

def validate(df: DataFrame, merchant_ids: Set[str]) -> Tuple[DataFrame, DataFrame]:
    if not merchant_ids:
        raise ValueError("merchant_ids set must not be empty")

    is_delete = F.coalesce(F.col("is_delete"), F.lit(False))

    checks = (
        df.withColumn("r_amount", _rule_amount(F.col("amount_minor"), is_delete))
          .withColumn("r_currency", _rule_currency(F.col("currency"), is_delete))
          .withColumn("r_uuid", _rule_uuid(F.col("transaction_id")))
          .withColumn("r_merchant", _rule_merchant(F.col("merchant_id"), merchant_ids))
          .withColumn(
              "rejection_reason",
              F.concat_ws(
                  ",",
                  F.col("r_amount"),
                  F.col("r_currency"),
                  F.col("r_uuid"),
                  F.col("r_merchant"),
              ),
          )
    )

    helper_cols = ["r_amount", "r_currency", "r_uuid", "r_merchant"]

    valid = (
        checks.filter(F.col("rejection_reason") == "")
              .drop("rejection_reason", *helper_cols)
    )

    invalid = (
        checks.filter(F.col("rejection_reason") != "")
              .drop(*helper_cols)
    )

    return valid, invalid