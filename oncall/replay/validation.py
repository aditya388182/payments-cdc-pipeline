"""Adapter around the pipeline's EXISTING validator (spark/utils/validator.validate).

The validator is a Spark DataFrame function, so a tiny local Spark session evaluates a one-row
DataFrame with the same rules the streaming job applies (no copy of the rules lives here). The
merchant registry is the pipeline's own MerchantRegistry, force-refreshed on every call so a merchant
added a second ago is visible.
"""
from __future__ import annotations

import threading

from .models import DlqEntry

_SCHEMA = "transaction_id STRING, merchant_id STRING, amount_minor BIGINT, currency STRING, is_delete BOOLEAN"


class SparkRecordValidator:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._spark = None

    def warm_up(self) -> None:
        import sys

        from pyspark.sql import SparkSession

        with self._lock:
            if self._spark is None:
                self._spark = (
                    SparkSession.builder.appName("replay-svc-validate")
                    .master("local[1]")
                    # Python workers must be the same interpreter as this process. Without it Spark picks `python3` from
                    # PATH (3.12 on the box) and aborts with PYTHON_VERSION_MISMATCH (found by the Playwright run).
                    .config("spark.pyspark.python", sys.executable)
                    .config("spark.pyspark.driver.python", sys.executable)
                    .config("spark.ui.enabled", "false")
                    .config("spark.driver.bindAddress", "127.0.0.1")
                    .config("spark.sql.shuffle.partitions", "1")
                    .config("spark.sql.session.timeZone", "UTC")
                    .getOrCreate()
                )
                self._spark.sparkContext.setLogLevel("ERROR")

    def reasons(self, entry: DlqEntry) -> list[str]:
        from spark.utils.dlq_writer import get_merchant_registry
        from spark.utils.validator import validate

        self.warm_up()
        registry = get_merchant_registry()
        registry.refresh(force=True)
        with self._lock:
            df = self._spark.createDataFrame(
                [(entry.transaction_id, entry.merchant_id, entry.amount_minor, entry.currency, entry.op == "d")],
                schema=_SCHEMA,
            )
            _valid, invalid = validate(df, registry.merchant_ids)
            rows = invalid.select("rejection_reason").collect()
        if not rows:
            return []
        return [r for r in rows[0]["rejection_reason"].split(",") if r]
