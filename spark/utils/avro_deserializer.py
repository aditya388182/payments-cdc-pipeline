from __future__ import annotations

import threading
from typing import Dict, List, Optional

import requests
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

REGISTRY_URL = "http://localhost:8081"
SUBJECT = "payments.public.transactions-value"

# Thread-safe in-memory cache: schema_id → schema JSON string
_schema_cache: Dict[int, str] = {}
_cache_lock = threading.Lock()


def latest_schema(subject: str = SUBJECT) -> str:
    """Fetch the latest schema JSON for a subject (used only by the legacy single-schema path)."""
    resp = requests.get(
        f"{REGISTRY_URL}/subjects/{subject}/versions/latest", timeout=10
    )
    resp.raise_for_status()
    return resp.json()["schema"]


def schema_by_id(schema_id: int) -> str:
    """Fetch schema JSON by ID with a process-wide thread-safe cache."""
    with _cache_lock:
        if schema_id in _schema_cache:
            return _schema_cache[schema_id]

    resp = requests.get(f"{REGISTRY_URL}/schemas/ids/{schema_id}", timeout=10)
    resp.raise_for_status()
    schema_json = resp.json()["schema"]

    with _cache_lock:
        _schema_cache[schema_id] = schema_json
    return schema_json


def flatten_envelope(decoded: DataFrame) -> DataFrame:
    """
    Flatten the Debezium CDC envelope.

    Critical Correctness Safeguard #2:
    Every business column uses coalesce(after, before) so that delete
    events (op='d', after=null) never lose their primary key or payload.
    """
    e = F.col("e")

    return (
        decoded.select(
            e.getField("op").alias("op"),
            # Core business columns – coalesce protects deletes
            F.coalesce(
                e.getField("after").getField("transaction_id"),
                e.getField("before").getField("transaction_id"),
            ).alias("transaction_id"),
            F.coalesce(
                e.getField("after").getField("merchant_id"),
                e.getField("before").getField("merchant_id"),
            ).alias("merchant_id"),
            F.coalesce(
                e.getField("after").getField("amount_minor"),
                e.getField("before").getField("amount_minor"),
            ).alias("amount_minor"),
            F.coalesce(
                e.getField("after").getField("currency"),
                e.getField("before").getField("currency"),
            ).alias("currency"),
            F.coalesce(
                e.getField("after").getField("status"),
                e.getField("before").getField("status"),
            ).alias("status"),
            F.coalesce(
                e.getField("after").getField("event_type"),
                e.getField("before").getField("event_type"),
            ).alias("event_type"),
            # Timestamps (microseconds → timestamp)
            F.coalesce(
                e.getField("after").getField("created_at"),
                e.getField("before").getField("created_at"),
            ).alias("created_at_raw"),
            F.coalesce(
                e.getField("after").getField("updated_at"),
                e.getField("before").getField("updated_at"),
            ).alias("updated_at_raw"),
            # Source metadata
            e.getField("source").getField("lsn").alias("lsn"),
            (e.getField("source").getField("ts_ms") / 1000.0)
            .cast("timestamp")
            .alias("source_ts"),
            # Kafka metadata carried forward
            F.col("partition"),
            F.col("offset"),
        )
        .withColumn(
            "created_at",
            F.when(
                F.col("created_at_raw").isNotNull(),
                (F.col("created_at_raw") / 1_000_000).cast("timestamp"),
            ),
        )
        .withColumn(
            "updated_at",
            F.when(
                F.col("updated_at_raw").isNotNull(),
                (F.col("updated_at_raw") / 1_000_000).cast("timestamp"),
            ),
        )
        .withColumn("is_delete", F.col("op") == "d")
        .drop("created_at_raw", "updated_at_raw")
    )


def _empty_result(spark) -> DataFrame:
    """Return a correctly-typed empty DataFrame matching the flattened schema."""
    schema = StructType(
        [
            StructField("op", StringType(), True),
            StructField("transaction_id", StringType(), True),
            StructField("merchant_id", StringType(), True),
            StructField("amount_minor", LongType(), True),
            StructField("currency", StringType(), True),
            StructField("status", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("lsn", LongType(), True),
            StructField("source_ts", TimestampType(), True),
            StructField("partition", IntegerType(), True),
            StructField("offset", LongType(), True),
            StructField("is_delete", BooleanType(), True),
        ]
    )
    return spark.createDataFrame([], schema)


def deserialize(df: DataFrame, value_schema_json: str) -> DataFrame:
    """
    Legacy single-schema path.
    Used only when explicitly requested; the production default is now
    deserialize_by_schema_id.
    """
    payload = df.withColumn(
        "avro_value",
        F.expr("substring(value, 6, length(value) - 5)"),
    )

    decoded = payload.select(
        from_avro(
            F.col("avro_value"),
            value_schema_json,
            {"mode": "PERMISSIVE"},
        ).alias("e"),
        F.col("partition"),
        F.col("offset"),
    )

    # Drop pure Kafka tombstones (null value)
    decoded = decoded.filter(F.col("e").isNotNull())

    return flatten_envelope(decoded)


def deserialize_by_schema_id(df: DataFrame) -> DataFrame:
    """
    Production path (Day-4).

    - Extracts the 4-byte Confluent schema ID from every message.
    - Groups the micro-batch by schema_id.
    - Deserializes each group with its exact writer schema.
    - Projects to the common column set the job understands.
    - Unknown fields introduced by a mid-stream ALTER TABLE are silently ignored.
    - Empty batches return a correctly-typed empty DataFrame.
    """
    with_id = df.withColumn(
        "schema_id",
        F.expr("conv(hex(substring(value, 2, 4)), 16, 10)").cast(IntegerType()),
    ).withColumn(
        "avro_value",
        F.expr("substring(value, 6, length(value) - 5)"),
    )

    # Collect distinct schema IDs present in this micro-batch
    schema_ids: List[int] = [
        row.schema_id
        for row in with_id.select("schema_id").distinct().collect()
        if row.schema_id is not None
    ]

    if not schema_ids:
        return _empty_result(df.sparkSession)

    frames: List[DataFrame] = []
    for sid in schema_ids:
        schema_json = schema_by_id(sid)
        subset = with_id.filter(F.col("schema_id") == sid)

        decoded = subset.select(
            from_avro(
                F.col("avro_value"),
                schema_json,
                {"mode": "PERMISSIVE"},
            ).alias("e"),
            F.col("partition"),
            F.col("offset"),
        ).filter(F.col("e").isNotNull())

        frames.append(flatten_envelope(decoded))

    result = frames[0]
    for f in frames[1:]:
        result = result.unionByName(f, allowMissingColumns=True)

    return result


if __name__ == "__main__":
    print("avro_deserializer.py loaded successfully (schema-id path is production default)")