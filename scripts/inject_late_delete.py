#!/usr/bin/env python3

from __future__ import annotations

import json
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Project constants
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = "localhost:29092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
CDC_TOPIC = "payments.public.transactions"
SUBJECT = "payments.public.transactions-value"
DELTA_TABLE = "s3a://payments-lake/transactions"

# Settle windows sized for observed 40 s+ batches on this hardware
SETTLE_SECONDS = 60
CROSS_BATCH_PAUSE = 20
IDLE_QUIET_SECONDS = 15


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def check_streaming_job_alive() -> None:
    """Fail fast if the CDC job is not running."""
    for pattern in ("spark.jobs.payments_cdc_job", "payments_cdc_job.py"):
        result = subprocess.run(
            ["pgrep", "-f", pattern],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return
    print("FATAL: streaming job is not running.")
    print("Start it first: python -m spark.jobs.payments_cdc_job")
    sys.exit(1)


def assert_pipeline_idle(spark: SparkSession, quiet_seconds: int = IDLE_QUIET_SECONDS) -> None:
    """
    Fail fast if something else is committing to the transactions table.
    Empty micro-batches do NOT bump the Delta version (merge_batch returns
    early on n == 0 without executing the MERGE). That is why the
    delta_version() == 1 assertion in the intra-batch test is viable:
    only a real MERGE that touches rows advances the table version.
    """
    v1 = delta_version(spark)
    time.sleep(quiet_seconds)
    v2 = delta_version(spark)
    if v2 != v1:
        print(
            f"FATAL: table is not idle (version {v1} -> {v2} in {quiet_seconds}s)."
        )
        print("  Stop the generator and let the stream drain before running this suite.")
        sys.exit(1)
    print(f"Pipeline idle (Delta version stable at {v1} for {quiet_seconds}s).")


def new_tid() -> str:
    return str(uuid.uuid4())


def _get_avro_serializer() -> AvroSerializer:
    """
    Build an AvroSerializer against the live Schema Registry subject.
    The schema must already be registered by Debezium (Day 1 / Day 4).
    """
    sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    latest = sr.get_latest_version(SUBJECT)
    schema_str = latest.schema.schema_str
    return AvroSerializer(sr, schema_str)


def make_envelope(
    op: str,
    transaction_id: str,
    lsn: int,
    amount_minor: int = 1000,
    currency: str = "USD",
    merchant_id: str = "MERCH_001",
    status: str = "PENDING",
    event_type: str = "AUTHORIZATION",
) -> Dict[str, Any]:
    """
    Build a Debezium-shaped envelope that matches the registered Avro schema
    (currently version 2 after the Day-4 risk_score ADD COLUMN).

    IMPORTANT:
    - is_delete is DERIVED by the deserializer from `op == "d"`.
    - lsn is read from `source.lsn`, not from the after/before record.
    - All fields present in the live schema are supplied explicitly
      (including risk_score=None) so we never rely on fastavro defaults.
    """
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    # ZonedTimestamp-compatible string (io.debezium.time.ZonedTimestamp)
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    record = {
        "transaction_id": transaction_id,
        "merchant_id": merchant_id,
        "amount_minor": amount_minor,
        "currency": currency,
        "status": status,
        "event_type": event_type,
        "created_at": now_iso,
        "updated_at": now_iso,
        "risk_score": None,  # added by Day-4 mid-stream ALTER (schema v2)
    }

    source = {
        "version": "2.5.4.Final",
        "connector": "postgresql",
        "name": "payments",
        "ts_ms": now_ms,
        "snapshot": "false",
        "db": "payments",
        "sequence": None,
        "schema": "public",
        "table": "transactions",
        "txId": None,
        "lsn": lsn,
        "xmin": None,
    }

    if op == "d":
        return {
            "before": record,
            "after": None,
            "source": source,
            "op": "d",
            "ts_ms": now_ms,
            "transaction": None,
        }
    else:
        return {
            "before": None,
            "after": record,
            "source": source,
            "op": op,
            "ts_ms": now_ms,
            "transaction": None,
        }


def publish(
    producer: Producer,
    key: str,
    value: dict,
    serializer: AvroSerializer,
) -> None:
    """
    Publish a single Confluent-wire-format Avro record.

    NOTE: keys are raw UTF-8, not Avro-encoded like real Debezium keys
    (key.converter = AvroConverter, message.key.columns = transaction_id).
    The Spark job reads only `value`, so this is invisible to the pipeline.
    It does mean these records are not byte-identical to production traffic.
    """
    ctx = SerializationContext(CDC_TOPIC, MessageField.VALUE)
    payload = serializer(value, ctx)
    producer.produce(
        topic=CDC_TOPIC,
        key=key.encode("utf-8"),
        value=payload,
    )
    # flush() already polls until the queue drains; no extra poll(0) needed
    producer.flush(10)


def delta_version(spark: SparkSession) -> int:
    """Return the current highest Delta version of the transactions table."""
    row = (
        spark.sql(f"DESCRIBE HISTORY delta.`{DELTA_TABLE}`")
             .agg({"version": "max"})
             .collect()[0]
    )
    return int(row[0]) if row[0] is not None else -1


def query_delta(spark: SparkSession, transaction_id: str) -> Optional[Dict[str, Any]]:
    """
    Return the current row for transaction_id or None if absent.
    Asserts uniqueness – a silent duplicate would be a critical MERGE failure.
    """
    rows = (
        spark.read.format("delta")
             .load(DELTA_TABLE)
             .filter(F.col("transaction_id") == transaction_id)
             .select("transaction_id", "lsn", "is_delete", "amount_minor", "currency")
             .collect()
    )
    if len(rows) > 1:
        raise AssertionError(
            f"DUPLICATE ROWS for {transaction_id}: {len(rows)} – MERGE key violated"
        )
    if not rows:
        return None
    r = rows[0]
    return {
        "transaction_id": r["transaction_id"],
        "lsn": r["lsn"],
        "is_delete": r["is_delete"],
        "amount_minor": r["amount_minor"],
        "currency": r["currency"],
    }


def wait_settle(seconds: int = SETTLE_SECONDS) -> None:
    print(f"  … settling {seconds}s (observed batch times can exceed 40 s) …")
    time.sleep(seconds)


# ---------------------------------------------------------------------------
# Test 1 – intra-batch out-of-order
# ---------------------------------------------------------------------------
def test_intra_batch(
    spark: SparkSession,
    producer: Producer,
    serializer: AvroSerializer,
) -> bool:
    print("\n[1/2] Intra-batch late-arriving delete")
    tid = new_tid()
    print(f"  transaction_id = {tid}")

    v_before = delta_version(spark)

    # Arrival order (not LSN order) – all three must land in the SAME micro-batch
    publish(producer, tid, make_envelope("c", tid, lsn=100, amount_minor=1000), serializer)
    time.sleep(0.3)
    publish(producer, tid, make_envelope("d", tid, lsn=140), serializer)
    time.sleep(0.3)
    publish(producer, tid, make_envelope("u", tid, lsn=120, amount_minor=9999), serializer)

    wait_settle()

    v_after = delta_version(spark)
    if v_after - v_before != 1:
        print(
            f"  FAIL – intra-batch test requires exactly ONE Delta commit, "
            f"saw {v_after - v_before}. Events straddled multiple micro-batches. "
            "Re-run when the stream is fully caught up."
        )
        return False

    row = query_delta(spark, tid)
    print(f"  Delta row = {row}")

    # Soft-delete / tombstone policy: a never-seen key that is deleted
    # still materialises a tombstone so the LSN guard has a target.
    if row is None:
        print("  FAIL – row is completely absent (expected tombstone)")
        return False
    if row["is_delete"] is not True:
        print("  FAIL – is_delete is not True")
        return False
    if row["lsn"] != 140:
        print(f"  FAIL – expected lsn=140, got lsn={row['lsn']}")
        return False
    if row["amount_minor"] == 9999:
        print("  FAIL – late update resurrected / overwrote the row")
        return False

    print("  PASS – delete (lsn=140) won; late update did not resurrect")
    return True


# ---------------------------------------------------------------------------
# Test 2 – cross-batch (delete commits first, late update arrives later)
# ---------------------------------------------------------------------------
def test_cross_batch(
    spark: SparkSession,
    producer: Producer,
    serializer: AvroSerializer,
) -> bool:
    print("\n[2/2] Cross-batch late-arriving update after delete")
    tid = new_tid()
    print(f"  transaction_id = {tid}")

    # Batch A: insert only. Settle so it genuinely commits on its own.
    publish(producer, tid, make_envelope("c", tid, lsn=200, amount_minor=2000), serializer)
    wait_settle()
    seeded = query_delta(spark, tid)
    if seeded is None or seeded["lsn"] != 200:
        print(f"  FAIL – insert did not land as expected: {seeded}")
        return False
    print(f"  Insert confirmed: {seeded}")

    # Batch B: delete. Now a MATCHED update → tombstone via the LSN guard,
    # independent of whatever the whenNotMatched insert policy is.
    publish(producer, tid, make_envelope("d", tid, lsn=240), serializer)
    wait_settle()
    mid = query_delta(spark, tid)
    if mid is None or mid["is_delete"] is not True or mid["lsn"] != 240:
        print(f"  FAIL – delete did not land as tombstone: {mid}")
        return False
    print(f"  Tombstone confirmed: {mid}")

    # Batch C: the late, lower-LSN update that must NOT resurrect.
    print(f"  … publishing late update (lsn=220) after {CROSS_BATCH_PAUSE}s pause …")
    time.sleep(CROSS_BATCH_PAUSE)
    publish(producer, tid, make_envelope("u", tid, lsn=220, amount_minor=8888), serializer)
    wait_settle()

    final = query_delta(spark, tid)
    print(f"  Final Delta row = {final}")

    if final is None:
        print("  FAIL – row disappeared (unexpected)")
        return False
    if final["is_delete"] is not True:
        print("  FAIL – late update cleared is_delete (resurrection)")
        return False
    if final["lsn"] != 240:
        print(f"  FAIL – expected lsn to stay 240, got {final['lsn']}")
        return False
    if final["amount_minor"] == 8888:
        print("  FAIL – late update overwrote amount")
        return False

    print("  PASS – late update could not resurrect the tombstone")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    print("=" * 72)
    print("Day 5 – Late-Arriving Delete + Insert-After-Delete Gap Test")
    print("=" * 72)

    check_streaming_job_alive()
    print("Streaming job is alive.")

    # One SparkSession + one Avro serializer + one Kafka Producer for the whole suite
    from spark.jobs.payments_cdc_job import build_spark

    spark = build_spark("late-delete-check")
    serializer = _get_avro_serializer()
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    results: List[Tuple[str, bool]] = []
    try:
        # Fail fast if the table is still receiving commits from the generator
        # or a backlog. Empty batches do not advance the version (see docstring
        # of assert_pipeline_idle), so a stable version means a true quiet period.
        assert_pipeline_idle(spark)

        results.append(("INTRA_BATCH", test_intra_batch(spark, producer, serializer)))
        results.append(("CROSS_BATCH", test_cross_batch(spark, producer, serializer)))
    finally:
        producer.flush(10)
        spark.stop()

    print("\n" + "=" * 72)
    print(f"{'TEST':<16} RESULT")
    print("-" * 72)
    all_pass = True
    for name, passed in results:
        status = "PASS" if passed else "FAIL"
        if not passed:
            all_pass = False
        print(f"{name:<16} {status}")
    print("=" * 72)

    if all_pass:
        print("\nLATE-DELETE TESTS PASSED")
        print("Tombstone policy + LSN guard correctly prevented resurrection")
        print("in both intra-batch and cross-batch cases.")
        sys.exit(0)
    else:
        print("\nONE OR MORE LATE-DELETE TESTS FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()