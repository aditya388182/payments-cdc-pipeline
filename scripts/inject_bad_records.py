#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import psycopg2
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from pyspark.sql import functions as F


PG_DSN = {
    "host": "localhost",
    "port": 5432,
    "dbname": "payments",
    "user": "payments",
    "password": "payments",
}
SCHEMA_REGISTRY_URL = "http://localhost:8081"
SUBJECT = "payments.public.transactions-value"
KAFKA_BOOTSTRAP = "localhost:29092"
CDC_TOPIC = "payments.public.transactions"
DLQ_TOPIC = "payments.transactions.dlq"

KNOWN_MERCHANT = "MERCH_007"
GHOST_MERCHANT = "MERCH_GHOST_000"
VALID_CURRENCY = "USD"
INVALID_CURRENCY = "XYZ"
VALID_STATUS = "PENDING"
VALID_EVENT_TYPE = "AUTHORIZATION"

SETTLE_SECONDS = 25.0
POLL_SECONDS = 90.0
POLL_INTERVAL = 2.0
DLQ_CONSUME_SECONDS = 12.0

ID_DUMP = Path("docs/day5_logs/injected_ids.json")


def check_streaming_job_alive() -> None:
    result = subprocess.run(
        ["pgrep", "-af", "spark.jobs.payments_cdc_job"],
        capture_output=True,
        text=True,
        check=False,
    )
    lines = [
        ln
        for ln in result.stdout.splitlines()
        if "spark.jobs.payments_cdc_job" in ln and "pgrep" not in ln
    ]
    if not lines:
        raise SystemExit("FATAL: streaming job is not running.")
    print("Streaming job is alive.")


def pg_conn():
    return psycopg2.connect(**PG_DSN)


def pg_execute(sql: str, params: Optional[Sequence[Any]] = None) -> None:
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def pg_query(sql: str, params: Optional[Sequence[Any]] = None) -> List[tuple]:
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall())
    finally:
        conn.close()


def assert_transaction_id_is_uuid() -> None:
    rows = pg_query(
        """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'transactions'
          AND column_name  = 'transaction_id'
        """
    )
    if not rows:
        raise SystemExit("FATAL: transactions.transaction_id column not found")
    if rows[0][0] != "uuid":
        raise SystemExit("FATAL: transaction_id is not uuid.")
    print("Precondition OK: transaction_id is still UUID.")


def insert_row(
    transaction_id: str,
    merchant_id: str,
    amount_minor: int,
    currency: str,
    status: str = VALID_STATUS,
    event_type: str = VALID_EVENT_TYPE,
) -> None:
    pg_execute(
        """
        INSERT INTO transactions (
            transaction_id, merchant_id, amount_minor, currency, status, event_type
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (transaction_id, merchant_id, amount_minor, currency, status, event_type),
    )


def cleanup_injected(all_bad_ids: List[str]) -> None:
    if not all_bad_ids:
        return
    pg_execute("DELETE FROM transactions WHERE transaction_id = ANY(%s)", (all_bad_ids,))
    print(f"cleanup_injected: deleted {len(all_bad_ids)} Postgres rows")


def _get_avro_serializer() -> AvroSerializer:
    sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    latest = sr.get_latest_version(SUBJECT)
    return AvroSerializer(
        sr,
        latest.schema.schema_str,
        conf={"auto.register.schemas": False, "use.latest.version": True},
    )


def _envelope(
    *, op: str, transaction_id: str, merchant_id: str, amount_minor: int, currency: str, lsn: int
) -> Dict[str, Any]:
    now_ms = int(time.time() * 1000)
    # Debezium ZonedTimestamp expects string format, not integer!
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    
    row = {
        "transaction_id": transaction_id,
        "merchant_id": merchant_id,
        "amount_minor": amount_minor,
        "currency": currency,
        "status": VALID_STATUS,
        "event_type": VALID_EVENT_TYPE,
        "created_at": now_str,
        "updated_at": now_str,
        "risk_score": None,
    }
    source = {
        "version": "2.5.4.Final",
        "connector": "postgresql",
        "name": "payments",
        "ts_ms": now_ms,
        "snapshot": "false",
        "db": "payments",
        "sequence": f'["{lsn}","{lsn}"]',
        "schema": "public",
        "table": "transactions",
        "txId": 1,
        "lsn": lsn,
        "xmin": None,
    }
    after = None if op == "d" else row
    before = row if op in ("d", "u") else None
    return {
        "before": before,
        "after": after,
        "source": source,
        "op": op,
        "ts_ms": now_ms,
        "transaction": None,
    }


def _try_serialize(serializer: AvroSerializer, envelope: Dict[str, Any]) -> bytes:
    ctx = SerializationContext(CDC_TOPIC, MessageField.VALUE)
    try:
        payload = serializer(envelope, ctx)
    except Exception as exc:
        raise SystemExit(f"FATAL: rogue envelope does not match registered schema ({exc}).") from exc
    if payload is None:
        raise SystemExit("FATAL: AvroSerializer returned None")
    return payload


def publish_rogue_avro(
    transaction_id: str,
    merchant_id: str = KNOWN_MERCHANT,
    amount_minor: int = 1000,
    currency: str = VALID_CURRENCY,
    lsn: int = 9_000_000_001,
) -> None:
    serializer = _get_avro_serializer()
    envelope = _envelope(
        op="c",
        transaction_id=transaction_id,
        merchant_id=merchant_id,
        amount_minor=amount_minor,
        currency=currency,
        lsn=lsn,
    )
    payload = _try_serialize(serializer, envelope)

    delivered = {"err": None}
    def _cb(err, _msg): delivered["err"] = err

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "all"})
    producer.produce(CDC_TOPIC, key=transaction_id.encode("utf-8"), value=payload, on_delivery=_cb)
    producer.flush(15)
    if delivered["err"] is not None:
        raise SystemExit(f"FATAL: Kafka produce failed: {delivered['err']}")
    print(f"  published rogue Avro envelope key={transaction_id!r} to {CDC_TOPIC}")


def read_dlq_messages(
    timeout_s: float = DLQ_CONSUME_SECONDS, max_records: int = 20_000
) -> List[Dict[str, Any]]:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"day5-inject-dlq-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(conf)
    consumer.subscribe([DLQ_TOPIC])
    
    out: List[Dict[str, Any]] = []
    start_time = time.time()
    try:
        while time.time() - start_time < timeout_s:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            raw = msg.value() or b""
            try:
                parsed = json.loads(raw.decode("utf-8"))
                rec = parsed if isinstance(parsed, dict) else {"_raw": parsed}
            except Exception:
                rec = {"_raw": raw.decode("utf-8", "replace")}
            key_raw = msg.key()
            rec["_key"] = key_raw.decode("utf-8", "replace") if key_raw else None
            out.append(rec)
            if len(out) >= max_records:
                break
    finally:
        consumer.close()
    return out


def wait_for_dlq(ids: Sequence[str]) -> List[Dict[str, Any]]:
    want = set(ids)
    deadline = time.time() + POLL_SECONDS
    
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"day5-inject-dlq-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(conf)
    consumer.subscribe([DLQ_TOPIC])
    
    out: List[Dict[str, Any]] = []
    have = set()
    try:
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            raw = msg.value() or b""
            try:
                parsed = json.loads(raw.decode("utf-8"))
                rec = parsed if isinstance(parsed, dict) else {"_raw": parsed}
            except Exception:
                rec = {"_raw": raw.decode("utf-8", "replace")}
            key_raw = msg.key()
            rec["_key"] = key_raw.decode("utf-8", "replace") if key_raw else None
            out.append(rec)
            
            tid = str(rec.get("transaction_id") or rec.get("_key") or "")
            if tid in want:
                have.add(tid)
            if have == want:
                return out
    finally:
        consumer.close()
    return out


def _reason_of(rec: Dict[str, Any]) -> str:
    return str(rec.get("rejection_reason") or rec.get("reason") or "")


def dlq_hit(transaction_id: str, messages: Optional[List[Dict[str, Any]]] = None) -> Tuple[bool, str]:
    blob = messages if messages is not None else read_dlq_messages()
    for rec in blob:
        tid = str(rec.get("transaction_id") or rec.get("_key") or "")
        if tid == transaction_id:
            return True, _reason_of(rec)
    return False, ""


def _spark():
    from spark.jobs.payments_cdc_job import TABLE_PATH, build_spark
    return build_spark("day5-inject-observe"), TABLE_PATH


def delta_live_ids(spark, table_path: str, ids: Sequence[str]) -> set:
    if not ids: return set()
    df = (
        spark.read.format("delta")
        .load(table_path)
        .filter(F.col("is_delete") == F.lit(False))
        .filter(F.col("transaction_id").isin(list(ids)))
        .select("transaction_id")
    )
    return {r["transaction_id"] for r in df.collect()}


def wait_for_delta_presence(spark, table_path: str, ids: Sequence[str], label: str) -> set:
    want = set(ids)
    deadline = time.time() + POLL_SECONDS
    seen: set = set()
    while time.time() < deadline:
        seen = delta_live_ids(spark, table_path, ids)
        if seen == want: return seen
        time.sleep(POLL_INTERVAL)
    return seen


def settle_absence(spark, table_path: str, ids: Sequence[str]) -> set:
    time.sleep(SETTLE_SECONDS)
    return delta_live_ids(spark, table_path, ids)


def test_1_negative_amount(spark, table_path: str) -> Dict[str, Any]:
    tid = str(uuid.uuid4())
    insert_row(tid, KNOWN_MERCHANT, -1500, VALID_CURRENCY)
    msgs = wait_for_dlq([tid])
    in_dlq, reason = dlq_hit(tid, msgs)
    leaked = tid in settle_absence(spark, table_path, [tid])
    ok = in_dlq and "NEGATIVE_AMOUNT" in reason and not leaked
    return {"name": "NEGATIVE_AMOUNT", "ok": ok, "ids": [tid], "detail": f"dlq={in_dlq} reason={reason or None} in_delta={leaked}"}


def test_2_invalid_currency(spark, table_path: str) -> Dict[str, Any]:
    tid = str(uuid.uuid4())
    insert_row(tid, KNOWN_MERCHANT, 2500, INVALID_CURRENCY)
    msgs = wait_for_dlq([tid])
    in_dlq, reason = dlq_hit(tid, msgs)
    leaked = tid in settle_absence(spark, table_path, [tid])
    ok = in_dlq and "INVALID_CURRENCY" in reason and not leaked
    return {"name": "INVALID_CURRENCY", "ok": ok, "ids": [tid], "detail": f"dlq={in_dlq} reason={reason or None} in_delta={leaked}"}


def test_3_invalid_uuid(spark, table_path: str) -> Dict[str, Any]:
    tid = "not-a-uuid-at-all-12345"
    publish_rogue_avro(tid)
    msgs = wait_for_dlq([tid])
    in_dlq, reason = dlq_hit(tid, msgs)
    leaked = tid in settle_absence(spark, table_path, [tid])
    ok = in_dlq and "INVALID_UUID" in reason and not leaked
    return {"name": "INVALID_UUID", "ok": ok, "ids": [tid], "detail": f"dlq={in_dlq} reason={reason or None} in_delta={leaked}", "in_postgres": False}


def test_4_unknown_merchant(spark, table_path: str) -> Dict[str, Any]:
    tid = str(uuid.uuid4())
    insert_row(tid, GHOST_MERCHANT, 3300, VALID_CURRENCY)
    msgs = wait_for_dlq([tid])
    in_dlq, reason = dlq_hit(tid, msgs)
    leaked = tid in settle_absence(spark, table_path, [tid])
    ok = in_dlq and "UNKNOWN_MERCHANT" in reason and not leaked
    return {"name": "UNKNOWN_MERCHANT", "ok": ok, "ids": [tid], "detail": f"dlq={in_dlq} reason={reason or None} in_delta={leaked}"}


def test_5_mixed_batch(spark, table_path: str) -> Dict[str, Any]:
    valid_ids: List[str] = [str(uuid.uuid4()) for _ in range(50)]
    invalid_ids: List[str] = [str(uuid.uuid4()) for _ in range(50)]

    invalid_spec: List[Tuple[str, str, int, str]] = []
    for i, tid in enumerate(invalid_ids):
        if i < 20: invalid_spec.append((tid, KNOWN_MERCHANT, -4000 - i, VALID_CURRENCY))
        elif i < 35: invalid_spec.append((tid, KNOWN_MERCHANT, 4000 + i, INVALID_CURRENCY))
        else: invalid_spec.append((tid, GHOST_MERCHANT, 4000 + i, VALID_CURRENCY))

    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            for tid in valid_ids:
                cur.execute(
                    "INSERT INTO transactions (transaction_id, merchant_id, amount_minor, currency, status, event_type) VALUES (%s, %s, %s, %s, %s, %s)",
                    (tid, KNOWN_MERCHANT, 5000, VALID_CURRENCY, VALID_STATUS, VALID_EVENT_TYPE),
                )
            for tid, mid, amt, ccy in invalid_spec:
                cur.execute(
                    "INSERT INTO transactions (transaction_id, merchant_id, amount_minor, currency, status, event_type) VALUES (%s, %s, %s, %s, %s, %s)",
                    (tid, mid, amt, ccy, VALID_STATUS, VALID_EVENT_TYPE),
                )
        conn.commit()
    finally:
        conn.close()

    present = wait_for_delta_presence(spark, table_path, valid_ids, "mixed-valid")
    msgs = wait_for_dlq(invalid_ids)
    leaked = settle_absence(spark, table_path, invalid_ids)

    dlq_ids = set()
    for rec in msgs:
        tid = str(rec.get("transaction_id") or rec.get("_key") or "")
        if tid in set(invalid_ids): dlq_ids.add(tid)

    ok = len(present) == 50 and len(leaked) == 0 and len(set(invalid_ids) - dlq_ids) == 0
    return {"name": "MIXED_BATCH", "ok": ok, "ids": invalid_ids, "detail": f"valid_present={len(present)}/50 invalid_leaked={len(leaked)}/50 dlq_missing={len(set(invalid_ids) - dlq_ids)}/50"}


def _dump_ids(pg_ids: List[str], rogue_ids: List[str]) -> None:
    ID_DUMP.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "written_at": datetime.now(timezone.utc).isoformat(),
        "postgres_ids": pg_ids,
        "rogue_ids": rogue_ids,
        "quarantine_oracle": len(pg_ids),
        "dlq_oracle": len(pg_ids) + len(rogue_ids),
    }
    ID_DUMP.write_text(json.dumps(payload, indent=2))
    print(f"Wrote injected ids -> {ID_DUMP}")


def run_suite() -> int:
    print("Day 5 – Five DLQ Injection Tests")
    check_streaming_job_alive()
    assert_transaction_id_is_uuid()
    print()

    spark, table_path = _spark()
    results: List[Dict[str, Any]] = []
    pg_ids, rogue_ids = [], []

    tests = [
        ("test_1_negative_amount", test_1_negative_amount),
        ("test_2_invalid_currency", test_2_invalid_currency),
        ("test_3_invalid_uuid (Avro path)", test_3_invalid_uuid),
        ("test_4_unknown_merchant", test_4_unknown_merchant),
        ("test_5_mixed_batch", test_5_mixed_batch),
    ]

    try:
        for i, (label, fn) in enumerate(tests, start=1):
            print(f"[{i}/5] Running {label} …")
            rec = fn(spark, table_path)
            results.append(rec)
            if rec.get("in_postgres", True): pg_ids.extend(rec["ids"])
            else: rogue_ids.extend(rec["ids"])
            print(f"  -> {'PASS' if rec['ok'] else 'FAIL'} ({rec['detail']})")
    finally:
        spark.stop()

    _dump_ids(pg_ids, rogue_ids)
    print("\n" + "=" * 72)
    for rec in results:
        print(f"{rec['name']:<22} {'PASS' if rec['ok'] else 'FAIL':<8} {rec['detail']}")
    print("=" * 72)

    if all(r["ok"] for r in results):
        print("ALL 5 INJECTION TESTS PASSED")
        return 0
    return 1


if __name__ == "__main__":
    if "--cleanup" in sys.argv:
        ids = json.loads(ID_DUMP.read_text()).get("postgres_ids") or []
        cleanup_injected(ids)
        sys.exit(0)
    sys.exit(run_suite())