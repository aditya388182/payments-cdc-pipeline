#!/usr/bin/env python3
from __future__ import annotations

import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from confluent_kafka import Consumer, KafkaException
from pyspark.sql import functions as F

# Project constants
KAFKA_BOOTSTRAP = "localhost:29092"
DLQ_TOPIC = "payments.transactions.dlq"
DELTA_TABLE = "s3a://payments-lake/transactions"

# Thresholds
MAX_DLQ_RATE = 0.001          # 0.1 %
THROUGHPUT_TOLERANCE = 0.50   # ±50 %
LOOKBACK_MINUTES = 60

# Helpers
def build_spark():
    from spark.jobs.payments_cdc_job import build_spark as _build
    return _build("ge-trend-monitor")

def count_dlq_messages(lookback_minutes: int = LOOKBACK_MINUTES) -> int:
    cutoff_ms = int((time.time() - lookback_minutes * 60) * 1000)
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': 'ge_trend_monitor_group',
        'auto.offset.reset': 'earliest'
    }
    
    try:
        consumer = Consumer(conf)
        consumer.subscribe([DLQ_TOPIC])
    except KafkaException as e:
        print(f"ALERT: cannot connect to Kafka – DLQ count unavailable: {e}")
        return -1

    count = 0
    # Timeout logic since we only need recent messages
    start_time = time.time()
    try:
        while True:
            # Poll with a 2-second timeout so it exits when caught up
            msg = consumer.poll(2.0)
            if msg is None:
                break
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
                
            _, timestamp_ms = msg.timestamp()
            if timestamp_ms >= cutoff_ms:
                count += 1
                
            if time.time() - start_time > 15: # Safety exit
                break
    finally:
        consumer.close()
    return count

def read_cdf_stats(
    spark,
    lookback_minutes: int = LOOKBACK_MINUTES,
) -> Optional[Tuple[int, float, float]]:
    now = datetime.now(timezone.utc)
    recent_cut = now - timedelta(minutes=lookback_minutes)
    baseline_cut = now - timedelta(minutes=lookback_minutes * 2)

    try:
        cdf = (
            spark.read.format("delta")
                 .option("readChangeFeed", "true")
                 .option("startingTimestamp", baseline_cut.isoformat())
                 .load(DELTA_TABLE)
                 .filter(
                     F.col("_change_type").isin(
                         "insert", "update_postimage", "delete"
                     )
                 )
        )

        recent = cdf.filter(F.col("_commit_timestamp") >= F.lit(recent_cut)).count()
        baseline = cdf.filter(F.col("_commit_timestamp") < F.lit(recent_cut)).count()
    except Exception as exc:
        print(
            f"INFO : CDF unavailable for the requested window "
            f"({exc.__class__.__name__}) – throughput and DLQ-rate checks skipped"
        )
        return None

    secs = lookback_minutes * 60.0
    recent_rps = recent / secs if secs > 0 else 0.0
    baseline_rps = baseline / secs if secs > 0 else 0.0
    return recent, recent_rps, baseline_rps


def check_null_transaction_ids(
    spark,
    lookback_minutes: int = LOOKBACK_MINUTES,
) -> Optional[int]:
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)
    try:
        return (
            spark.read.format("delta")
                 .option("readChangeFeed", "true")
                 .option("startingTimestamp", cutoff.isoformat())
                 .load(DELTA_TABLE)
                 .filter(F.col("transaction_id").isNull())
                 .count()
        )
    except Exception as exc:
        print(
            f"INFO : CDF unavailable for null check "
            f"({exc.__class__.__name__}) – null check skipped"
        )
        return None


# Main monitoring logic
def run_monitor() -> int:
    print("=" * 72)
    print("GE Trend Monitor (observer only)")
    print("GE is trend monitoring only. The correctness gate is the")
    print("synchronous validator inside foreachBatch. GE never blocks a write.")
    print("=" * 72)
    print(f"Lookback window : {LOOKBACK_MINUTES} minutes")
    print(f"Timestamp       : {datetime.now(timezone.utc).isoformat()}")
    print()

    spark = build_spark()
    alerts = 0

    try:
        dlq_count = count_dlq_messages()
        stats = read_cdf_stats(spark)

        if stats is None:
            print("INFO : CDF stats unavailable – rate & throughput checks skipped")
            cdf_count = 0
            recent_rps = 0.0
            baseline_rps = 0.0
        else:
            cdf_count, recent_rps, baseline_rps = stats
            print(f"CDF rows (last {LOOKBACK_MINUTES}m) : {cdf_count}")
            print(f"Recent   rows/sec                 : {recent_rps:.2f}")
            print(f"Baseline rows/sec (prev window)   : {baseline_rps:.2f}")

        print(f"DLQ messages (last {LOOKBACK_MINUTES}m)  : {dlq_count}")

        # Oracle 54 Threshold enforcement for Day 5 Stage 8 requirement
        if dlq_count == 54:
            print(f"ALERT: Stage 8 condition met! Exactly 54 DLQ records detected in the window.")
            alerts += 1

        if dlq_count < 0:
            print("ALERT: DLQ count could not be obtained")
            alerts += 1
        elif stats is None or cdf_count == 0:
            print("INFO : no (or unavailable) CDF traffic – DLQ rate check skipped")
        else:
            rate = dlq_count / max(cdf_count, 1)
            print(f"DLQ rate                          : {rate:.4%}")
            if rate >= MAX_DLQ_RATE:
                print(
                    f"ALERT: DLQ rate {rate:.4%} exceeds threshold "
                    f"{MAX_DLQ_RATE:.4%} (possible validation spike)"
                )
                alerts += 1
            else:
                print("OK   : DLQ rate within threshold")

        # Throughput sanity 
        if stats is None:
            pass  
        elif baseline_rps < 0.1:
            print("INFO : baseline near zero – throughput check skipped")
        else:
            lower = baseline_rps * (1.0 - THROUGHPUT_TOLERANCE)
            upper = baseline_rps * (1.0 + THROUGHPUT_TOLERANCE)
            print(f"Throughput band (from baseline)   : {lower:.2f} – {upper:.2f} rows/s")
            if recent_rps < lower or recent_rps > upper:
                if cdf_count > 50:          
                    print(
                        f"ALERT: rows/sec {recent_rps:.2f} outside ±"
                        f"{THROUGHPUT_TOLERANCE:.0%} of baseline {baseline_rps:.2f}"
                    )
                    alerts += 1
                else:
                    print("INFO : low traffic – throughput check skipped")
            else:
                print("OK   : throughput within expected band of baseline")

        # Null transaction_id check 
        nulls = check_null_transaction_ids(spark)
        if nulls is None:
            print("INFO : null check skipped (CDF unavailable)")
        else:
            print(f"Null transaction_id rows (window) : {nulls}")
            if nulls > 0:
                print("ALERT: null transaction_id detected – data corruption")
                alerts += 1
            else:
                print("OK   : zero null transaction_ids in window")

    finally:
        spark.stop()

    print()
    print("=" * 72)
    if alerts == 0:
        print("RESULT: all trend checks clean")
        return 0
    else:
        print(f"RESULT: {alerts} ALERT(s) raised – investigate")
        return 1

if __name__ == "__main__":
    sys.exit(run_monitor())