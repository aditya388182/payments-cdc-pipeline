"""Observability-only progress events (SPEC section 7).

One JSON message per committed micro-batch goes to the `pipeline.progress` topic. It is published AFTER the
Delta MERGE and the batch-ledger commit succeeded, is never read by the pipeline, and can never fail a
batch: every error is logged and swallowed (same fail-open rule as metrics_listener.push_metrics).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Optional

logger = logging.getLogger("payments.progress")

KAFKA_BOOTSTRAP = "localhost:29092"
PROGRESS_TOPIC = "pipeline.progress"
FLUSH_TIMEOUT_S = 3.0

_producer: Any = None


def _get_producer() -> Any:
    global _producer
    if _producer is None:
        from confluent_kafka import Producer

        _producer = Producer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "socket.timeout.ms": 3000,
                "message.timeout.ms": 3000,
                "retries": 0,
            }
        )
    return _producer


def build_event(
    *,
    app_id: str,
    batch_id: int,
    input_rows: int,
    merged_rows: int,
    dlq_rows: int,
    duration_seconds: float,
    max_source_ts: Optional[datetime],
    committed_at: Optional[datetime] = None,
    end_offsets: Optional[dict] = None,
) -> dict:
    committed_at = committed_at or datetime.now(timezone.utc)
    if max_source_ts is not None and max_source_ts.tzinfo is None:
        max_source_ts = max_source_ts.replace(tzinfo=timezone.utc)
    return {
        "app_id": app_id,
        "batch_id": int(batch_id),
        "input_rows": int(input_rows),
        "merged_rows": int(merged_rows),
        "dlq_rows": int(dlq_rows),
        "duration_seconds": round(float(duration_seconds), 3),
        "max_source_ts": max_source_ts.isoformat() if max_source_ts else None,
        "committed_at": committed_at.isoformat(),
        # exclusive end offset per Kafka partition present in this batch (next offset to read)
        "end_offsets": end_offsets,
    }


def publish_progress(event: dict, producer_factory: Callable[[], Any] = _get_producer) -> bool:
    """Best effort. Returns True if the broker acknowledged the message; never raises."""
    try:
        p = producer_factory()
        result: dict = {}

        def _cb(err, _msg):
            result["err"] = err

        p.produce(PROGRESS_TOPIC, key=str(event["batch_id"]).encode(), value=json.dumps(event).encode(), on_delivery=_cb)
        p.flush(FLUSH_TIMEOUT_S)
        if result.get("err") is not None or "err" not in result:
            logger.warning("progress event for batch %s not acknowledged (%s)", event.get("batch_id"), result.get("err"))
            return False
        return True
    except Exception as exc:  # observability must never abort a committed batch
        logger.warning("progress event publish failed (%s: %s)", exc.__class__.__name__, exc)
        return False
