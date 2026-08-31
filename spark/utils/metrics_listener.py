from __future__ import annotations

import json
import logging
import time
from typing import Dict, Optional

import requests

logger = logging.getLogger("payments.metrics")

PUSHGATEWAY_URL = "http://localhost:9091/metrics/job/payments_cdc"
PUSH_TIMEOUT_S = 2


def push_metrics(
    batch_id: int,
    n_in: int,
    n_merged: int,
    n_dlq: int,
    batch_seconds: float,
    last_commit_ts: float,
    processed_offsets: Optional[Dict[str, int]] = None,
) -> None:
    lines = [
        f"payments_cdc_batch_id {int(batch_id)}",
        f"payments_cdc_batch_duration_seconds {float(batch_seconds):.3f}",
        f"payments_cdc_batch_input_rows {int(n_in)}",
        f"payments_cdc_batch_merged_rows {int(n_merged)}",
        f"payments_cdc_batch_dlq_rows {int(n_dlq)}",
        f"payments_cdc_last_ledger_commit_timestamp_seconds {float(last_commit_ts):.3f}",
        f"payments_cdc_heartbeat_timestamp_seconds {time.time():.3f}",
    ]
    if processed_offsets:
        for partition, off in processed_offsets.items():
            part = str(partition).replace('"', "")
            try:
                offset = int(off)
            except (TypeError, ValueError):
                continue
            lines.append(f'payments_cdc_processed_offset{{partition="{part}"}} {offset}')
    body = "\n".join(lines) + "\n"
    try:
        requests.post(PUSHGATEWAY_URL, data=body, timeout=PUSH_TIMEOUT_S)
    except Exception as exc:
        logger.warning("metrics push failed (%s)", exc.__class__.__name__)


def extract_processed_offsets(last_progress) -> Dict[str, int]:
    if not last_progress:
        return {}
    try:
        sources = last_progress.get("sources") or []
        if not sources:
            return {}
        raw = sources[0].get("endOffset") or sources[0].get("startOffset")
        if raw is None:
            return {}
        if isinstance(raw, str):
            raw = json.loads(raw)
        if not isinstance(raw, dict):
            return {}
        topic_map = raw.get("payments.public.transactions", raw)
        if (
            isinstance(topic_map, dict)
            and "payments.public.transactions" not in raw
            and len(raw) == 1
        ):
            topic_map = next(iter(raw.values()))
        if not isinstance(topic_map, dict):
            return {}
        out: Dict[str, int] = {}
        for part, off in topic_map.items():
            try:
                out[str(part)] = int(off)
            except (TypeError, ValueError):
                continue
        return out
    except Exception as exc:
        logger.warning("extract_processed_offsets failed (%s)", exc.__class__.__name__)
        return {}