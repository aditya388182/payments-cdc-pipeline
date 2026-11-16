"""Reads the DLQ topic (payments.transactions.dlq, JSON values, one partition) with confluent_kafka."""
from __future__ import annotations

import time
import uuid
from typing import Optional

from confluent_kafka import Consumer, KafkaError, TopicPartition

from .models import DlqEntry


class KafkaDlq:
    def __init__(self, bootstrap: str, topic: str, partition: int = 0) -> None:
        self._bootstrap = bootstrap
        self._topic = topic
        self._partition = partition

    def _consumer(self) -> Consumer:
        return Consumer(
            {
                "bootstrap.servers": self._bootstrap,
                "group.id": f"replay-svc-{uuid.uuid4()}",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "session.timeout.ms": 10000,
            }
        )

    def _watermarks(self, c: Consumer) -> tuple[int, int]:
        low, high = c.get_watermark_offsets(TopicPartition(self._topic, self._partition), timeout=5, cached=False)
        return low, high

    def message_count(self) -> int:
        c = self._consumer()
        try:
            low, high = self._watermarks(c)
            return max(0, high - low)
        finally:
            c.close()

    def end_offset(self) -> int:
        c = self._consumer()
        try:
            return self._watermarks(c)[1]
        finally:
            c.close()

    def get(self, offset: int) -> Optional[DlqEntry]:
        c = self._consumer()
        try:
            low, high = self._watermarks(c)
            if offset < low or offset >= high:
                return None
            c.assign([TopicPartition(self._topic, self._partition, offset)])
            deadline = time.time() + 8
            while time.time() < deadline:
                m = c.poll(1.0)
                if m is None:
                    continue
                if m.error():
                    if m.error().code() == KafkaError._PARTITION_EOF:
                        return None
                    continue
                if m.offset() == offset:
                    return DlqEntry.from_message(m.offset(), m.key(), m.value() or b"")
            return None
        finally:
            c.close()

    def scan(self, start: int, max_messages: int) -> tuple[list[DlqEntry], int, int]:
        c = self._consumer()
        try:
            low, high = self._watermarks(c)
            start = max(start, low)
            out: list[DlqEntry] = []
            if start >= high:
                return out, high, high
            c.assign([TopicPartition(self._topic, self._partition, start)])
            nxt = start
            deadline = time.time() + 10
            while nxt < high and len(out) < max_messages and time.time() < deadline:
                m = c.poll(1.0)
                if m is None or m.error():
                    continue
                out.append(DlqEntry.from_message(m.offset(), m.key(), m.value() or b""))
                nxt = m.offset() + 1
            return out, nxt, high
        finally:
            c.close()
