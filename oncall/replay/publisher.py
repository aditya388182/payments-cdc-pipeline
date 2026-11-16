"""Re-publishes a DLQ record through the pipeline's normal path by reusing the repo's producer helpers
(scripts/inject_bad_records: _get_avro_serializer, _envelope, _try_serialize, CDC topic)."""
from __future__ import annotations

import threading

from confluent_kafka import Producer

from .models import DlqEntry, PublishError


class AvroCdcPublisher:
    def __init__(self) -> None:
        from scripts import inject_bad_records as ibr  # the repo's existing producer code, imported not copied

        self._ibr = ibr
        self._lock = threading.Lock()
        self._serializer = None
        self._producer = Producer(
            {"bootstrap.servers": ibr.KAFKA_BOOTSTRAP, "acks": "all", "enable.idempotence": True, "socket.timeout.ms": 10000}
        )

    def build_message(self, e: DlqEntry) -> bytes:
        ibr = self._ibr
        with self._lock:
            if self._serializer is None:
                self._serializer = ibr._get_avro_serializer()
        env = ibr._envelope(
            op=e.op if e.op in ("c", "u", "d") else "c",
            transaction_id=e.transaction_id,
            merchant_id=e.merchant_id,
            amount_minor=e.amount_minor,
            currency=e.currency,
            lsn=e.lsn,
        )
        for side in ("after", "before"):
            if env.get(side):
                env[side]["status"] = e.status or env[side]["status"]
                env[side]["event_type"] = e.event_type or env[side]["event_type"]
        try:
            return ibr._try_serialize(self._serializer, env)
        except SystemExit as exc:  # the repo helper exits on a schema mismatch
            raise PublishError(str(exc)) from exc

    def publish(self, e: DlqEntry) -> tuple[int, int]:
        payload = self.build_message(e)
        result: dict = {}

        def cb(err, msg):
            result["err"], result["msg"] = err, msg

        self._producer.produce(self._ibr.CDC_TOPIC, key=e.transaction_id.encode(), value=payload, on_delivery=cb)
        self._producer.flush(15)
        if result.get("err") is not None or "msg" not in result:
            raise PublishError(f"kafka produce failed: {result.get('err')}")
        m = result["msg"]
        return m.partition(), m.offset()
