"""Plain data types shared by the replay service modules (no gRPC / Spark / Kafka imports)."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Optional, Protocol


@dataclass(frozen=True)
class DlqEntry:
    """One message from the DLQ topic, parsed."""

    offset: int
    transaction_id: str
    merchant_id: str
    amount_minor: int
    currency: str
    status: str
    event_type: str
    op: str
    lsn: int
    rejection_reason: str
    rejected_at: str
    payload_json: str

    @staticmethod
    def from_message(offset: int, key: Optional[bytes], value: bytes) -> "DlqEntry":
        text = value.decode("utf-8", "replace")
        try:
            d = json.loads(text)
            if not isinstance(d, dict):
                d = {}
        except ValueError:
            d = {}
        tid = d.get("transaction_id") or (key.decode("utf-8", "replace") if key else "")
        return DlqEntry(
            offset=offset,
            transaction_id=str(tid),
            merchant_id=str(d.get("merchant_id") or ""),
            amount_minor=int(d.get("amount_minor") or 0),
            currency=str(d.get("currency") or ""),
            status=str(d.get("status") or ""),
            event_type=str(d.get("event_type") or ""),
            op=str(d.get("op") or "c"),
            lsn=int(d.get("lsn") or 0),
            rejection_reason=str(d.get("rejection_reason") or ""),
            rejected_at=str(d.get("rejected_at") or ""),
            payload_json=text,
        )


@dataclass(frozen=True)
class LakeStatsData:
    row_count: int
    tombstone_count: int
    max_lsn: int
    last_commit_ts: str
    delta_version: int


@dataclass(frozen=True)
class LakeSnapshot:
    """Key -> LSN of every row in the lake (tombstones included), plus the summary stats."""

    lsns: dict[str, int]
    stats: LakeStatsData


class Lake(Protocol):
    def snapshot(self, *, refresh: bool = False) -> LakeSnapshot: ...


class DlqStore(Protocol):
    def get(self, offset: int) -> Optional[DlqEntry]: ...

    def scan(self, start: int, max_messages: int) -> tuple[list[DlqEntry], int, int]:
        """Return (entries, next_offset, end_offset) reading from `start` up to max_messages."""
        ...

    def message_count(self) -> int: ...

    def end_offset(self) -> int: ...


class RecordValidator(Protocol):
    def reasons(self, entry: DlqEntry) -> list[str]:
        """Empty list when the existing validator now accepts the record."""
        ...


class Publisher(Protocol):
    def publish(self, entry: DlqEntry) -> tuple[int, int]:
        """Re-publish through the normal CDC topic; returns (partition, offset)."""
        ...


class PublishError(RuntimeError):
    pass


@dataclass
class ReplayOutcome:
    status: int
    detail: str
    lake_lsn: int = 0
    extra: dict = field(default_factory=dict)
