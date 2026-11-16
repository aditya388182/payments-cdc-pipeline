"""ReplayService gRPC implementation. All I/O sits behind small protocols (models.py) so tests can run the
real gRPC stack in-process with fakes."""
from __future__ import annotations

import collections
import logging
import threading
import time
from typing import Optional

import grpc

from oncall.v1 import replay_pb2 as pb
from oncall.v1 import replay_pb2_grpc as pb_grpc

from .models import DlqEntry, DlqStore, Lake, Publisher, PublishError, RecordValidator, SourceKeys

log = logging.getLogger("replay.service")

DEFAULT_DEADLINE_S = 30.0
APPLY_WAIT_S = 75.0
POLL_S = 1.0
MAX_SCAN = 5000
MAX_LIMIT = 200


def _fields(**kw):
    return {"fields": kw}


def _record(e: DlqEntry, applied: bool) -> pb.DlqRecord:
    return pb.DlqRecord(
        id=str(e.offset), transaction_id=e.transaction_id, merchant_id=e.merchant_id, amount_minor=e.amount_minor,
        currency=e.currency, status=e.status, event_type=e.event_type, op=e.op, lsn=e.lsn,
        rejection_reason=e.rejection_reason, rejected_at=e.rejected_at, payload_json=e.payload_json, applied=applied,
    )


class ReplayServicer(pb_grpc.ReplayServiceServicer):
    def __init__(self, lake: Lake, dlq: DlqStore, validator: RecordValidator, publisher: Publisher,
                 apply_wait_s: float = APPLY_WAIT_S, poll_s: float = POLL_S, source: SourceKeys | None = None) -> None:
        self._lake, self._dlq, self._validator, self._publisher = lake, dlq, validator, publisher
        self._source = source
        self._apply_wait, self._poll = apply_wait_s, poll_s
        self._idem: "collections.OrderedDict[str, pb.ReplayResponse]" = collections.OrderedDict()
        self._idem_lock = threading.Lock()
        self._replay_lock = threading.Lock()  # one replay at a time: the validator session is shared
        self._open_cache: tuple[int, int, int, int] | None = None  # (dlq_end, lake_version, open_total, open_missing_keys)
        self._open_lock = threading.Lock()

    # -- helpers ---------------------------------------------------------------------------------
    @staticmethod
    def _remaining(ctx: grpc.ServicerContext) -> float:
        r = ctx.time_remaining()
        return DEFAULT_DEADLINE_S if r is None else r

    @staticmethod
    def _parse_id(raw: str) -> Optional[int]:
        try:
            v = int(raw)
            return v if v >= 0 else None
        except (TypeError, ValueError):
            return None

    # -- RPCs ------------------------------------------------------------------------------------
    def Replay(self, request: pb.ReplayRequest, context: grpc.ServicerContext) -> pb.ReplayResponse:
        t0 = time.monotonic()
        key = request.idempotency_key
        if key:
            with self._idem_lock:
                cached = self._idem.get(key)
            if cached is not None:
                log.info("replay served from idempotency cache", extra=_fields(dlq_id=request.dlq_id, idempotency_key=key))
                return cached
        with self._replay_lock:
            resp = self._replay(request, context)
        if key and resp.status != pb.REPLAY_STATUS_PENDING:
            with self._idem_lock:
                self._idem[key] = resp
                while len(self._idem) > 1000:
                    self._idem.popitem(last=False)
        log.info(
            "replay", extra=_fields(dlq_id=request.dlq_id, requested_by=request.requested_by, idempotency_key=key,
                                    status=pb.ReplayStatus.Name(resp.status), seconds=round(time.monotonic() - t0, 3)),
        )
        return resp

    def _replay(self, req: pb.ReplayRequest, ctx: grpc.ServicerContext) -> pb.ReplayResponse:
        offset = self._parse_id(req.dlq_id)
        entry = self._dlq.get(offset) if offset is not None else None
        if entry is None:
            return pb.ReplayResponse(status=pb.REPLAY_STATUS_NOT_FOUND, detail=f"no DLQ record with id {req.dlq_id!r}")

        lake_lsn = self._lake.snapshot(refresh=True).lsns.get(entry.transaction_id)
        if lake_lsn is not None and lake_lsn >= entry.lsn:
            return pb.ReplayResponse(
                status=pb.REPLAY_STATUS_ALREADY_APPLIED, lake_lsn=lake_lsn,
                detail=f"lake already holds {entry.transaction_id} at lsn {lake_lsn} >= record lsn {entry.lsn}; nothing published",
            )

        reasons = self._validator.reasons(entry)
        if reasons:
            return pb.ReplayResponse(
                status=pb.REPLAY_STATUS_STILL_INVALID, lake_lsn=lake_lsn or 0,
                detail="validator still rejects the record: " + ",".join(reasons),
            )

        dlq_end_before = self._dlq.end_offset()
        try:
            part, off = self._publisher.publish(entry)
        except PublishError as exc:
            ctx.abort(grpc.StatusCode.UNAVAILABLE, f"could not publish to Kafka: {exc}")
        log.info("republished", extra=_fields(transaction_id=entry.transaction_id, partition=part, offset=off, lsn=entry.lsn))

        wait = max(0.0, min(self._apply_wait, self._remaining(ctx) - 2.0))
        deadline = time.monotonic() + wait
        while True:
            lsn = self._lake.snapshot(refresh=True).lsns.get(entry.transaction_id)
            if lsn is not None and lsn >= entry.lsn:
                return pb.ReplayResponse(
                    status=pb.REPLAY_STATUS_APPLIED, lake_lsn=lsn,
                    detail=f"validated, re-published to partition {part} offset {off}, lake now at lsn {lsn}",
                )
            requarantined = self._requarantined(entry, dlq_end_before)
            if requarantined:
                return pb.ReplayResponse(
                    status=pb.REPLAY_STATUS_PENDING, lake_lsn=lsn or 0,
                    detail=(f"re-published to partition {part} offset {off} but the pipeline's own validator quarantined it again "
                            f"({requarantined}). Its reference-data cache refreshes every 120 s; retry after that."),
                )
            if time.monotonic() >= deadline:
                return pb.ReplayResponse(
                    status=pb.REPLAY_STATUS_PENDING, lake_lsn=lsn or 0,
                    detail=(f"re-published to partition {part} offset {off} but the lake does not hold it yet after {wait:.0f}s; "
                            "the stream may be behind, or the pipeline's merchant cache (refreshes every 120 s) re-quarantined it; retry"),
                )
            time.sleep(self._poll)

    def _open_counts(self) -> tuple[int, int]:
        """(open records, distinct open keys missing from the lake). Cached per (DLQ end offset, lake version)."""
        snap = self._lake.snapshot()
        end = self._dlq.end_offset()
        with self._open_lock:
            if self._open_cache and self._open_cache[:2] == (end, snap.stats.delta_version):
                return self._open_cache[2], self._open_cache[3]
        seen: set[tuple[str, int]] = set()
        missing: set[str] = set()
        cursor = 0
        while cursor < end:
            entries, cursor, _end = self._dlq.scan(cursor, 1000)
            if not entries:
                break
            for e in entries:
                lsn = snap.lsns.get(e.transaction_id)
                if lsn is None or lsn < e.lsn:
                    seen.add((e.transaction_id, e.lsn))
                    if lsn is None:
                        missing.add(e.transaction_id)
        if self._source is not None and missing:
            # only keys that are real rows in the source explain a missing lake row (not rogue messages / deleted rows)
            missing = self._source.present(missing)
        with self._open_lock:
            self._open_cache = (end, snap.stats.delta_version, len(seen), len(missing))
        return len(seen), len(missing)

    def _requarantined(self, entry: DlqEntry, since_offset: int) -> str:
        """Reason string if the pipeline wrote a NEW DLQ message for this key after `since_offset`."""
        entries, _nxt, _end = self._dlq.scan(since_offset, 200)
        for e in entries:
            if e.transaction_id == entry.transaction_id and e.lsn == entry.lsn:
                return e.rejection_reason or "rejected"
        return ""

    def GetDlq(self, request: pb.GetDlqRequest, context: grpc.ServicerContext) -> pb.GetDlqResponse:
        offset = self._parse_id(request.id)
        entry = self._dlq.get(offset) if offset is not None else None
        if entry is None:
            return pb.GetDlqResponse(found=False)
        lsn = self._lake.snapshot().lsns.get(entry.transaction_id)
        return pb.GetDlqResponse(found=True, record=_record(entry, lsn is not None and lsn >= entry.lsn))

    def ListDlq(self, request: pb.ListDlqRequest, context: grpc.ServicerContext) -> pb.ListDlqResponse:
        limit = min(max(request.limit or 50, 1), MAX_LIMIT)
        start = 0
        if request.cursor:
            parsed = self._parse_id(request.cursor)
            if parsed is None:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "cursor must be a non-negative integer offset")
            start = parsed
        lsns = self._lake.snapshot().lsns
        want = request.reason.upper()
        out: list[pb.DlqRecord] = []
        seen: set[tuple[str, int]] = set()
        cursor = start
        end = start
        while len(out) < limit:
            entries, cursor, end = self._dlq.scan(cursor, min(MAX_SCAN, 500))
            if not entries:
                break
            for i, e in enumerate(entries):
                lsn = lsns.get(e.transaction_id)
                applied = lsn is not None and lsn >= e.lsn
                if applied and not request.include_applied:
                    continue
                if want and want not in e.rejection_reason.upper():
                    continue
                ident = (e.transaction_id, e.lsn)
                if ident in seen:
                    continue
                seen.add(ident)
                out.append(_record(e, applied))
                if len(out) >= limit:
                    cursor = e.offset + 1
                    break
            if cursor >= end:
                break
        next_cursor = str(cursor) if cursor < end else ""
        open_total, open_missing = self._open_counts()
        return pb.ListDlqResponse(records=out, next_cursor=next_cursor, topic_messages=self._dlq.message_count(),
                                  open_total=open_total, open_missing_keys=open_missing)

    def DlqStats(self, request: pb.DlqStatsRequest, context: grpc.ServicerContext) -> pb.DlqStatsResponse:
        open_total, open_missing = self._open_counts()
        return pb.DlqStatsResponse(topic_messages=self._dlq.message_count(), open_total=open_total, open_missing_keys=open_missing)

    def LakeStats(self, request: pb.LakeStatsRequest, context: grpc.ServicerContext) -> pb.LakeStatsResponse:
        s = self._lake.snapshot().stats
        return pb.LakeStatsResponse(
            row_count=s.row_count, tombstone_count=s.tombstone_count, max_lsn=s.max_lsn,
            last_commit_ts=s.last_commit_ts, delta_version=s.delta_version,
        )
