from __future__ import annotations

import sys
import threading
from pathlib import Path

import grpc
import pytest

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import oncall.replay  # noqa: E402,F401  (puts gen/ on sys.path)
from oncall.replay.models import DlqEntry, LakeSnapshot, LakeStatsData, PublishError  # noqa: E402
from oncall.replay.service import ReplayServicer  # noqa: E402
from oncall.v1 import replay_pb2_grpc as pb_grpc  # noqa: E402
from concurrent import futures  # noqa: E402


def entry(offset: int, tid: str = "t-1", reason: str = "UNKNOWN_MERCHANT", lsn: int = 100, merchant: str = "MERCH_NEW", op: str = "c") -> DlqEntry:
    return DlqEntry(offset=offset, transaction_id=tid, merchant_id=merchant, amount_minor=1500, currency="USD", status="PENDING",
                    event_type="AUTHORIZATION", op=op, lsn=lsn, rejection_reason=reason, rejected_at="2026-11-16T00:00:00Z",
                    payload_json="{}")


class FakeLake:
    def __init__(self, lsns=None):
        self.lsns = dict(lsns or {})
        self.lock = threading.Lock()

    def snapshot(self, *, refresh: bool = False) -> LakeSnapshot:
        with self.lock:
            live = len(self.lsns)
            return LakeSnapshot(dict(self.lsns), LakeStatsData(live, 0, max(self.lsns.values(), default=0), "2026-11-16T00:00:00Z", 7))


class FakeDlq:
    def __init__(self, entries):
        self.entries = {e.offset: e for e in entries}

    def get(self, offset):
        return self.entries.get(offset)

    def scan(self, start, max_messages):
        offs = sorted(o for o in self.entries if o >= start)[:max_messages]
        end = (max(self.entries) + 1) if self.entries else 0
        nxt = (offs[-1] + 1) if offs else end
        return [self.entries[o] for o in offs], nxt, end

    def message_count(self):
        return len(self.entries)

    def end_offset(self):
        return (max(self.entries) + 1) if self.entries else 0


class FakeValidator:
    def __init__(self, reasons=None):
        self._reasons = list(reasons or [])
        self.calls = 0

    def reasons(self, entry):
        self.calls += 1
        return list(self._reasons)


class FakePublisher:
    """Publishing 'applies' the record to the fake lake after `delay` seconds, like the stream would."""

    def __init__(self, lake: FakeLake, delay: float = 0.0, apply: bool = True, fail: bool = False):
        self.lake, self.delay, self.apply, self.fail = lake, delay, apply, fail
        self.published = []
        self.on_publish = None  # optional hook, e.g. to simulate the pipeline re-quarantining the record

    def publish(self, entry):
        if self.fail:
            raise PublishError("broker down")
        self.published.append(entry)
        if self.on_publish:
            self.on_publish(entry)

        def do():
            with self.lake.lock:
                cur = self.lake.lsns.get(entry.transaction_id)
                if cur is None or entry.lsn > cur:  # the LSN guard
                    self.lake.lsns[entry.transaction_id] = entry.lsn

        if self.apply:
            if self.delay:
                threading.Timer(self.delay, do).start()
            else:
                do()
        return 0, len(self.published)


class Harness:
    def __init__(self, servicer):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        pb_grpc.add_ReplayServiceServicer_to_server(servicer, self.server)
        port = self.server.add_insecure_port("127.0.0.1:0")
        self.server.start()
        self.channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        self.stub = pb_grpc.ReplayServiceStub(self.channel)

    def close(self):
        self.channel.close()
        self.server.stop(0)


@pytest.fixture
def make_harness():
    made = []

    def make(entries, lake=None, validator=None, publisher_kwargs=None, apply_wait_s=3.0):
        lake = lake or FakeLake()
        pub = FakePublisher(lake, **(publisher_kwargs or {}))
        val = validator or FakeValidator()
        svc = ReplayServicer(lake, FakeDlq(entries), val, pub, apply_wait_s=apply_wait_s, poll_s=0.05)
        h = Harness(svc)
        h.servicer_dlq = svc._dlq
        made.append(h)
        return h, lake, val, pub

    yield make
    for h in made:
        h.close()
