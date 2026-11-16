"""Spark progress events (SPEC 7): shape, fail-open behaviour, and 'only after a real ledger commit'."""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

import spark.jobs.payments_cdc_job as job
from spark.utils import progress_events as pe


class OkProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key=None, value=None, on_delivery=None):
        self.sent.append((topic, key, json.loads(value)))
        on_delivery(None, None)

    def flush(self, timeout):
        return 0


class BoomProducer:
    def produce(self, *a, **k):
        raise RuntimeError("broker unreachable")

    def flush(self, timeout):
        return 0


def test_event_has_the_fields_the_spec_lists():
    ev = pe.build_event(app_id="a", batch_id=7, input_rows=10, merged_rows=8, dlq_rows=2, duration_seconds=1.23456,
                        max_source_ts=datetime(2026, 11, 16, 1, 2, 3), committed_at=datetime(2026, 11, 16, 1, 2, 9, tzinfo=timezone.utc))
    assert ev == {"app_id": "a", "batch_id": 7, "input_rows": 10, "merged_rows": 8, "dlq_rows": 2, "duration_seconds": 1.235,
                  "max_source_ts": "2026-11-16T01:02:03+00:00", "committed_at": "2026-11-16T01:02:09+00:00",
                  "end_offsets": None}


def test_publish_goes_to_the_progress_topic_and_reports_ack():
    p = OkProducer()
    ev = pe.build_event(app_id="a", batch_id=1, input_rows=1, merged_rows=1, dlq_rows=0, duration_seconds=0.1, max_source_ts=None)
    assert pe.publish_progress(ev, producer_factory=lambda: p) is True
    assert p.sent[0][0] == "pipeline.progress" and p.sent[0][2]["batch_id"] == 1


def test_a_broken_broker_never_raises_into_the_batch():
    ev = pe.build_event(app_id="a", batch_id=1, input_rows=1, merged_rows=1, dlq_rows=0, duration_seconds=0.1, max_source_ts=None)
    assert pe.publish_progress(ev, producer_factory=lambda: BoomProducer()) is False


def test_factory_failure_is_also_swallowed():
    def factory():
        raise OSError("cannot create producer")

    ev = pe.build_event(app_id="a", batch_id=1, input_rows=0, merged_rows=0, dlq_rows=0, duration_seconds=0, max_source_ts=None)
    assert pe.publish_progress(ev, producer_factory=factory) is False


def test_nothing_is_published_when_the_ledger_was_not_written(monkeypatch):
    calls = []
    monkeypatch.setattr(job, "publish_progress", lambda ev: calls.append(ev))
    job._emit_progress(False, None, 5, 10, 10, 0, 1.0)  # replay harnesses use commit=False
    assert calls == []


def test_emit_progress_swallows_publisher_exceptions(monkeypatch):
    def boom(ev):
        raise RuntimeError("kaboom")

    monkeypatch.setattr(job, "publish_progress", boom)
    job._emit_progress(True, None, 5, 0, 0, 0, 1.0)  # must not raise


def test_emit_progress_publishes_after_a_real_commit(monkeypatch):
    calls = []
    monkeypatch.setattr(job, "publish_progress", lambda ev: calls.append(ev))
    job._emit_progress(True, None, 9, 4, 3, 1, 2.0)
    assert calls and calls[0]["batch_id"] == 9 and calls[0]["merged_rows"] == 3 and calls[0]["dlq_rows"] == 1


def test_event_carries_exclusive_end_offsets_per_partition():
    ev = pe.build_event(app_id="a", batch_id=3, input_rows=1, merged_rows=1, dlq_rows=0, duration_seconds=0.1,
                        max_source_ts=None, end_offsets={"0": 2325, "2": 2354})
    assert ev["end_offsets"] == {"0": 2325, "2": 2354}


def test_emit_progress_reports_offsets_from_the_raw_batch_after_commit(monkeypatch):
    class Row(dict):
        pass

    class Grouped:
        def agg(self, *_):
            return self

        def collect(self):
            return [Row(partition=0, m=2324), Row(partition=2, m=2353)]

    class RawBatch:
        def groupBy(self, *_):
            return Grouped()

    calls = []
    monkeypatch.setattr(job, "publish_progress", lambda ev: calls.append(ev))
    monkeypatch.setattr(job, "_RAW_BATCH", RawBatch())
    monkeypatch.setattr(job.F, "max", lambda c: type("C", (), {"alias": lambda self, n: n})())
    job._emit_progress(True, None, 9, 4, 3, 1, 2.0)
    assert calls[0]["end_offsets"] == {"0": 2325, "2": 2354}


def test_no_offsets_when_there_is_no_raw_batch(monkeypatch):
    calls = []
    monkeypatch.setattr(job, "publish_progress", lambda ev: calls.append(ev))
    monkeypatch.setattr(job, "_RAW_BATCH", None)  # replay harnesses never set it
    job._emit_progress(True, None, 9, 4, 3, 1, 2.0)
    assert calls[0]["end_offsets"] is None
