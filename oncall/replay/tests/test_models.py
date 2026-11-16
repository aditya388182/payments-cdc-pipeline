import json

from oncall.replay.models import DlqEntry


def test_parses_a_real_dlq_message():
    raw = {"op": "c", "transaction_id": "a334b805", "merchant_id": "MERCH_007", "amount_minor": -1500, "currency": "USD",
           "status": "PENDING", "event_type": "AUTHORIZATION", "lsn": 28970472, "rejection_reason": "NEGATIVE_AMOUNT",
           "rejected_at": "2026-11-16T06:06:32.317638+00:00"}
    e = DlqEntry.from_message(7, b"a334b805", json.dumps(raw).encode())
    assert (e.offset, e.amount_minor, e.lsn, e.rejection_reason) == (7, -1500, 28970472, "NEGATIVE_AMOUNT")


def test_garbage_value_does_not_crash_and_falls_back_to_the_key():
    e = DlqEntry.from_message(1, b"key-1", b"not json")
    assert e.transaction_id == "key-1" and e.lsn == 0 and e.payload_json == "not json"
