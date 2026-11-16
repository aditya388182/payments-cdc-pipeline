"""replay-svc tests: the real gRPC stack served in-process, with fake lake / DLQ / validator / publisher."""
from __future__ import annotations

import grpc
import pytest

from oncall.v1 import replay_pb2 as pb

from conftest import FakeLake, FakeSource, FakeValidator, entry


def replay(h, dlq_id, key="k1", by="tester", timeout=20):
    return h.stub.Replay(pb.ReplayRequest(dlq_id=dlq_id, requested_by=by, idempotency_key=key), timeout=timeout)


def test_replay_applied_publishes_once_and_waits_for_lake(make_harness):
    h, lake, val, pub = make_harness([entry(0)], publisher_kwargs={"delay": 0.3})
    r = replay(h, "0")
    assert r.status == pb.REPLAY_STATUS_APPLIED
    assert r.lake_lsn == 100
    assert len(pub.published) == 1 and val.calls == 1


def test_replay_twice_is_applied_then_already_applied_with_one_publish(make_harness):
    h, lake, val, pub = make_harness([entry(0)])
    first = replay(h, "0", key="a")
    second = replay(h, "0", key="b")
    assert first.status == pb.REPLAY_STATUS_APPLIED
    assert second.status == pb.REPLAY_STATUS_ALREADY_APPLIED
    assert len(pub.published) == 1, "the second replay must not publish again"
    assert lake.lsns == {"t-1": 100}


def test_same_idempotency_key_returns_first_result(make_harness):
    h, lake, val, pub = make_harness([entry(0)])
    a = replay(h, "0", key="same")
    b = replay(h, "0", key="same")
    assert a.status == b.status == pb.REPLAY_STATUS_APPLIED
    assert a.detail == b.detail
    assert val.calls == 1 and len(pub.published) == 1


def test_still_invalid_returns_validator_reason_and_does_not_publish(make_harness):
    h, lake, val, pub = make_harness([entry(0)], validator=FakeValidator(["UNKNOWN_MERCHANT"]))
    r = replay(h, "0")
    assert r.status == pb.REPLAY_STATUS_STILL_INVALID
    assert "UNKNOWN_MERCHANT" in r.detail
    assert pub.published == [] and lake.lsns == {}


def test_lake_with_newer_lsn_is_already_applied_never_resurrected(make_harness):
    # the key was later deleted (tombstone at a higher lsn): replaying the old insert must not publish
    h, lake, val, pub = make_harness([entry(0, lsn=100)], lake=FakeLake({"t-1": 250}))
    r = replay(h, "0")
    assert r.status == pb.REPLAY_STATUS_ALREADY_APPLIED
    assert r.lake_lsn == 250
    assert pub.published == [] and val.calls == 0


@pytest.mark.parametrize("bad", ["", "abc", "-4", "99"])
def test_not_found(make_harness, bad):
    h, *_ = make_harness([entry(0)])
    assert replay(h, bad).status == pb.REPLAY_STATUS_NOT_FOUND


def test_pending_when_the_lake_never_catches_up(make_harness):
    h, lake, val, pub = make_harness([entry(0)], publisher_kwargs={"apply": False}, apply_wait_s=0.3)
    r = replay(h, "0")
    assert r.status == pb.REPLAY_STATUS_PENDING
    assert len(pub.published) == 1


def test_pending_results_are_not_cached_so_a_retry_can_succeed(make_harness):
    h, lake, val, pub = make_harness([entry(0)], publisher_kwargs={"apply": False}, apply_wait_s=0.2)
    assert replay(h, "0", key="r").status == pb.REPLAY_STATUS_PENDING
    lake.lsns["t-1"] = 100  # the stream catches up
    assert replay(h, "0", key="r").status == pb.REPLAY_STATUS_ALREADY_APPLIED


def test_short_client_deadline_bounds_the_wait(make_harness):
    # remaining time ~1s -> the apply wait is clamped to 0 and the call answers PENDING instead of timing out
    h, lake, val, pub = make_harness([entry(0)], publisher_kwargs={"apply": False}, apply_wait_s=60)
    r = replay(h, "0", timeout=1.5)
    assert r.status == pb.REPLAY_STATUS_PENDING


def test_publish_failure_maps_to_unavailable(make_harness):
    h, *_ = make_harness([entry(0)], publisher_kwargs={"fail": True})
    with pytest.raises(grpc.RpcError) as ei:
        replay(h, "0")
    assert ei.value.code() == grpc.StatusCode.UNAVAILABLE


def test_list_open_only_reason_filter_and_paging(make_harness):
    entries = [entry(i, tid=f"t-{i}", reason="UNKNOWN_MERCHANT" if i % 2 == 0 else "NEGATIVE_AMOUNT") for i in range(10)]
    h, lake, *_ = make_harness(entries, lake=FakeLake({"t-0": 100}))  # t-0 already applied
    page1 = h.stub.ListDlq(pb.ListDlqRequest(limit=3, reason="unknown_merchant"))
    assert [r.id for r in page1.records] == ["2", "4", "6"], "applied t-0 and other reasons are filtered out"
    assert page1.next_cursor == "7"
    page2 = h.stub.ListDlq(pb.ListDlqRequest(limit=3, cursor=page1.next_cursor, reason="UNKNOWN_MERCHANT"))
    assert [r.id for r in page2.records] == ["8"]
    assert page2.next_cursor == ""
    assert page2.topic_messages == 10
    assert page2.open_total == 9, "10 records, one already applied"
    assert page2.open_missing_keys == 9, "every open record's key is absent from the lake"
    with_applied = h.stub.ListDlq(pb.ListDlqRequest(limit=50, include_applied=True))
    assert len(with_applied.records) == 10
    assert [r.applied for r in with_applied.records][:2] == [True, False]


def test_list_dedupes_repeat_quarantines_of_the_same_record(make_harness):
    entries = [entry(0, tid="dup"), entry(1, tid="dup"), entry(2, tid="other")]
    h, *_ = make_harness(entries)
    got = h.stub.ListDlq(pb.ListDlqRequest(limit=10))
    assert [r.transaction_id for r in got.records] == ["dup", "other"]


def test_list_rejects_a_bad_cursor(make_harness):
    h, *_ = make_harness([entry(0)])
    with pytest.raises(grpc.RpcError) as ei:
        h.stub.ListDlq(pb.ListDlqRequest(cursor="nope"))
    assert ei.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_get_dlq_found_and_missing(make_harness):
    h, *_ = make_harness([entry(3)], lake=FakeLake({"t-1": 100}))
    ok = h.stub.GetDlq(pb.GetDlqRequest(id="3"))
    assert ok.found and ok.record.applied and ok.record.rejection_reason == "UNKNOWN_MERCHANT"
    assert not h.stub.GetDlq(pb.GetDlqRequest(id="4")).found
    assert not h.stub.GetDlq(pb.GetDlqRequest(id="x")).found


def test_lake_stats(make_harness):
    h, *_ = make_harness([], lake=FakeLake({"a": 5, "b": 9}))
    s = h.stub.LakeStats(pb.LakeStatsRequest())
    assert (s.row_count, s.max_lsn, s.delta_version) == (2, 9, 7)


def test_health_service_reports_serving(make_harness):
    from grpc_health.v1 import health, health_pb2, health_pb2_grpc

    from concurrent import futures

    hs = health.HealthServicer()
    srv = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    hs.set("", health_pb2.HealthCheckResponse.SERVING)
    health_pb2_grpc.add_HealthServicer_to_server(hs, srv)
    port = srv.add_insecure_port("127.0.0.1:0")
    srv.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as ch:
            r = health_pb2_grpc.HealthStub(ch).Check(health_pb2.HealthCheckRequest(service=""), timeout=5)
            assert r.status == health_pb2.HealthCheckResponse.SERVING
    finally:
        srv.stop(0)


def test_pipeline_requarantine_is_reported_immediately_not_after_the_full_wait(make_harness):
    # real bug found in Phase 1: the Spark job's merchant cache is stale for up to 120 s, so a record replayed right
    # after the reference data was fixed is dead-lettered again. replay-svc must say so instead of waiting 75 s.
    import time

    from conftest import entry as mk
    from oncall.replay.models import DlqEntry

    h, lake, val, pub = make_harness([entry(0)], publisher_kwargs={"apply": False}, apply_wait_s=30)
    pub.on_publish = lambda e: servicer_dlq.entries.update({1: mk(1, tid=e.transaction_id, lsn=e.lsn, reason="UNKNOWN_MERCHANT")})
    servicer_dlq = h.servicer_dlq
    t0 = time.monotonic()
    r = replay(h, "0")
    assert r.status == pb.REPLAY_STATUS_PENDING
    assert "quarantined it again" in r.detail and "UNKNOWN_MERCHANT" in r.detail
    assert time.monotonic() - t0 < 5


def test_dlq_stats_counts_open_and_missing_keys(make_harness):
    entries = [entry(0, tid="a"), entry(1, tid="b"), entry(2, tid="b"), entry(3, tid="c", lsn=50)]
    h, *_ = make_harness(entries, lake=FakeLake({"c": 50, "a": 10}))  # c applied; a present but older than the record
    s = h.stub.DlqStats(pb.DlqStatsRequest())
    assert s.topic_messages == 4
    assert s.open_total == 2, "a (lake lsn 10 < 100) and b; the two b entries share key+lsn"
    assert s.open_missing_keys == 1, "only b is absent from the lake; a exists with an older version"


def test_open_missing_keys_only_counts_keys_that_exist_in_the_source(make_harness):
    # a rogue message (key not in Postgres) and a row deleted since do not explain a missing lake row
    entries = [entry(0, tid="real-1"), entry(1, tid="rogue", reason="INVALID_UUID"), entry(2, tid="gone")]
    h, *_ = make_harness(entries, source=FakeSource({"real-1"}))
    s = h.stub.DlqStats(pb.DlqStatsRequest())
    assert s.open_total == 3, "all three records are still open in the DLQ"
    assert s.open_missing_keys == 1, "but only real-1 is a Postgres row the lake lacks"
