"""Tiny gRPC client for poking replay-svc by hand: stats | list [reason] | get ID | replay ID."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import grpc  # noqa: E402

import oncall.replay  # noqa: E402,F401
from oncall.v1 import replay_pb2 as pb, replay_pb2_grpc as g  # noqa: E402

ch = grpc.insecure_channel("127.0.0.1:50051")
stub = g.ReplayServiceStub(ch)
cmd = sys.argv[1]
if cmd == "stats":
    print(stub.LakeStats(pb.LakeStatsRequest(), timeout=20))
elif cmd == "list":
    r = stub.ListDlq(pb.ListDlqRequest(limit=5, reason=sys.argv[2] if len(sys.argv) > 2 else ""), timeout=30)
    print("topic_messages", r.topic_messages, "next", repr(r.next_cursor))
    for x in r.records:
        print(x.id, x.transaction_id, x.rejection_reason, x.lsn, x.applied)
elif cmd == "get":
    print(stub.GetDlq(pb.GetDlqRequest(id=sys.argv[2]), timeout=20))
elif cmd == "replay":
    print(stub.Replay(pb.ReplayRequest(dlq_id=sys.argv[2], requested_by="smoke", idempotency_key=sys.argv[3] if len(sys.argv) > 3 else ""), timeout=100))
