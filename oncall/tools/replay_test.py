"""Replay-twice proof through the real stack and the real API (SPEC 11: results/oncall/replay_test.json).

1. inject a transaction for an unknown merchant -> the pipeline's validator dead-letters it
2. replay while the merchant is still unknown -> STILL_INVALID
3. add the merchant to the reference table, wait out the pipeline's 120 s merchant cache (D-010)
4. replay (key A) -> APPLIED, replay (key B) -> ALREADY_APPLIED, repeat key A -> the stored first result
5. LSN guard probe: publish the same record to Kafka AGAIN, bypassing replay-svc, wait for the stream to process it,
   and show the lake still holds exactly one row with the same LSN
6. count lake rows for the key and the audit rows for the replays
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from provenance import REPO, write_result  # noqa: E402

API = os.environ.get("ONCALL_API", "http://127.0.0.1:8088")
CACHE_WAIT_S = 125


def mint(role: str, sub: str) -> str:
    env = {**os.environ}
    for line in (REPO / ".env").read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            env.setdefault(k, v)
    return subprocess.run([str(REPO / "oncall/bin/oncall-api"), "token", "-sub", sub, "-role", role], env=env, capture_output=True, text=True, check=True).stdout.strip()


def lake_rows(txid: str) -> list[dict]:
    from deltalake import DeltaTable

    dt = DeltaTable("s3://payments-lake/transactions", storage_options={
        "AWS_ENDPOINT_URL": "http://127.0.0.1:9000", "AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin",
        "AWS_REGION": "us-east-1", "AWS_ALLOW_HTTP": "true"})
    t = dt.to_pyarrow_dataset().to_table(columns=["transaction_id", "merchant_id", "lsn", "is_delete", "amount_minor"]).to_pylist()
    return [r for r in t if r["transaction_id"] == txid]


def main() -> None:
    admin = {"Authorization": "Bearer " + mint("admin", "admin-user")}
    steps: list[dict] = []

    def log(name: str, **kw):
        steps.append({"step": name, **kw})
        print(name, json.dumps(kw)[:300], flush=True)

    merchant = "MERCH_RT_" + uuid.uuid4().hex[:5].upper()
    out = subprocess.run([str(REPO / "venv/bin/python"), "oncall/tools/dlq_demo.py", "inject", merchant], cwd=REPO, capture_output=True, text=True, check=True).stdout.split()
    txid = out[0]
    log("injected", transaction_id=txid, merchant=merchant)

    dlq_id = None
    for _ in range(60):
        r = requests.get(f"{API}/v1/dlq", params={"reason": "UNKNOWN_MERCHANT", "limit": 100}, headers=admin, timeout=30).json()
        hit = [x for x in r.get("records", []) if x["transaction_id"] == txid]
        # page forward if needed
        cur = r.get("next_cursor")
        while not hit and cur:
            r = requests.get(f"{API}/v1/dlq", params={"reason": "UNKNOWN_MERCHANT", "limit": 100, "cursor": cur}, headers=admin, timeout=30).json()
            hit = [x for x in r.get("records", []) if x["transaction_id"] == txid]
            cur = r.get("next_cursor")
        if hit:
            dlq_id, lsn = hit[0]["id"], hit[0]["lsn"]
            break
        time.sleep(5)
    assert dlq_id, "record never reached the DLQ"
    log("dead_lettered", dlq_id=dlq_id, lsn=lsn, reason=hit[0]["rejection_reason"])

    def replay(key: str):
        r = requests.post(f"{API}/v1/dlq/{dlq_id}/replay", headers={**admin, "Idempotency-Key": key}, timeout=120)
        return r.status_code, r.json(), r.headers.get("Idempotent-Replayed")

    code, body, _ = replay("rt-before-fix-" + txid[:8])
    log("replay_before_fixing_reference_data", http=code, status=body.get("status"), detail=body.get("detail"))
    assert body.get("status") == "STILL_INVALID"

    subprocess.run([str(REPO / "venv/bin/python"), "oncall/tools/dlq_demo.py", "fix", merchant], cwd=REPO, capture_output=True, text=True, check=True)
    log("merchant_added_to_reference_table", merchant=merchant, waiting_s=CACHE_WAIT_S, why="pipeline merchant cache refreshes every 120 s (DECISIONS D-010)")
    time.sleep(CACHE_WAIT_S)

    keyA, keyB = "rt-first-" + txid[:8], "rt-second-" + txid[:8]
    c1, b1, h1 = replay(keyA)
    c2, b2, h2 = replay(keyB)
    c3, b3, h3 = replay(keyA)
    log("replay_1", key=keyA, http=c1, status=b1.get("status"), detail=b1.get("detail"))
    log("replay_2", key=keyB, http=c2, status=b2.get("status"), detail=b2.get("detail"))
    log("replay_1_repeated_same_key", http=c3, status=b3.get("status"), idempotent_replayed_header=h3)

    rows_after_replays = lake_rows(txid)
    log("lake_after_replays", rows=len(rows_after_replays), lsns=[r["lsn"] for r in rows_after_replays])

    # LSN guard probe: bypass the API and replay-svc's lake check, publish the same record again
    from oncall.replay.models import DlqEntry
    from oncall.replay.publisher import AvroCdcPublisher

    dlq = requests.get(f"{API}/v1/dlq/{dlq_id}", headers=admin, timeout=30).json()
    entry = DlqEntry.from_message(int(dlq_id), txid.encode(), dlq["payload_json"].encode())
    t_pub = time.time()
    part, off = AvroCdcPublisher().publish(entry)
    log("duplicate_published_directly_to_kafka", partition=part, offset=off)
    from confluent_kafka import Consumer, TopicPartition

    deadline, processed = time.time() + 180, False
    c = Consumer({"bootstrap.servers": "127.0.0.1:29092", "group.id": "rt-" + uuid.uuid4().hex, "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe(["pipeline.progress"])
    while time.time() < deadline and not processed:
        m = c.poll(1.0)
        if m is None or m.error():
            continue
        ev = json.loads(m.value())
        offs = (ev.get("end_offsets") or {})
        if offs.get(str(part), 0) > off:
            processed = True
            log("stream_processed_the_duplicate", batch_id=ev["batch_id"], merged_rows=ev["merged_rows"], end_offset=offs[str(part)])
    c.close()
    assert processed, "the stream never processed the duplicate"
    rows_after_dup = lake_rows(txid)
    log("lake_after_duplicate", rows=len(rows_after_dup), lsns=[r["lsn"] for r in rows_after_dup])

    audit = requests.get(f"{API}/v1/audit", params={"limit": 200}, headers=admin, timeout=30).json()["entries"]
    mine = [a for a in audit if a["action"] == "dlq.replay" and a["target"] == f"dlq:{dlq_id}"]
    ok_rows = [a for a in mine if a["idempotency_key"] in (keyA, keyB)]
    log("audit_rows", total_for_target=len(mine), for_the_two_replays=len(ok_rows),
        results=[(a["idempotency_key"], a["result"]) for a in mine])

    passed = (b1.get("status") == "APPLIED" and b2.get("status") == "ALREADY_APPLIED" and h3 == "true" and b3 == b1
              and len(rows_after_replays) == 1 and len(rows_after_dup) == 1
              and rows_after_dup[0]["lsn"] == rows_after_replays[0]["lsn"] == lsn and len(ok_rows) == 2)
    write_result("replay_test.json", "python oncall/tools/replay_test.py", {
        "passed": passed, "transaction_id": txid, "dlq_id": dlq_id,
        "first_replay_status": b1.get("status"), "second_replay_status": b2.get("status"),
        "repeat_same_key_returned_first_result": b3 == b1 and h3 == "true",
        "lake_rows_for_key_after_replays": len(rows_after_replays), "lake_rows_for_key_after_duplicate_publish": len(rows_after_dup),
        "lake_lsn": rows_after_dup[0]["lsn"], "record_lsn": lsn, "audit_rows_for_the_two_replays": len(ok_rows),
        "audit_rows_for_target_total": len(mine), "steps": steps,
    })
    print("PASSED" if passed else "FAILED")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
