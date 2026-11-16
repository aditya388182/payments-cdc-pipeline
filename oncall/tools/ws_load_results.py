"""Builds results/oncall/ws_load.json from the k6 summaries, the /metrics samples and the slow-client summary."""
from __future__ import annotations

import csv
import json
import subprocess
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from provenance import REPO, write_result  # noqa: E402

RAW = REPO / "results" / "oncall" / "raw"


def instance_type() -> str:
    try:
        req = urllib.request.Request("http://169.254.169.254/latest/api/token", method="PUT", headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"})
        token = urllib.request.urlopen(req, timeout=2).read().decode()
        req = urllib.request.Request("http://169.254.169.254/latest/meta-data/instance-type", headers={"X-aws-ec2-metadata-token": token})
        return urllib.request.urlopen(req, timeout=2).read().decode()
    except Exception:
        return "unknown (metadata service not reachable)"


def samples(variant: str) -> list[dict[str, float]]:
    p = RAW / f"metrics_{variant}.csv"
    rows = []
    with p.open() as f:
        for r in csv.DictReader(f):
            rows.append({k: (float(v) if v not in ("", None) else 0.0) for k, v in r.items()})
    return rows


def server_stats(variant: str, ramp_s: int, hold_s: int) -> dict:
    s = samples(variant)
    hold = [r for r in s if ramp_s <= r["elapsed_s"] <= ramp_s + hold_s] or s
    cpu_cores = None
    if len(hold) >= 2 and hold[-1]["elapsed_s"] > hold[0]["elapsed_s"]:
        cpu_cores = (hold[-1]["process_cpu_seconds_total"] - hold[0]["process_cpu_seconds_total"]) / (hold[-1]["elapsed_s"] - hold[0]["elapsed_s"])
    return {
        "avg_cpu_cores_during_hold": round(cpu_cores, 3) if cpu_cores is not None else None,
        "peak_rss_mb": round(max(r["process_resident_memory_bytes"] for r in s) / 1e6, 1),
        "peak_heap_inuse_mb": round(max(r["go_memstats_heap_inuse_bytes"] for r in s) / 1e6, 1),
        "peak_goroutines": int(max(r["go_goroutines"] for r in s)),
        "peak_open_fds": int(max(r["process_open_fds"] for r in s)),
        "peak_ws_clients": int(max(r["oncall_ws_clients"] for r in s)),
        "ws_clients_dropped_total": int(s[-1]["oncall_ws_clients_dropped_total"]),
        "ws_write_timeouts_total": int(s[-1]["oncall_ws_write_timeouts_total"]),
        "ws_ping_timeouts_total": int(s[-1].get("oncall_ws_ping_timeouts_total", 0)),
        "ws_connects_total": int(s[-1]["oncall_ws_connects_total"]),
        "snapshots_published_total": int(s[-1]["oncall_snapshots_published_total"]),
        "metric_samples": len(s),
    }


def main() -> None:
    body: dict = {"label": "local load test: clients and server on the same box (loopback), not a production measurement",
                  "instance_type": instance_type(), "vcpus": int(subprocess.run(["nproc"], capture_output=True, text=True).stdout.strip() or 0),
                  "scope_not_measured": {"push_interval_seconds": 2, "target_clients": 1000}, "variants": {}}
    for variant in ("baseline", "slow_clients"):
        f = RAW / f"ws_{variant}.json"
        if not f.exists():
            continue
        k6 = json.loads(f.read_text())
        ramp_s = {"s": 1, "m": 60}[k6["ramp"][-1]] * int(k6["ramp"][:-1])
        hold_s = {"s": 1, "m": 60}[k6["hold"][-1]] * int(k6["hold"][:-1])
        expected = k6["expected_snapshots_per_client_if_connected_for_ramp_plus_hold"]
        spc = k6["snapshots_per_client"]
        v = {
            "k6": k6,
            "server": server_stats(variant, ramp_s, hold_s),
            "snapshots_received_per_client_median": spc.get("p50"),
            "snapshots_expected_per_client_during_hold": expected,
            "received_over_expected_min": k6["received_over_expected"]["min"],
            "received_over_expected_median": k6["received_over_expected"]["p50"],
            "connections_established": k6["connected"],
            "connections_failed": k6["connect_failed"],
            "unexpected_disconnects": k6["unexpected_close"],
            "seq_order_violations": k6["seq_violations"],
            "delivery_latency_ms": k6["delivery_ms"],
        }
        if variant == "slow_clients" and (RAW / "slow_clients.json").exists():
            v["slow_clients"] = json.loads((RAW / "slow_clients.json").read_text())
        body["variants"][variant] = v
    b, s = body["variants"].get("baseline"), body["variants"].get("slow_clients")
    if b and s:
        body["fast_clients_unaffected_by_slow_ones"] = {
            "baseline_p95_ms": b["delivery_latency_ms"]["p95"], "with_slow_clients_p95_ms": s["delivery_latency_ms"]["p95"],
            "baseline_p99_ms": b["delivery_latency_ms"]["p99"], "with_slow_clients_p99_ms": s["delivery_latency_ms"]["p99"],
            "slow_clients_dropped_by_server": s.get("slow_clients", {}).get("closed_by_server_before_end"),
            "server_side_disconnect_counters": {
                "channel_overflow_drops": s["server"]["ws_clients_dropped_total"], "write_timeouts": s["server"]["ws_write_timeouts_total"],
                "ping_timeouts": s["server"]["ws_ping_timeouts_total"],
                "note": "ping_timeouts can exceed the slow-client count by a few: k6's own clients disconnecting at the very end of the run",
            },
            "slow_clients_closed_by_server_client_view": s.get("slow_clients", {}).get("closed_by_server_before_end"),
            "median_seconds_until_a_slow_client_was_dropped": s.get("slow_clients", {}).get("median_seconds_until_dropped"),
        }
    write_result("ws_load.json", "make ws-load", body)
    print(json.dumps({k: (v if k != "variants" else list(v)) for k, v in body.items()}, indent=2)[:1200])


if __name__ == "__main__":
    main()
