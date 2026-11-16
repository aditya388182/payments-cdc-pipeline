"""Samples the API's /metrics (loopback port) every few seconds into a CSV while a load test runs.

usage: sample_metrics.py OUT.csv SECONDS [INTERVAL=5] [URL=http://127.0.0.1:8089/metrics]
"""
from __future__ import annotations

import csv
import sys
import time
import urllib.request

WANT = [
    "oncall_ws_clients", "oncall_ws_connects_total", "oncall_ws_clients_dropped_total", "oncall_ws_write_timeouts_total", "oncall_ws_ping_timeouts_total",
    "oncall_snapshots_published_total", "process_cpu_seconds_total", "process_resident_memory_bytes", "go_goroutines",
    "go_memstats_heap_inuse_bytes", "process_open_fds",
]


def scrape(url: str) -> dict[str, float]:
    out: dict[str, float] = {}
    with urllib.request.urlopen(url, timeout=5) as r:
        for line in r.read().decode().splitlines():
            if line.startswith("#"):
                continue
            name, _, val = line.partition(" ")
            if name in WANT:
                out[name] = float(val.split()[0])
    return out


def main() -> None:
    path, seconds = sys.argv[1], float(sys.argv[2])
    interval = float(sys.argv[3]) if len(sys.argv) > 3 else 5.0
    url = sys.argv[4] if len(sys.argv) > 4 else "http://127.0.0.1:8089/metrics"
    t0 = time.time()
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["elapsed_s"] + WANT)
        while time.time() - t0 < seconds:
            try:
                s = scrape(url)
                w.writerow([round(time.time() - t0, 1)] + [s.get(k, "") for k in WANT])
                f.flush()
            except Exception as exc:  # the API may be restarting; keep sampling
                print("sample failed:", exc, file=sys.stderr)
            time.sleep(interval)


if __name__ == "__main__":
    main()
