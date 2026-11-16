"""Audit row 7: rerun scripts/aggregation_skew_job.py in naive and salted mode and read the per-task
shuffle-read record counts from the Spark REST API (the same numbers the committed screenshots show).

Ratio reported = max / min records over the 8 tasks of the heaviest shuffle-read stage
(the screenshots: 5667/346 = 16.4x naive, 2776/935 = 3.0x salted).
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from provenance import REPO, write_result  # noqa: E402

PY = str(REPO / "venv" / "bin" / "python")
ENV = {**os.environ, "PYTHONPATH": str(REPO)}
ENV.pop("PYSPARK_SUBMIT_ARGS", None)
ENV["PYTHONUNBUFFERED"] = "1"


def get(url: str):
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.load(r)


def find_ui_port(log: Path) -> int:
    """Spark logs 'could not bind on port N. Attempting port N+1' at WARN when 4040 is taken
    (the streaming job owns 4040). Poll the candidate ports until one answers as a Spark API."""
    for _ in range(120):
        for port in range(4041, 4046):
            try:
                if get(f"http://127.0.0.1:{port}/api/v1/applications")[0]["name"] == "aggregation_skew_job":
                    return port
            except Exception:
                pass
        time.sleep(1)
    raise RuntimeError("Spark UI port not found")


def stage_stats(port: int) -> dict:
    app = get(f"http://127.0.0.1:{port}/api/v1/applications")[0]["id"]
    best = None
    for st in get(f"http://127.0.0.1:{port}/api/v1/applications/{app}/stages?status=complete"):
        if st["numTasks"] != 8 or st.get("shuffleReadRecords", 0) <= 0:
            continue
        if best is None or st["shuffleReadRecords"] > best["shuffleReadRecords"]:
            best = st
    if best is None:
        raise RuntimeError("no 8-task shuffle-read stage found")
    summ = get(f"http://127.0.0.1:{port}/api/v1/applications/{app}/stages/{best['stageId']}/{best['attemptId']}/taskSummary?quantiles=0,0.5,1")
    mn, med, mx = summ["shuffleReadMetrics"]["readRecords"]
    return {"stage_id": best["stageId"], "tasks": 8, "total_records": best["shuffleReadRecords"],
            "min": mn, "median": med, "max": mx, "max_over_min": round(mx / mn, 2), "max_over_median": round(mx / med, 2)}


def run(mode: str) -> dict:
    log = REPO / "logs" / f"skew_{mode}.log"
    p = subprocess.Popen([PY, "scripts/aggregation_skew_job.py", "--mode", mode, "--hold-seconds", "40"], cwd=REPO, env=ENV,
                         stdout=log.open("w"), stderr=subprocess.STDOUT)
    try:
        port = find_ui_port(log)
        for _ in range(180):
            if "Holding driver open" in log.read_text(errors="ignore"):
                break
            time.sleep(2)
        out = stage_stats(port)
    finally:
        p.wait(timeout=120)
    return out


def main() -> None:
    body = {m: run(m) for m in ("naive", "salted")}
    n, s = body["naive"]["max_over_min"], body["salted"]["max_over_min"]
    body["reduction_pct"] = round((n - s) / n * 100, 1)
    write_result("audit_skew.json", "python oncall/tools/audit_skew.py", body)
    print(json.dumps(body))


if __name__ == "__main__":
    main()
