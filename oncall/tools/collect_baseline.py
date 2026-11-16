"""Run the repo's existing checks BEFORE any pipeline change and record baseline.json.

Each check's full output goes to results/oncall/logs/baseline_<name>.log; baseline.json keeps
exit code, duration and the lines that carry the result.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from provenance import REPO, RESULTS_DIR, write_result  # noqa: E402

ENV = {**os.environ, "PYTHONPATH": str(REPO)}
ENV.pop("PYSPARK_SUBMIT_ARGS", None)
ENV["PYSPARK_PYTHON"] = ENV["PYSPARK_DRIVER_PYTHON"] = sys.executable  # workers must match the driver (box default python is 3.12)

CHECKS = {
    "pytest": ("python -m pytest spark/tests/ -v", 900),
    "parity_checker": ("python scripts/parity_checker.py", 900),
    "replay_offsets_both": ("python scripts/chaos/replay_offsets.py --mode both", 1800),
}
KEEP = re.compile(r"(passed|failed|error|PARITY|\[parity\]|REPLAY|LEDGER|BEFORE|AFTER|Kafka records|All Day-3|Traceback|Assertion)", re.I)


def run(name: str, cmd: str, timeout: int) -> dict:
    t0 = time.time()
    p = subprocess.run(cmd, shell=True, cwd=REPO, env=ENV, capture_output=True, text=True, timeout=timeout)
    out = (p.stdout or "") + (p.stderr or "")
    (RESULTS_DIR / "logs" / f"baseline_{name}.log").write_text(out)
    lines = [l.strip() for l in out.splitlines() if KEEP.search(l) and "Stage" not in l and "WARN" not in l]
    return {"command": cmd, "exit_code": p.returncode, "seconds": round(time.time() - t0, 1), "result_lines": lines[-40:]}


def main() -> None:
    args = sys.argv[1:]
    out = "baseline.json"
    if "--out" in args:
        i = args.index("--out")
        out = args[i + 1]
        del args[i : i + 2]
    only = args or list(CHECKS)
    body = {name: run(name, *CHECKS[name]) for name in only}
    m = re.search(r"(\d+) passed", " ".join(body.get("pytest", {}).get("result_lines", [])))
    if m:
        body["pytest"]["passed"] = int(m.group(1))
    write_result(out, "python oncall/tools/collect_baseline.py " + " ".join(sys.argv[1:]), {"stack": "existing pipeline, SeaweedFS in place of MinIO" + ("" if out == "baseline.json" else " (re-run after a pipeline-side change)"), "checks": body})
    print("wrote results/oncall/" + out)


if __name__ == "__main__":
    main()
