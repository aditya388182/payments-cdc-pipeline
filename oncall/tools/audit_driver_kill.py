"""Audit row 2: rerun the driver-kill failure test with the repo's own scripts and record parity.

generator (background) -> kill -9 the Spark driver mid-stream (scripts/chaos/kill_spark.sh)
-> restart the job from its checkpoint -> wait for the stream to drain -> parity checker.
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

PY = str(REPO / "venv" / "bin" / "python")
ENV = {**os.environ, "PYTHONPATH": str(REPO)}
ENV.pop("PYSPARK_SUBMIT_ARGS", None)
LOG = REPO / "logs" / "spark_job_driverkill.log"


def sh(cmd: str, **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, shell=True, cwd=REPO, env=ENV, capture_output=True, text=True, **kw)


def main() -> None:
    gen = subprocess.Popen([PY, "scripts/data_generator.py", "--rate", "20", "--duration", "45"], cwd=REPO, env=ENV,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(20)
    kill = sh("./scripts/chaos/kill_spark.sh")
    job = subprocess.Popen([PY, "-m", "spark.jobs.payments_cdc_job", "--starting-offsets", "earliest"], cwd=REPO, env=ENV,
                           stdout=LOG.open("w"), stderr=subprocess.STDOUT)
    gen.wait()
    deadline = time.time() + 300
    while time.time() < deadline:
        txt = LOG.read_text(errors="ignore")
        if txt.count("empty (no Kafka records)") >= 1 and "finished" in txt:
            # an empty batch AFTER the generator stopped means the topic is drained
            if txt.rfind("empty (no Kafka records)") > txt.rfind("finished"):
                break
        time.sleep(3)
    par = sh(f"{PY} scripts/parity_checker.py", timeout=900)
    out = par.stdout + par.stderr
    (RESULTS_DIR / "logs" / "audit_driver_kill_parity.log").write_text(out)
    nums = {k: int(v) for k, v in re.findall(r"\[parity\] ([A-Za-z_ ()]+?)\s*= (\d+)", out)}
    resumed = "resuming from checkpoint" in LOG.read_text(errors="ignore")
    write_result("audit_driver_kill.json", "python oncall/tools/audit_driver_kill.py", {
        "kill_output": kill.stdout.strip(),
        "resumed_from_checkpoint": resumed,
        "parity_exit_code": par.returncode,
        "parity_pass": "PARITY: PASS" in out,
        "parity": nums,
        "job_pid_now": job.pid,
    })
    print("PARITY:", "PASS" if "PARITY: PASS" in out else "FAIL", nums)


if __name__ == "__main__":
    main()
