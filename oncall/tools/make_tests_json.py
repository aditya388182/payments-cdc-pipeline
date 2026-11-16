"""Runs every test suite and writes results/oncall/tests.json with counts per suite (SPEC section 11).

  python oncall/tools/make_tests_json.py

Suites: existing pytest (spark/tests), new pytest (oncall/replay/tests incl. the schema gate), Go with -race
(needs ONCALL_TEST_PG_DSN or reads it from .env), Jest, Playwright (taken from results/oncall/e2e.json, which
`make oncall-e2e` writes, because that run needs the whole stack).
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from provenance import REPO, RESULTS_DIR, write_result  # noqa: E402

LOGS = RESULTS_DIR / "logs"
LOGS.mkdir(parents=True, exist_ok=True)


def env_with_dotenv() -> dict[str, str]:
    env = {**os.environ, "PYTHONPATH": str(REPO), "PYSPARK_PYTHON": sys.executable, "PYSPARK_DRIVER_PYTHON": sys.executable,
           "PATH": f"{Path.home()}/.local/bin:{Path.home()}/go/bin:" + os.environ.get("PATH", "")}
    env.pop("PYSPARK_SUBMIT_ARGS", None)
    dotenv = REPO / ".env"
    if dotenv.exists():
        for line in dotenv.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env.setdefault(k, v)
    if "ONCALL_TEST_PG_DSN" not in env and "ONCALL_PG_DSN" in env:
        env["ONCALL_TEST_PG_DSN"] = env["ONCALL_PG_DSN"]
    return env


def pytest_suite(name: str, path: str, env: dict[str, str]) -> dict:
    xml = LOGS / f"tests_{name}.xml"
    p = subprocess.run([sys.executable, "-m", "pytest", path, "-q", "-p", "no:cacheprovider", f"--junitxml={xml}"], cwd=REPO, env=env,
                       capture_output=True, text=True)
    (LOGS / f"tests_{name}.log").write_text(p.stdout + p.stderr)
    root = ET.parse(xml).getroot()
    suite = root if root.tag == "testsuite" else root.find("testsuite")
    t, f, e, s = (int(suite.get(k, 0)) for k in ("tests", "failures", "errors", "skipped"))
    return {"command": f"python -m pytest {path}", "tests": t, "passed": t - f - e - s, "failed": f + e, "skipped": s, "exit_code": p.returncode}


def go_suite(env: dict[str, str]) -> dict:
    p = subprocess.run(["go", "test", "-race", "-count=1", "-json", "./..."], cwd=REPO / "oncall" / "api", env=env, capture_output=True, text=True)
    (LOGS / "tests_go.json").write_text(p.stdout)
    passed = failed = skipped = 0
    pkgs: dict[str, str] = {}
    for line in p.stdout.splitlines():
        try:
            ev = json.loads(line)
        except ValueError:
            continue
        if ev.get("Test") and ev.get("Action") in ("pass", "fail", "skip"):
            if "/" in ev["Test"]:  # count subtests too; they are real cases
                pass
            passed += ev["Action"] == "pass"
            failed += ev["Action"] == "fail"
            skipped += ev["Action"] == "skip"
        elif not ev.get("Test") and ev.get("Action") in ("pass", "fail") and ev.get("Package"):
            pkgs[ev["Package"].split("/oncall/api/")[-1]] = ev["Action"]
    return {"command": "cd oncall/api && go test -race -count=1 ./...", "tests": passed + failed + skipped, "passed": passed, "failed": failed,
            "skipped": skipped, "packages": len(pkgs), "packages_failed": sum(1 for v in pkgs.values() if v == "fail"), "exit_code": p.returncode,
            "race_detector": True, "postgres_integration": "ONCALL_TEST_PG_DSN" in env}


def jest_suite(env: dict[str, str]) -> dict:
    out = LOGS / "tests_jest.json"
    p = subprocess.run(["npx", "jest", "--ci", "--json", f"--outputFile={out}"], cwd=REPO / "oncall" / "mobile", env=env, capture_output=True, text=True)
    (LOGS / "tests_jest.log").write_text(p.stdout + p.stderr)
    j = json.loads(out.read_text())
    return {"command": "cd oncall/mobile && npx jest --ci", "tests": j["numTotalTests"], "passed": j["numPassedTests"], "failed": j["numFailedTests"],
            "skipped": j["numPendingTests"], "suites": j["numTotalTestSuites"], "exit_code": p.returncode}


def playwright_from_e2e() -> dict:
    f = RESULTS_DIR / "e2e.json"
    if not f.exists():
        return {"tests": None, "passed": None, "note": "results/oncall/e2e.json is missing: run `make oncall-e2e`"}
    j = json.loads(f.read_text())
    return {"command": "make oncall-e2e (Playwright on the Expo web build)", "tests": j["tests"], "passed": j["passed"], "failed": j["failed"],
            "from_commit": j.get("commit"), "generated_at_utc": j.get("generated_at_utc")}


def main() -> None:
    env = env_with_dotenv()
    suites = {
        "pytest_existing_pipeline": pytest_suite("existing", "spark/tests", env),
        "pytest_new_oncall": pytest_suite("new", "oncall/replay/tests", env),
        "go_race": go_suite(env),
        "jest": jest_suite(env),
        "playwright": playwright_from_e2e(),
    }
    total = sum(s.get("tests") or 0 for s in suites.values())
    failed = sum(s.get("failed") or 0 for s in suites.values())
    write_result("tests.json", "python oncall/tools/make_tests_json.py", {"suites": suites, "total_tests": total, "total_failed": failed})
    for k, v in suites.items():
        print(f"{k:28} tests={v.get('tests')} passed={v.get('passed')} failed={v.get('failed')}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
