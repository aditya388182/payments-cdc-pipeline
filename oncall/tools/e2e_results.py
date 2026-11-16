"""Turns Playwright's JSON report into results/oncall/e2e.json (counts, per-test status, screenshot files)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from provenance import REPO, write_result  # noqa: E402

report = json.loads((REPO / "oncall/mobile/test-results/results.json").read_text())
tests = []


def walk(suite: dict) -> None:
    for spec in suite.get("specs", []):
        for t in spec.get("tests", []):
            res = t.get("results", [{}])[-1]
            tests.append({"title": spec["title"], "status": res.get("status"), "duration_ms": res.get("duration")})
    for s in suite.get("suites", []):
        walk(s)


for s in report.get("suites", []):
    walk(s)
shots = sorted(p.name for p in (REPO / "docs/oncall/screenshots").glob("*.png"))
passed = sum(1 for t in tests if t["status"] == "passed")
write_result("e2e.json", "make oncall-e2e", {
    "tests": len(tests), "passed": passed, "failed": len(tests) - passed, "per_test": tests, "screenshots": shots,
    "stats": report.get("stats", {}),
})
print(f"e2e: {passed}/{len(tests)} passed, {len(shots)} screenshots")
