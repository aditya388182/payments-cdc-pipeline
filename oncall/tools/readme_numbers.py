"""Generates the results-derived text blocks: the README numbers table and the "every results value" dump for DONE.md.

  python oncall/tools/readme_numbers.py --readme   # rewrite the block between the oncall:numbers markers in README.md
  python oncall/tools/readme_numbers.py --done     # rewrite the block between the results markers in docs/oncall/DONE.md

Nothing is typed by hand: every cell is read from results/oncall/*.json and shows MISSING when its key is absent.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
RES = ROOT / "results" / "oncall"


def load(name: str) -> dict:
    p = RES / name
    return json.loads(p.read_text()) if p.exists() else {}


def g(d: dict, path: str, fmt: str = "{}") -> str:
    cur: Any = d
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur and cur[part] is not None:
            cur = cur[part]
        else:
            return "MISSING"
    try:
        return fmt.format(cur)
    except (ValueError, TypeError):
        return "MISSING"


def numbers_table() -> str:
    dk, audit, replay, ws, gate, tests, e2e = (load(n) for n in ("audit_driver_kill.json", "audit_summary.json", "replay_test.json", "ws_load.json",
                                                                 "schema_gate.json", "tests.json", "e2e.json"))
    base = ws.get("variants", {}).get("baseline", {})
    slow = ws.get("variants", {}).get("slow_clients", {})
    nums = audit.get("numbers", {})
    rows = [
        ("Rows matched after a driver kill (parity checker rerun, this box)", f"{g(dk, 'parity.Delta live rows', '{:,}')} of {g(dk, 'parity.Postgres rows', '{:,}')}", "audit_driver_kill.json"),
        ("Existing pipeline pytest tests", g(tests, "suites.pytest_existing_pipeline.passed"), "tests.json"),
        ("Skew, max-to-min task records (committed Spark UI screenshots)", f"{g(nums, 'skew_screenshot_naive_ratio', '{:.1f}')}x to {g(nums, 'skew_screenshot_salted_ratio', '{:.1f}')}x ({g(nums, 'skew_screenshot_reduction_pct', '{:.0f}')}% cut)", "audit_summary.json"),
        ("Skew rerun on a smaller dataset (salted aggregation)", f"{g(nums, 'skew_rerun_naive_max_over_min', '{:.1f}')}x to {g(nums, 'skew_rerun_salted_max_over_min', '{:.1f}')}x ({g(nums, 'skew_rerun_reduction_pct', '{:.0f}')}% cut)", "audit_summary.json"),
        ("Same DLQ record replayed twice", f"{g(replay, 'first_replay_status')} then {g(replay, 'second_replay_status')}", "replay_test.json"),
        ("Lake rows for that key (after both replays and a direct duplicate publish to Kafka)", g(replay, "lake_rows_for_key_after_duplicate_publish"), "replay_test.json"),
        ("Audit rows for the two replays", g(replay, "audit_rows_for_the_two_replays"), "replay_test.json"),
        ("WebSocket clients established / failed (k6, local, 1,000 target)", f"{g(base, 'connections_established', '{:,}')} / {g(base, 'connections_failed')}", "ws_load.json"),
        ("Snapshots received / expected for the time each client was connected (median, min)", f"{g(base, 'received_over_expected_median', '{:.2f}')} / {g(base, 'received_over_expected_min', '{:.2f}')} (median client received {g(base, 'snapshots_received_per_client_median')})", "ws_load.json"),
        ("Delivery latency p50 / p95 / p99 (ms)", f"{g(base, 'delivery_latency_ms.p50', '{:.1f}')} / {g(base, 'delivery_latency_ms.p95', '{:.1f}')} / {g(base, 'delivery_latency_ms.p99', '{:.1f}')}", "ws_load.json"),
        ("Unexpected disconnects / out-of-order frames", f"{g(base, 'unexpected_disconnects')} / {g(base, 'seq_order_violations')}", "ws_load.json"),
        ("Server peak RSS (MB) / average CPU cores during the hold", f"{g(base, 'server.peak_rss_mb')} / {g(base, 'server.avg_cpu_cores_during_hold', '{:.2f}')}", "ws_load.json"),
        ("Slow-client run: slow readers closed by the server", g(ws, "fast_clients_unaffected_by_slow_ones.slow_clients_dropped_by_server"), "ws_load.json"),
        ("Slow-client run: fast clients' delivery p95 / p99 (ms) vs baseline", f"{g(slow, 'delivery_latency_ms.p95', '{:.1f}')} / {g(slow, 'delivery_latency_ms.p99', '{:.1f}')} vs {g(base, 'delivery_latency_ms.p95', '{:.1f}')} / {g(base, 'delivery_latency_ms.p99', '{:.1f}')}", "ws_load.json"),
        ("Avro gate: breaking fixtures rejected / compatible accepted", f"{g(gate, 'breaking_fixtures_rejected')} of {g(gate, 'breaking_fixtures_total')} / {g(gate, 'compatible_fixtures_accepted')} of {g(gate, 'compatible_fixtures_total')}", "schema_gate.json"),
        ("New pytest tests (replay-svc, progress events, schema gate)", g(tests, "suites.pytest_new_oncall.passed"), "tests.json"),
        ("Go tests under the race detector", g(tests, "suites.go_race.passed"), "tests.json"),
        ("Jest tests", g(tests, "suites.jest.passed"), "tests.json"),
        ("Playwright end-to-end tests (Expo web build, mock GitHub)", f"{g(e2e, 'passed')} of {g(e2e, 'tests')}", "e2e.json"),
        ("Screenshots captured", str(len(e2e.get("screenshots", []))) if e2e else "MISSING", "e2e.json"),
    ]
    out = ["| Measure | Value | Source |", "|---|---|---|"]
    out += [f"| {a} | {b} | `results/oncall/{c}` |" for a, b, c in rows]
    ctx = f"Load test label: {g(ws, 'label')}. Instance type: {g(ws, 'instance_type')} ({g(ws, 'vcpus')} vCPUs). Generated by `oncall/tools/readme_numbers.py`."
    return "\n".join(out) + "\n\n" + ctx + "\n"


def resume_table() -> str:
    audit = load("audit_summary.json")
    rows = audit.get("resume_changes", [])
    if not rows:
        return "MISSING (run `python oncall/tools/audit.py`)\n"
    out = ["| Claim as written | What was wrong | Exact new wording |", "|---|---|---|"]
    out += [f"| {r['as_written']} | {r['problem']} | {r['new_wording']} |" for r in rows]
    return "\n".join(out) + "\n"


def flatten(d: Any, prefix: str = "") -> list[tuple[str, Any]]:
    out: list[tuple[str, Any]] = []
    if isinstance(d, dict):
        for k, v in d.items():
            out += flatten(v, f"{prefix}.{k}" if prefix else k)
    elif isinstance(d, list):
        if len(d) <= 8 and all(not isinstance(x, (dict, list)) for x in d):
            out.append((prefix, d))
        else:
            for i, v in enumerate(d[:40]):
                out += flatten(v, f"{prefix}[{i}]")
            if len(d) > 40:
                out.append((prefix + "[...]", f"{len(d) - 40} more items in the file"))
    else:
        out.append((prefix, d))
    return out


SKIP_KEYS = re.compile(r"(result_lines|local_messages|registry_messages|per_test|steps\[|\.fixtures\[|kill_output)")


def all_values() -> str:
    parts = []
    for f in sorted(RES.glob("*.json")):
        data = json.loads(f.read_text())
        lines = [f"- `{k}` = {json.dumps(v) if not isinstance(v, str) else v}" for k, v in flatten(data) if not SKIP_KEYS.search(k)]
        parts.append(f"<details><summary><code>results/oncall/{f.name}</code> ({len(lines)} values)</summary>\n\n" + "\n".join(lines) + "\n\n</details>")
    return "\n\n".join(parts) + "\n"


def replace_block(path: Path, start: str, end: str, body: str) -> None:
    text = path.read_text()
    pat = re.compile(re.escape(start) + r".*?" + re.escape(end), re.S)
    if not pat.search(text):
        raise SystemExit(f"markers {start} ... {end} not found in {path}")
    path.write_text(pat.sub(lambda m: f"{start}\n{body}{end}", text))


def main() -> None:
    if "--readme" in sys.argv:
        replace_block(ROOT / "README.md", "<!-- oncall:numbers:start -->", "<!-- oncall:numbers:end -->", numbers_table())
        print("README numbers table updated")
    if "--done" in sys.argv:
        body = "### Numbers table\n\n" + numbers_table() + "\n### Every results value\n\n" + all_values()
        done = ROOT / "docs" / "oncall" / "DONE.md"
        replace_block(done, "<!-- results:start -->", "<!-- results:end -->", body)
        replace_block(done, "<!-- resume:start -->", "<!-- resume:end -->", resume_table())
        print("DONE.md results and resume blocks updated")
    if len(sys.argv) == 1:
        print(numbers_table())


if __name__ == "__main__":
    main()
