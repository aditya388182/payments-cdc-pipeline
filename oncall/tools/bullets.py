"""`make bullets`: prints the resume bullets with every number taken from results/oncall/*.json.

A number whose key is absent prints MISSING (never a guess). Items that can only be verified on a phone (push delivery,
the real GitHub login) are listed separately and stay MISSING until BLOCKERS.md Phase 6 is done and recorded.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
RES = ROOT / "results" / "oncall"


def load(name: str) -> dict:
    p = RES / name
    return json.loads(p.read_text()) if p.exists() else {}


def get(d: dict, path: str, fmt: str = "{}") -> str:
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


def main() -> int:
    audit, ws, replay, gate, tests, dk, skew = (load(n) for n in ("audit_summary.json", "ws_load.json", "replay_test.json", "schema_gate.json",
                                                                  "tests.json", "audit_driver_kill.json", "audit_skew.json"))
    base = ws.get("variants", {}).get("baseline", {})
    slow = ws.get("variants", {}).get("slow_clients", {})
    delta, pg = get(dk, "parity.Delta live rows", "{:,}"), get(dk, "parity.Postgres rows", "{:,}")
    scr = audit.get("numbers", {})
    n_ratio = get(scr, "skew_screenshot_naive_ratio", "{:.1f}")
    s_ratio = get(scr, "skew_screenshot_salted_ratio", "{:.1f}")
    pct = get(scr, "skew_screenshot_reduction_pct", "{:.0f}")

    one_page = [
        "Built a Go service that streams live metrics over WebSockets with goroutines and channels and replays failed records over gRPC "
        f"(k6-tested with {get(base, 'connections_established', '{:,}')} concurrent clients, delivery p95 {get(base, 'delivery_latency_ms.p95', '{:.0f}')} ms), "
        "plus a React Native on-call app with push alerts and OAuth 2.0 login.",
        f"Data: streaming CDC (Debezium, Kafka, Spark, Delta Lake) matched {delta} of {pg} rows after a driver kill; idempotent upserts keep a record "
        f"replayed twice to one row ({get(replay, 'lake_rows_for_key_after_duplicate_publish')} lake row, {get(replay, 'audit_rows_for_the_two_replays')} audit rows); "
        f"data skew cut {pct}% ({n_ratio}x to {s_ratio}x max-to-min task records) with salted aggregation.",
    ]
    two_page = [
        "Go console service (chi, pgx): a WebSocket hub pushing throughput, consumer lag, freshness and lag-aware parity every 2 seconds, with a bounded buffer per client "
        f"(with {get(ws, 'fast_clients_unaffected_by_slow_ones.slow_clients_dropped_by_server')} deliberately slow readers the server disconnected them, median "
        f"{get(ws, 'fast_clients_unaffected_by_slow_ones.median_seconds_until_a_slow_client_was_dropped', '{:.0f}')} s after connecting, while the other "
        f"{get(slow, 'connections_established', '{:,}')} clients kept receiving at p95 {get(slow, 'delivery_latency_ms.p95', '{:.0f}')} ms against "
        f"{get(base, 'delivery_latency_ms.p95', '{:.0f}')} ms without them).",
        "gRPC (Protocol Buffers) client that replays dead-lettered records through the existing Python validator (the same record replayed twice: "
        f"{get(replay, 'first_replay_status')} then {get(replay, 'second_replay_status')}); admin-only replay behind GitHub OAuth 2.0 with PKCE and rotating refresh tokens, and an audit log.",
        f"CI gate that rejects breaking Avro schema changes ({get(gate, 'breaking_fixtures_rejected')} of {get(gate, 'breaking_fixtures_total')} breaking fixtures rejected, "
        f"{get(gate, 'compatible_fixtures_accepted')} of {get(gate, 'compatible_fixtures_total')} compatible ones accepted).",
        f"{get(tests, 'suites.pytest_existing_pipeline.passed')} pytest tests on the existing pipeline; {get(tests, 'suites.pytest_new_oncall.passed')} more on the replay service; "
        f"{get(tests, 'suites.go_race.passed')} Go tests under the race detector; {get(tests, 'suites.jest.passed')} Jest tests; {get(tests, 'suites.playwright.passed')} Playwright end-to-end tests.",
        f"k6 WebSocket load test: {get(base, 'connections_established', '{:,}')} clients established, {get(base, 'connections_failed')} failed, "
        f"{get(base, 'unexpected_disconnects')} unexpected disconnects, {get(base, 'seq_order_violations')} out-of-order frames; "
        f"server peak {get(base, 'server.peak_rss_mb', '{:.0f}')} MB RSS and {get(base, 'server.avg_cpu_cores_during_hold', '{:.2f}')} cores on a {get(ws, 'instance_type')} (local, clients on the same box).",
    ]
    phone_only = {
        "push alerts delivered to a physical phone": "MISSING (phone only: BLOCKERS.md section 3)",
        "sign-in with the real GitHub (not the mock)": "MISSING (BLOCKERS.md section 1)",
    }
    print("ONE-PAGE")
    for b in one_page:
        print(" -", b)
    print("\nTWO-PAGE ADDITIONS")
    for b in two_page:
        print(" -", b)
    print("\nVERIFIED ONLY ON A PHONE (until then say: tested with a fake push sender and a mock OAuth server)")
    for k, v in phone_only.items():
        print(f" - {k}: {v}")
    missing = sum(line.count("MISSING") for line in one_page + two_page)
    print(f"\n{missing} MISSING value(s) in the bullets above (only phone-only checks may stay MISSING in the final state).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
