"""Phase 0 audit generator: runs read-only evidence commands against the live stack and the repo,
reads the results files produced by the expensive reruns, and writes docs/oncall/AUDIT.md and
results/oncall/audit_summary.json. Nothing in the evidence column is typed by hand except the two
screenshot transcriptions, which are labelled as such.
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from provenance import REPO, RESULTS_DIR, write_result  # noqa: E402

AUDIT_MD = REPO / "docs" / "oncall" / "AUDIT.md"


def sh(cmd: str, timeout: int = 120) -> str:
    p = subprocess.run(cmd, shell=True, cwd=REPO, capture_output=True, text=True, timeout=timeout)
    return (p.stdout + p.stderr).strip()


def lines(text: str, pat: str | None = None, n: int = 12) -> str:
    ls = [l.rstrip() for l in text.splitlines() if l.strip()]
    if pat:
        ls = [l for l in ls if re.search(pat, l)]
    return "\n".join(ls[:n])


def jload(name: str) -> dict:
    return json.loads((RESULTS_DIR / name).read_text())


rows: list[dict] = []


def add(num, claim, exists, runs, tested, cmds: list[tuple[str, str]], action: str, verdict: str):
    rows.append(dict(num=num, claim=claim, exists=exists, runs=runs, tested=tested, cmds=cmds, action=action, verdict=verdict))


base = jload("baseline.json")
dk = jload("audit_driver_kill.json")
skew = jload("audit_skew.json")

# 1 ---------------------------------------------------------------------------------------------
ps = sh("docker compose -f infra/docker-compose.yml ps --format '{{.Name}}  {{.Status}}'")
conn = sh("curl -s localhost:8083/connectors/payments-postgres-cdc/status | jq -c '{connector:.connector.state,tasks:[.tasks[].state]}'")
par = base["checks"]["parity_checker"]["result_lines"]
subj = sh("curl -s localhost:8081/subjects | jq -c .")
add(1, "The stack runs: Postgres, Debezium, Kafka, Schema Registry, Spark job, Delta on S3-compatible storage",
    "yes", "yes (after the MinIO image replacement, DECISIONS D-001)", "parity checker + chaos scripts",
    [("docker compose -f infra/docker-compose.yml ps", ps), ("connector status", conn), ("schema registry subjects", subj),
     ("rows arriving in Delta (baseline parity checker)", "\n".join(l for l in par if "rows" in l or "PARITY" in l))],
    "none", "TRUE")

# 2 ---------------------------------------------------------------------------------------------
readme = sh("grep -n '100,812' README.md | cut -c1-150")
hist = sh("git log --all -S'100812' --oneline -- . ':!docs' | head -3; git log --all -S'100,812' --format='%h %ad %s' --date=short -- README.md | head -3")
n = dk["parity"]
add(2, '"100,812 of 100,812 rows matched after a driver kill"', "partly", "yes (rerun below)",
    "oncall/tools/audit_driver_kill.py (repo scripts: data_generator, kill_spark.sh, restart, parity_checker)",
    [("where 100,812 appears in the repo", readme),
     ("git history for the number (README only, never a parity output)", hist or "(no other commit)"),
     ("committed parity screenshots (transcribed by eye from the PNGs, not machine-read)",
      "screenshots/01_stage0_parity_pass.png: Postgres rows = 1000, Delta live rows = 1000, PARITY: PASS\n"
      "screenshots/03_chaos_parity_pass.png: Postgres rows = 52361, Delta live rows = 52361, PARITY: PASS"),
     ("rerun on this box (results/oncall/audit_driver_kill.json)",
      f"kill: {dk['kill_output'].splitlines()[0]}\nresumed_from_checkpoint={dk['resumed_from_checkpoint']}\n"
      f"Postgres rows={n['Postgres rows']}  Delta live rows={n['Delta live rows']}  missing={n['missing_in_delta_total']}  "
      f"extra={n['extra_in_delta']}  value_mismatch={n['value_mismatch']}  duplicate_keys={n['duplicate_keys_in_delta']}  "
      f"PARITY: {'PASS' if dk['parity_pass'] else 'FAIL'}")],
    f'Reword. 100,812 is README text with no committed parity output behind it (the largest committed parity screenshot is 52,361). '
    f'Use the measured rerun: "matched {n["Delta live rows"]:,} of {n["Postgres rows"]:,} rows after a driver kill and checkpoint restart".',
    "NOT VERIFIABLE AS WRITTEN (mechanism verified)")

# 3 ---------------------------------------------------------------------------------------------
code = sh("grep -n 'staged.lsn > target.lsn\\|LEDGER_PATH = \\|def already_committed\\|def commit_ledger\\|whenNotMatchedInsertAll' spark/jobs/payments_cdc_job.py")
rp = base["checks"]["replay_offsets_both"]["result_lines"]
add(3, "Idempotent Delta MERGE with an LSN watermark and a batch ledger", "yes", "yes", "scripts/chaos/replay_offsets.py --mode both",
    [("code paths", code), ("baseline: replay_offsets.py --mode both (exit %d)" % base["checks"]["replay_offsets_both"]["exit_code"], "\n".join(rp))],
    "none", "TRUE")

# 4 ---------------------------------------------------------------------------------------------
dlq_off = sh("docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server kafka:9092 --topic payments.transactions.dlq --time -1")
dlq_msg = sh("docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic payments.transactions.dlq --from-beginning --max-messages 1 --property print.key=true 2>/dev/null | cut -c1-420")
inj = sh("grep -E 'PASS|FAIL|INJECTION' logs/inject_bad.log | tail -7 | cut -c1-140")
add(4, "Validator and DLQ", "yes", "yes", "scripts/inject_bad_records.py (5 tests) and spark/tests/test_validator.py",
    [("DLQ is a Kafka topic (not a Delta table): end offsets", dlq_off), ("one DLQ record (key, JSON value: flattened row + rejection_reason + rejected_at)", dlq_msg),
     ("bad records are injected by scripts/inject_bad_records.py (Postgres inserts and one rogue Avro message); run output", inj)],
    "none. Facts for the console: DLQ lives in Kafka topic payments.transactions.dlq (JSON), is append-only (at-least-once), and the "
    "currency rule is a hard-coded set while the merchant rule reads Postgres `merchants` (cached 120 s). The demo scenario therefore uses an unknown merchant.",
    "TRUE")

# 5 ---------------------------------------------------------------------------------------------
add(5, '"23 pytest tests"', "yes", "yes", "python -m pytest spark/tests/ -v",
    [("baseline pytest", "\n".join(base["checks"]["pytest"]["result_lines"][-3:]))], "none", "TRUE (23 passed)")

# 6 ---------------------------------------------------------------------------------------------
tree = sh("git ls-tree -r HEAD --name-only | grep -c '^.github/' || true")
ci = sh("git show df853f6:.github/workflows/ci.yml | grep -n 'compatibility/subjects\\|Skipping\\|exit 0\\|schemas/transactions_v2.avsc\\|runs-on'")
size = sh("wc -c < schemas/transactions_v2.avsc")
run0 = sh("bash scripts/avro_contract_check.sh; echo exit=$?")
add(6, "CI schema check", "only on old PR branches (not in this tree)", "partly", "none in this tree",
    [("workflow files in this branch (count of .github/ paths)", tree), ("the workflow as it exists in history (df853f6)", ci),
     ("schemas/transactions_v2.avsc size in bytes (the CI baseline file)", size),
     ("scripts/avro_contract_check.sh against the live registry, default inputs", run0)],
    "No resume change needed for existing lines, but the gate must be rebuilt: it is a real registry compatibility call (not shape-only), "
    "yet it is skipped when no proposed file exists, the committed baseline is empty, and no workflow is checked in. Phase 5 builds it.",
    "PARTLY TRUE")

# 7 ---------------------------------------------------------------------------------------------
sk = sh("grep -n 'def salted_aggregate\\|def naive_aggregate\\|pmod(_hash\\|repartition(' scripts/aggregation_skew_job.py")
nv, sl = skew["naive"], skew["salted"]
add(7, '"data skew cut 82% (16.4x to 3.0x) with salted aggregation"', "yes", "yes", "oncall/tools/audit_skew.py (reruns the repo job and reads Spark REST task stats)",
    [("mechanism in code", sk),
     ("committed Spark UI screenshots (per-task Shuffle Read Records, transcribed by eye)",
      "07_skew_before.png: min 346 max 5667 -> %.2fx ; 08_skew_after.png: min 935 max 2776 -> %.2fx ; reduction %.1f%%" % (5667/346, 2776/935, (5667/346-2776/935)/(5667/346)*100)),
     ("rerun on this box, whale data (results/oncall/audit_skew.json)",
      f"naive : min {nv['min']:.0f} median {nv['median']:.0f} max {nv['max']:.0f} of {nv['total_records']} records -> max/min {nv['max_over_min']}x\n"
      f"salted: min {sl['min']:.0f} median {sl['median']:.0f} max {sl['max']:.0f} of {sl['total_records']} records -> max/min {sl['max_over_min']}x\n"
      f"reduction {skew['reduction_pct']}%")],
    'Keep, but name the metric: the ratio is max-to-min records per task in the final stage, mechanism is two-phase salted aggregation on (merchant_id, salt) in the aggregation consumer only. '
    f'The rerun on a smaller dataset gave {nv["max_over_min"]}x to {sl["max_over_min"]}x ({skew["reduction_pct"]}%), so the 82% figure rests on the committed screenshots.',
    "PARTLY TRUE (mechanism and direction reproduced; the rerun on smaller data gave a smaller cut, so 82% rests on the screenshots)")

# 8 ---------------------------------------------------------------------------------------------
cfg = sh("grep -n 'converter' connectors/debezium-postgres-connector.json | cut -c1-110")
compat = sh("curl -s localhost:8081/config; echo; curl -s localhost:8081/config/payments.public.transactions-value")
trans = sh("grep -rn 'TRANSITIVE' README.md | cut -c1-120 | head -3; grep -rln 'TRANSITIVE' --include='*.py' --include='*.sh' --include='*.json' --include='*.yml' . 2>/dev/null | grep -v venv | head -2 || true")
dec = sh("grep -n 'from_avro\\|schemas/ids' spark/utils/avro_deserializer.py | head -4")
add(8, "Avro in the path (Schema Registry, schema-id decode)", "yes", "yes", "inject_bad_records.py publishes Avro; whole stack decodes it",
    [("connector converters", cfg), ("Spark decode by schema id", dec),
     ("registry compatibility (global, then subject)", compat), ("README says TRANSITIVE; does any code set it?", trans or "(no code sets it)")],
    "Do not say BACKWARD_TRANSITIVE anywhere: the live subject uses the registry default, BACKWARD, and nothing in the repo configures TRANSITIVE. Avro and Schema Registry claims are true.",
    "TRUE (README's BACKWARD_TRANSITIVE is not reproduced)")

# 9 ---------------------------------------------------------------------------------------------
graf = sh("curl -s -u admin:admin localhost:3000/api/search | jq -c '[.[]|.title]'")
exprs = sh("for f in infra/grafana/dashboards/*.json; do echo \"$f\"; jq -r '[.. | .expr? // empty] | unique | .[]' $f | cut -c1-100 | head -3; done")
prom = sh("curl -s localhost:9091/metrics | grep -E '^payments_cdc_(last_ledger|batch_input|processed_offset)' | head -4 | cut -c1-120")
tg = sh("curl -s localhost:9090/api/v1/targets | jq -c '[.data.activeTargets[]|{job:.labels.job,health}]'")
add(9, "Prometheus and Grafana", "yes", "yes", "live queries", [("Grafana dashboards via API", graf), ("dashboard queries", exprs), ("Pushgateway series", prom), ("Prometheus targets", tg)],
    "none. Note Prometheus only scrapes the Pushgateway; the Go console reads Kafka/Connect/Postgres/lake directly.", "TRUE")

# 10 --------------------------------------------------------------------------------------------
rb = sh("ls runbooks 2>&1 | head -2; git ls-tree -r HEAD --name-only | grep -ci runbook || true")
doc = sh("ls -la documents/ | tail -2; head -c 400 documents/inconsistency_window_sla.md")
add(10, "Runbooks and incident write-up", "runbooks: no. SLA write-up: yes", "n/a", "n/a",
    [("runbooks/ directory and tracked runbook files (README lists 4)", rb), ("documents/", doc)],
    "Do not claim runbooks on a resume: the four files the README names are not in the repo. The inconsistency-window write-up exists.", "PARTLY TRUE")

# write -----------------------------------------------------------------------------------------
resume = [
    ("matched 100,812 of 100,812 rows after a driver kill", "100,812 is README text with no committed parity output; the largest committed parity screenshot is 52,361",
     f'matched {n["Delta live rows"]:,} of {n["Postgres rows"]:,} rows after a driver kill and checkpoint restart (parity checker rerun, 0 missing / 0 extra / 0 mismatched)'),
    ("data skew cut 82% (16.4x to 3.0x)", "metric undefined; reproduced only in direction on smaller data",
     f'data skew cut 82% (16.4x to 3.0x max-to-min task records) with two-phase salted aggregation (rerun on a smaller set: {nv["max_over_min"]}x to {sl["max_over_min"]}x)'),
    ("BACKWARD_TRANSITIVE (README only)", "the registry subject is BACKWARD", "Avro contracts in Schema Registry (BACKWARD compatibility)"),
    ("runbooks (README only)", "runbook files are not in the repo", "(drop the word, or add the files)"),
]
md = ["# Phase 0 audit", "", "Generated by `oncall/tools/audit.py` on %s (commit %s). Evidence is real command output captured by the script; expensive reruns are in `results/oncall/*.json`." %
      (sh("date -u +%Y-%m-%dT%H:%M:%SZ"), sh("git rev-parse --short HEAD")), "",
      "| # | Claim | exists | runs | tested | verdict | resume action |", "|---|---|---|---|---|---|---|"]
for r in rows:
    md.append(f"| {r['num']} | {r['claim']} | {r['exists']} | {r['runs']} | {r['tested']} | {r['verdict']} | {r['action'].splitlines()[0][:160]}{'...' if len(r['action'])>160 else ''} |")
md += ["", "## Evidence", ""]
for r in rows:
    md += [f"### {r['num']}. {r['claim']}", "", f"**Verdict:** {r['verdict']}. **Resume action:** {r['action']}", ""]
    for title, out in r["cmds"]:
        md += [f"`{title}`", "```text", out if out else "(no output)", "```", ""]
md += ["## Resume changes", "", "| Claim as written | Problem | New wording |", "|---|---|---|"]
for a, b, c in resume:
    md.append(f"| {a} | {b} | {c} |")
md += ["", "## Other findings from running things", "",
       "- `scripts/chaos/kill_spark.sh` could not find the job (pgrep pattern vs `python -m` launch) - fixed, DECISIONS D-005.",
       "- `schemas/transactions_v2.avsc` is 0 bytes - DECISIONS D-006.",
       "- `scripts/parity_checker.py` hard-codes `quarantined in (0, 53)` - DECISIONS D-007.",
       "- `req.txt` is unpinned: a fresh install gets `confluent-kafka` 2.16 whose schema-registry client needs `authlib` (`ModuleNotFoundError: authlib`) so `inject_bad_records.py` crashed until `confluent-kafka[avro,schemaregistry]` and `authlib` were added; the repo has no lockfile. Phase 0 adds `req.lock.txt`.",
       "- `scripts/data_generator.py --help` crashes (`%` in an argparse help string).",
       "- No Makefile, CI workflow, runbooks or `.env.example` in the tree - DECISIONS D-003."]
AUDIT_MD.write_text("\n".join(md) + "\n")
write_result("audit_summary.json", "python oncall/tools/audit.py", {
    "rows": [{k: r[k] for k in ("num", "claim", "exists", "runs", "tested", "verdict", "action")} for r in rows],
    "resume_changes": [{"as_written": a, "problem": b, "new_wording": c} for a, b, c in resume],
    "numbers": {"driver_kill_rows_postgres": n["Postgres rows"], "driver_kill_rows_delta": n["Delta live rows"], "pytest_passed": base["checks"]["pytest"].get("passed"),
                "skew_rerun_naive_max_over_min": nv["max_over_min"], "skew_rerun_salted_max_over_min": sl["max_over_min"], "skew_rerun_reduction_pct": skew["reduction_pct"],
                # transcribed by eye from the committed Spark UI screenshots (07_skew_before.png: 346 .. 5667; 08_skew_after.png: 935 .. 2776)
                "skew_screenshot_naive_min": 346, "skew_screenshot_naive_max": 5667, "skew_screenshot_salted_min": 935, "skew_screenshot_salted_max": 2776,
                "skew_screenshot_naive_ratio": 5667 / 346, "skew_screenshot_salted_ratio": 2776 / 935,
                "skew_screenshot_reduction_pct": (5667 / 346 - 2776 / 935) / (5667 / 346) * 100},
})
print("wrote", AUDIT_MD)
