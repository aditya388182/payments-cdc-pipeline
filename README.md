# Payments CDC Ledger: Exactly-Once Real-Time Ingestion

![Python](https://img.shields.io/badge/Python-3.10-3776AB?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft%207.6.1-231F20?logo=apachekafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.1-00ADD4?logo=databricks&logoColor=white)
![Debezium](https://img.shields.io/badge/Debezium-2.5.4-333333)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-Observability-F46800?logo=grafana&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green.svg)

Payments CDC Ledger is an operational, exactly-once change-data-capture pipeline for a payments transaction table.

It tails Postgres logical replication with Debezium, carries Avro through Kafka and Confluent Schema Registry, applies a synchronous validation gate in PySpark Structured Streaming, and upserts into Delta Lake on MinIO under an LSN monotonicity guard plus a batch ledger. Deleted keys are retained as tombstones so a late event cannot resurrect a row the LSN watermark already closed.

This is **not** a Bronze → Silver → Gold analytics stack and it does **not** use dbt, Airflow, or a medallion layout. The Delta table must match live Postgres after every insert, update, and delete — including after a hard driver kill.

> Local checkout on this machine is `project_4_1/`. On EC2 the same repo lives at `~/payments-cdc-pipeline` (branch `day6fixes`). The architecture is Project 1 from the six-day plan.

---

## The claim this project actually defends

**Checkpointing alone does not give you exactly-once.**

Structured Streaming checkpoints *source offsets*, not the side effects of your `foreachBatch` body. If the driver dies after the Delta `MERGE` commits but before the offset checkpoint is written, the batch replays on restart and the `MERGE` runs a second time. Most pipelines that advertise exactly-once are relying on checkpointing and have never induced that specific failure.

This pipeline carries two independent mechanisms, because they defend against two different failures:

| Mechanism | Defends against | Implementation |
| :-- | :-- | :-- |
| **LSN monotonicity guard** | A stale or replayed *event* re-applying | `whenMatchedUpdateAll(condition="staged.lsn > target.lsn")` — strict `>`, so an equal-LSN replay is a pure no-op |
| **Batch ledger** | A replayed *micro-batch* re-applying | `_batch_ledger` Delta table on `(app_id, batch_id, committed_at)`, exact-match lookup before any write |

> Delta OSS supports `txnAppId` / `txnVersion` on DataFrame writes, but a `MERGE` inside `foreachBatch` needs an explicit ledger to get the same guarantee. That is why `_batch_ledger` exists as a separate table rather than a write option.

Both are exercised by `scripts/chaos/replay_offsets.py --mode both`. Replay harnesses call `process_batch(..., commit=False)` so a fake `batch_id` cannot poison the ledger.

---

## Architecture Overview

```text
Application / scripts/data_generator.py
        │  INSERT / UPDATE / DELETE
        ▼
 PostgreSQL 15  (wal_level=logical, pgoutput)
        │  logical decoding
        ▼
 Debezium 2.5.4.Final  (Avro, heartbeat, tombstones.on.delete)
        │  connector: payments-postgres-cdc
        ▼
 Kafka KRaft (CP 7.6.1)   topic: payments.public.transactions (3 partitions)
        │                 + payments.transactions.dlq
        ▼
 Confluent Schema Registry   subject: payments.public.transactions-value
        │                     compatibility: BACKWARD_TRANSITIVE
        ▼
 ┌─────────────────────────────────────────────────────────────┐
 │  PySpark 3.5 Structured Streaming  (foreachBatch, 10s)      │
 │                                                             │
 │  schema-id Avro decode                                      │
 │       coalesce(after, before)   ← deletes have null after   │
 │       validate()                ← deletes skip amt/ccy/merch│
 │       write_to_dlq(invalid)                                 │
 │       dedup_latest(lsn, offset)                             │
 │       MERGE  staged.lsn > target.lsn                        │
 │              whenNotMatchedInsertAll()  ← tombstones in     │
 │       commit_ledger(commit=True|False)                      │
 │       push_metrics()            ← fail-open                 │
 └─────────────────────────────────────────────────────────────┘
        │
        ▼
 Delta Lake 3.1 on MinIO
   s3a://payments-lake/transactions     (CDF enabled)
   s3a://payments-lake/_batch_ledger
   s3a://payments-lake/checkpoints/payments-cdc
        │
        ├── scripts/parity_checker.py      (is_delete = false)
        ├── scripts/lag_sidecar.py         (topic end offsets)
        └── Pushgateway → Prometheus → Grafana
```

Spark runs **on the host** as `local[2]`. Everything else is Docker Compose. That is a deliberate design decision: Spark UI and checkpoint debugging stay on the host filesystem, and we do not pay ~2 GB of container overhead on a 16 GB box / `t3.xlarge`.

---

## Tech Stack

| Category | Technologies |
| :-- | :-- |
| **Programming** | Python 3.10 (`venv/`, not `.venv`) |
| **Source of truth** | PostgreSQL 15, logical replication (`pgoutput`) |
| **CDC** | Debezium 2.5.4.Final (image **pinned** — `:latest` resolved to 3.x and failed to load on the Java 11 Connect image) |
| **Transport** | Confluent Platform 7.6.1 / Apache Kafka 3.6, KRaft (no ZooKeeper) |
| **Contracts** | Avro + Schema Registry, `BACKWARD_TRANSITIVE` |
| **Stream processing** | PySpark 3.5 Structured Streaming, `foreachBatch` |
| **Lakehouse** | Delta Lake 3.1.0 on MinIO (S3A), Change Data Feed on |
| **Quality gate** | In-process validator + Kafka DLQ (GE monitor is post-commit, not a write-path gate) |
| **Exactly-once** | Delta `MERGE` + LSN watermark + `_batch_ledger` + Spark checkpoint |
| **Observability** | Pushgateway, Prometheus (`honor_labels: true`), Grafana file provisioning |
| **Containerization** | Docker Compose on Ubuntu EC2 |

---

## Key Highlights

- Built an end-to-end **Postgres → Debezium → Kafka → Spark → Delta** payments ledger with a measured **`PARITY: PASS` of 100,812 = 100,812** live rows (2026-08-31).
- Implemented **exactly-once as two cooperating mechanisms**, not a slogan: an LSN monotonicity predicate on `MERGE`, and a durable `_batch_ledger` so a replayed `batch_id` is a no-op.
- Chose a **soft-delete / tombstone policy** so a late-arriving update cannot resurrect a key the watermark already passed. Physical deletes would punch a hole in that watermark.
- Flattened Debezium envelopes with **`coalesce(after, before)`** so `op='d'` rows keep their primary key when `after` is null.
- Put a **synchronous validation gate inside `foreachBatch`** (UUID, merchant registry, amount, currency) with an explicit **delete exemption** on amount / currency / merchant — UUID still enforced.
- Made the **schema-id Avro path** the production default and proved a live `ALTER TABLE ADD COLUMN` survived without a job restart. Schema Registry rejects a field rename (`currency` → `ccy`) with HTTP 409 and CI goes red.
- Isolated merchant-level skew mitigation to the **aggregation consumer only** — measured **16.4× → 3.0×**. The `MERGE` is never salted; the Kafka key stays `transaction_id`.
- Exposed the **inconsistency window as PromQL** — `time() - last_ledger_commit_timestamp`, not a pre-computed delta. Measured reset: **1,281s (idle) → 6.25s (post-commit) → 8.43s**.
- Computed consumption lag as **sidecar topic end offset − `query.lastProgress` processed offset**. Spark Structured Streaming has no Kafka consumer group to scrape.

---

## Skills Demonstrated

- Change data capture and logical replication internals (LSN, `pgoutput`, Debezium envelope, heartbeat)
- Exactly-once streaming design (checkpoints vs application ledger vs MERGE watermark)
- Schema evolution and contract testing against Schema Registry
- Lakehouse upserts, Change Data Feed, and tombstone-aware reconciliation
- Operational observability that is allowed to fail without poisoning the ledger
- Chaos / replay / late-delete test harnesses, not only happy-path demos

---

## Scale & Load Characteristics

Measured on an Ubuntu EC2 `t3.xlarge`, Spark `local[2]`, three Kafka partitions, 20-merchant registry.

| Metric | Value |
| :-- | :-- |
| **Live Postgres / Delta rows** | 100,812 = 100,812 (`PARITY: PASS`, 2026-08-31) |
| **Merchants in registry** | 20 |
| **Generator mix** | ~70% INSERT / 20% UPDATE / 10% DELETE |
| **Burst used in Day 6** | 20 tps × 15s → 300 ops (210 / 60 / 30) |
| **Micro-batch trigger** | 10 seconds |
| **Observed batch on resume** | 150 in / 150 merged / 0 DLQ (batches 114, 115) |
| **Batch wall time on `local[2]`** | 16–42 seconds while catching up (trigger then reports "falling behind") |
| **Kafka high-water (sidecar)** | ~63,500–63,650 per partition at that snapshot |
| **Inconsistency window (busy)** | resets to ~6–8s after commit; climbs until the next batch |
| **Inconsistency window (idle)** | climbs without bound — empty Kafka batches skip the metrics push (known gap) |

This is a single-node teaching / interview system. It is sized to prove the mechanisms, not to saturate a cluster.

---

## Data Model — Ledger table

Delta path: `s3a://payments-lake/transactions` (CDF on). Live rows are `is_delete = false`.

| Column | Source | Notes |
| :-- | :-- | :-- |
| `transaction_id` | PK (UUID) | MERGE join key |
| `merchant_id` | string | validated against an in-process merchant registry |
| `amount_minor` | long | integer minor units; deletes are exempt from `>= 0` |
| `currency` | ISO code | deletes exempt from allow-list |
| `status` | string | business status from the source row |
| `lsn` | Debezium `source.lsn` | MERGE predicate `staged.lsn > target.lsn` |
| `is_delete` | derived from `op = 'd'` | tombstone flag; never physically removed |
| `op` | Debezium `c` / `u` / `d` / `r` | `r` (snapshot) treated as insert |
| `offset` / `partition` | Kafka | carried for deterministic `dedup_latest` |

`_batch_ledger` is a separate Delta table: `(app_id, batch_id, committed_at)`. It is the second exactly-once mechanism. `process_batch(..., commit=False)` writes **nothing** here, so replay harnesses cannot poison the high-water `batch_id`.

---

## Critical Correctness Safeguards

These four rules are load-bearing. Breaking any one of them looks fine in a demo and is fatal in production.

| # | Rule | Why it exists |
| :-: | :-- | :-- |
| **1** | `process_batch(..., commit: bool = True)` | Replay / offset harnesses call `commit=False`. A fake `batch_id=999_999_999` committed to the ledger would skip every real batch afterward. |
| **2** | `coalesce(after.*, before.*)` on flatten | Debezium sets `after = null` on `op='d'`. Without coalesce the PK becomes null and the delete is dropped. |
| **3** | `is_delete=true` skips amount, currency, **and merchant**; UUID still required | A before-image can carry a retired merchant or a null amount. Dropping that tombstone lets a late update resurrect the key. |
| **4** | Parity reads Delta with `is_delete = false` | Tombstones are supposed to exist. Counting them against Postgres live rows is a false fail. |

---

## Quick Start & Verification

### 1. Prerequisites

- Ubuntu / EC2 instance (this repo was run via VS Code Remote-SSH)
- Docker Compose
- Python 3.10 virtualenv at `venv/` (**not** `.venv`)
- **Always** `unset PYSPARK_SUBMIT_ARGS` before any Spark process

A stale `PYSPARK_SUBMIT_ARGS` value silently overrides `spark.jars.packages` and produces a `ClassNotFoundException` that looks like a dependency bug in your own code.

```bash
cd project_4_1
python3 -m venv venv
source venv/bin/activate
unset PYSPARK_SUBMIT_ARGS
pip install -r req.txt
```

### 2. Start the data plane

```bash
cd infra
docker compose up -d
docker compose ps
cd ..
./infra/kafka/topics.sh
```

Expect `postgres`, `kafka`, `schema-registry`, `connect`, `minio`, `pushgateway`, `prometheus`, `grafana` as `Up`. First start takes 2–4 minutes while Connect downloads the Debezium plugin.

Register the connector if it is not already running:

```bash
curl -s http://localhost:8083/connectors/payments-postgres-cdc/status
# if 404:
curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/debezium-postgres-connector.json
```

Initialize the lake if this is a blank MinIO:

```bash
PYTHONPATH=. python scripts/init_delta_table.py
```

### 3. Start the stream + sidecar

```bash
# Terminal 1 — one driver only against this checkpoint
PYTHONPATH=. python -m spark.jobs.payments_cdc_job --starting-offsets latest

# Terminal 2
python scripts/lag_sidecar.py
```

`--starting-offsets` applies **only** when the checkpoint is absent. If you see

```text
[stream] checkpoint EXISTS ... startingOffsets is IGNORED
```

that is correct. Do not start a second driver against `s3a://payments-lake/checkpoints/payments-cdc`.

Healthy startup logs:

```text
Startup – loaded 20 merchants
[stream] using deserialize_by_schema_id (schema-evolution path)
[stream] Streaming query started. Awaiting termination...
COMMITTED batch N @ ...
```

### 4. Prove correctness

```bash
python scripts/data_generator.py --rate 20 --duration 15
PYTHONPATH=. python scripts/parity_checker.py
```

You want `PARITY: PASS` with `missing`, `unaccounted`, `extra`, `value_mismatch`, `duplicate_keys` all zero. Tombstones are excluded by the checker.

### 5. Prove observability

```bash
curl -s http://localhost:9091/metrics | grep payments_cdc
curl -sG 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=time()-payments_cdc_last_ledger_commit_timestamp_seconds'
```

After a real `COMMITTED batch` the window should be seconds, not hours. After the generator stops it will climb — empty Kafka batches do not push metrics. That is documented, not a MERGE failure.

### 6. Access the UIs

| Service | URL | Credentials | What to look at |
| :-- | :-- | :-- | :-- |
| **Grafana** | http://localhost:3000 | `admin` / `admin` | Payments CDC → Inconsistency Window, Pipeline Health, Partition Balance |
| **Prometheus** | http://localhost:9090 | — | `payments_cdc_*` gauges |
| **Pushgateway** | http://localhost:9091 | — | last push from the driver and the sidecar |
| **MinIO** | http://localhost:9001 | `minioadmin` / `minioadmin` | bucket `payments-lake` |
| **Schema Registry** | http://localhost:8081 | — | subject `payments.public.transactions-value` |
| **Kafka Connect** | http://localhost:8083 | — | connector `payments-postgres-cdc` |
| **Spark UI** | http://localhost:4040 | — | streaming query; a second Spark process (parity) falls over to 4041 |

File-provisioned Grafana dashboards survive `docker restart grafana`. Confirm with:

```bash
curl -s -u admin:admin http://localhost:3000/api/search
```

---

## How to Run Each Proof

Every claim in this README maps to a command you can run. That is what makes the project auditable rather than assertable.

| Claim | Command |
| :-- | :-- |
| Postgres and Delta agree, tombstones excluded | `PYTHONPATH=. python scripts/parity_checker.py` |
| Replaying committed offsets is a no-op | `PYTHONPATH=. python scripts/chaos/replay_offsets.py --mode both` |
| Snapshot (`op='r'`) rows are handled as inserts | `PYTHONPATH=. python scripts/snapshot_transition_checker.py` |
| A late lower-LSN update cannot resurrect a tombstone | `PYTHONPATH=. python scripts/inject_late_delete.py` |
| Invalid records are quarantined, never merged | `PYTHONPATH=. python scripts/inject_bad_records.py` |
| The pipeline recovers from a hard driver kill | `./scripts/chaos/kill_spark.sh`, restart the job, then parity |
| A breaking schema change is rejected | `bash scripts/avro_contract_check.sh` |
| Dedup and validation logic are correct in isolation | `PYTHONPATH=. pytest spark/tests/ -v` |
| Merchant skew is isolated to the aggregation consumer | `PYTHONPATH=. python scripts/aggregation_skew_job.py` |

---

## Project Structure

This is the **actual** tree of this checkout (`tree -I 'venv|.git|__pycache__|.pytest_cache|.DS_Store'`). Paths in commands above match these files. Do not look for `requirements.txt`, `docs/screenshots/`, or `docs/inconsistency_window_sla.md` — those names are from an earlier draft.

```text
project_4_1/
├── connectors/
│   └── debezium-postgres-connector.json
├── docs/
│   ├── daily_log.md
│   └── day6_logs/
├── documents/
│   └── inconsistency_window_sla.md      # measured window + SLA (not under docs/)
├── great_expectations/
│   └── payments_trend_monitor.py        # post-commit trend monitor, NOT a write-path gate
├── infra/
│   ├── docker-compose.yml               # Postgres, Kafka, SR, Connect, MinIO, Prom, Grafana
│   ├── grafana/
│   │   ├── dashboards/
│   │   │   ├── inconsistency_window.json
│   │   │   ├── partition_balance.json
│   │   │   └── pipeline_health.json
│   │   └── provisioning/
│   │       ├── dashboards/dashboards.yml
│   │       └── datasources/ds.yml       # uid: prometheus
│   ├── kafka/
│   │   └── topics.sh
│   ├── postgres/
│   │   └── init.sql
│   └── prometheus/
│       └── prometheus.yml               # honor_labels: true on Pushgateway
├── README.md
├── req.txt                              # pinned Python deps (not requirements.txt)
├── runbooks/                            # present; populate with Day-6 runbook markdown if missing
├── schemas/
│   └── transactions_v2.avsc             # live subject dump — not transactions_v1.avsc
├── screenshots/                         # repo-root, not docs/screenshots/
│   ├── 01_stage0_parity_pass.png
│   ├── 02_chaos_recovery_spark_ui.png
│   ├── 03_chaos_parity_pass.png
│   ├── 04_offset_replay_unchanged.png
│   ├── 05_schema_evolution_no_restart.png
│   ├── 05_schema_evolution_no_restart_2.png
│   ├── 06_breaking_change_rejected.png
│   ├── 07_skew_before.png.pdf
│   ├── 08_skew_after.png.pdf
│   ├── 11_ci_pass.png
│   └── 12_ci_schema_fail copy.png
├── scripts/
│   ├── aggregation_skew_job.py          # two-phase salting; never touches MERGE / Kafka key
│   ├── avro_contract_check.sh           # fail-closed compatibility probe
│   ├── chaos/
│   │   ├── kill_connect.sh
│   │   ├── kill_spark.sh
│   │   └── replay_offsets.py            # must call process_batch(commit=False)
│   ├── data_generator.py
│   ├── init_delta_table.py
│   ├── inject_bad_records.py
│   ├── inject_late_delete.py
│   ├── lag_sidecar.py                   # topic end offsets → Pushgateway
│   ├── parity_checker.py                # filter is_delete = false
│   └── snapshot_transition_checker.py
├── spark/
│   ├── jobs/
│   │   └── payments_cdc_job.py          # one def each: commit_ledger / merge_batch / run_stream
│   ├── tests/
│   │   ├── conftest.py
│   │   ├── test_dedup.py                # imports production dedup_latest
│   │   └── test_validator.py
│   └── utils/
│       ├── avro_deserializer.py         # schema-id path, coalesce flatten
│       ├── dlq_writer.py                # MerchantRegistry + write_to_dlq
│       ├── metrics_listener.py          # fail-open Pushgateway client
│       └── validator.py                 # sync gate + delete exemptions
└── terraform/                           # reserved; not required to run the compose stack
```

<details>
<summary><strong>Path corrections vs the earlier interview draft</strong></summary>

| Draft claimed | What is actually in this repo |
| :-- | :-- |
| `requirements.txt` | `req.txt` |
| `docs/screenshots/` | `screenshots/` at repo root |
| `docs/inconsistency_window_sla.md` | `documents/inconsistency_window_sla.md` |
| `schemas/transactions_v1.avsc` | `schemas/transactions_v2.avsc` |
| `infra/init.sql` / connector under `infra/` | `infra/postgres/init.sql`, `connectors/debezium-postgres-connector.json` |
| `scripts/replay_offsets.py`, `scripts/kill_spark.sh` | `scripts/chaos/replay_offsets.py`, `scripts/chaos/kill_spark.sh` |
| `docs/screenshots/12_ci_schema_fail.png` | `screenshots/12_ci_schema_fail copy.png` |
| `.github/workflows/ci.yml` in every clone | CI ran on GitHub (PR #4 green / PR #5 red). This tree listing does not include `.github/` — do not claim it is checked in here unless you add it. |
| `09_dlq_message.png`, `10_grafana_live.png` | Not in this tree. Do not link them until captured. |

</details>

---

## What Broke, and How I Found It

These are the failures that actually cost hours. They are interview gold because they are specific.

| Failure | Symptom | Root cause | Fix |
| :-- | :-- | :-- | :-- |
| Ledger poisoning | Stream "healthy", MERGE count stuck at 0 after a replay test | Replay called `process_batch` with `batch_id=999_999_999` and `commit=True` | `commit=False` on harnesses (Safeguard #1) |
| Deletes never landed | Parity `extra_in_pg` ≈ delete count | Flatten selected `e.after.*`; `after` is null on `op='d'` | `coalesce(after, before)` (Safeguard #2) |
| Deletes quarantined | Tombstone never reached MERGE; row resurrected later | Validator applied amount/currency/merchant to before-images | Delete exemption; UUID still required (Safeguard #3) |
| Parity false-FAIL on Day 5 | `extra_in_delta` ≈ tombstone count | Checker counted `is_delete=true` rows against live Postgres | `.filter("is_delete = false")` (Safeguard #4) |
| Frankenstein job | `TypeError: run_stream() got unexpected keyword 'starting_offsets'` | Day-6 edits concatenated a second `def run_stream` that won | One definition of each function; `grep -c "def run_stream"` |
| Schema-id explode | `AnalysisException` on the streaming DF | First schema-id dispatch called `.collect()` on the stream | Dispatch moved *inside* `foreachBatch` |
| Window stuck at ~0 | Grafana looked "perfect" and lied | Python pushed `now - commit` as a gauge | PromQL `time() - last_commit_ts` |
| Dummy Pushgateway series | First scrape showed `batch_id=1`, offsets=100 | Leftover grouping from an earlier probe | Let it age out; do not bounce the job |
| `:latest` Debezium | Connector would not load on CP 7.6.1 Connect | Image floated to 3.x / wrong Java | Pin `2.5.4.Final` |
| Stale submit args | `ClassNotFoundException` on a package you requested | `PYSPARK_SUBMIT_ARGS` overrode `spark.jars.packages` | `unset PYSPARK_SUBMIT_ARGS` before every Spark process |
| Mac registry dump was empty | `transactions_v2.avsc` was 0 bytes | Schema Registry lives on EC2, not the laptop | Fetch the subject on the box that runs Compose |

---

## Key Engineering Decisions

| Decision | Rationale | Trade-off accepted |
| :-- | :-- | :-- |
| **Soft-delete tombstones** | A physical delete removes the LSN the next late event would lose to. Keeping `is_delete=true` makes "later LSN wins" well-defined. | Delta live-row count ≠ raw Delta count. Parity must filter. |
| **LSN guard *and* batch ledger** | Checkpoint restart replays a micro-batch. The ledger makes that replay a no-op. The LSN predicate makes a late *event* a no-op. | Two pieces of state to reason about. |
| **Unrestricted `whenNotMatchedInsertAll()`** | A delete for a key the lake has never seen must still land, or a subsequent lower-LSN insert would recreate it. | Tombstones for never-seen keys occupy rows. That is the point. |
| **Sync validator inside `foreachBatch`** | Quarantine before `MERGE`. A bad currency must not become a ledger fact that parity then has to explain. | Validator bugs stall the micro-batch. |
| **Delete exemption on constraints** | Before-images are not subject to current check constraints. | A malformed UUID on a delete is still rejected — we cannot `MERGE` a null key. |
| **Schema-id decode per micro-batch** | Writer schema lives in the 5-byte Avro wire header. A new optional column does not require a driver bounce. | First implementation called `.collect()` and exploded. |
| **Salting only in aggregation** | MERGE is keyed on `transaction_id` and is already uniform. Salting the Kafka key or the MERGE would hide skew and break key affinity. | Aggregation job is a separate consumer (`scripts/aggregation_skew_job.py`). |
| **Window as PromQL, lag as sidecar** | A pushed `now - commit` gauge is ~0 at scrape time. Spark has no consumer-group lag API. | Two processes must stay up (driver + sidecar). Empty batches freeze the window. |
| **`push_metrics` swallows** | Observability must not abort a committed `MERGE`. | A silent Pushgateway outage looks like a dead job. |
| **Debezium 2.5.4.Final pin** | `:latest` moved under us mid-build and changed envelope defaults. | Must bump deliberately. |

---

## Testing & Validation

Unit tests hit production functions, not a rewritten window:

```bash
unset PYSPARK_SUBMIT_ARGS
source venv/bin/activate
PYTHONPATH=. pytest spark/tests/ -v
```

`spark/tests/test_dedup.py` must import `dedup_latest` from `spark.jobs.payments_cdc_job`. Tie-break is `lsn.desc, offset.desc` — not a replica window on `updated_at`.

Integration / chaos (live stack, one at a time):

```bash
PYTHONPATH=. python scripts/parity_checker.py
PYTHONPATH=. python scripts/snapshot_transition_checker.py
PYTHONPATH=. python scripts/inject_late_delete.py
PYTHONPATH=. python scripts/inject_bad_records.py
PYTHONPATH=. python scripts/chaos/replay_offsets.py --mode both
```

A full-history replay that *heals* residual rows and lands on the live Postgres count is not corruption. **Parity is the gate**, not the replay script's count assertion.

Schema contract against the live registry on this box:

```bash
curl -s http://localhost:8081/subjects/payments.public.transactions-value/versions
bash scripts/avro_contract_check.sh
```

CI on GitHub spins an ephemeral Schema Registry, registers the baseline, and POSTs a proposed schema to `/compatibility/subjects/.../versions/latest`. It is hermetic and fail-closed. Evidence already captured:

| PR | Change | Gate |
| :-- | :-- | :-- |
| **#4** `feature/schema-add-channel` | optional nullable `channel` | Schema Compatibility CI **green** (47s) — `screenshots/11_ci_pass.png` |
| **#5** `feature/schema-rename-currency` | `currency` → `ccy` | same job **red** (56s) — `screenshots/12_ci_schema_fail copy.png`. **Do not merge #5.** |

`main` must not carry a `transactions_proposed.avsc`. The red proof lives on PR #5, not on the default branch.

---

## Observability Contract

| Series | Producer | Meaning |
| :-- | :-- | :-- |
| `payments_cdc_last_ledger_commit_timestamp_seconds` | driver, after `commit_ledger` | PromQL window = `time() - this` |
| `payments_cdc_heartbeat_timestamp_seconds` | driver, same push | job liveness while batches flow |
| `payments_cdc_batch_{input,merged,dlq}_rows` | driver | last micro-batch shape (no `_total` suffix) |
| `payments_cdc_processed_offset_{partition}` | driver, from `lastProgress` | what Spark has written |
| `payments_cdc_topic_end_offset_{partition}` | `scripts/lag_sidecar.py` | what Kafka currently ends at |

Lag on a panel is **end − processed**. Do not read checkpoint files for this. Prometheus scrape of Pushgateway **must** set `honor_labels: true` or `job=payments_cdc` is overwritten.

---

## Screenshots

Files live in `screenshots/` at the repo root.

| File | Shows |
| :-- | :-- |
| `screenshots/01_stage0_parity_pass.png` | First `PARITY: PASS` on a clean baseline |
| `screenshots/02_chaos_recovery_spark_ui.png` | Spark UI after a hard driver kill and restart |
| `screenshots/03_chaos_parity_pass.png` | Parity clean after combined Connect + Spark chaos |
| `screenshots/04_offset_replay_unchanged.png` | Table unchanged after a committed-offset replay |
| `screenshots/05_schema_evolution_no_restart.png` | Stream alive through a mid-stream `ALTER TABLE` |
| `screenshots/05_schema_evolution_no_restart_2.png` | Second capture of the same proof |
| `screenshots/06_breaking_change_rejected.png` | Registry HTTP 409 on a field rename |
| `screenshots/07_skew_before.png.pdf` | Naive aggregation — 16.4× record imbalance |
| `screenshots/08_skew_after.png.pdf` | Salted aggregation — 3.0× on identical volume |
| `screenshots/11_ci_pass.png` | CI green on the compatible schema PR |
| `screenshots/12_ci_schema_fail copy.png` | CI red on the breaking rename PR |

Not in this tree (do not invent captions for missing files): `09_dlq_message.png`, `10_grafana_live.png`.

---

## Troubleshooting

| Issue | Likely cause | What to do |
| :-- | :-- | :-- |
| `TypeError: run_stream() got unexpected keyword 'starting_offsets'` | frankenstein job — two `def run_stream`, the stub won | One definition. `grep -c "def run_stream"` must print `1` |
| `PARITY` fails by roughly the delete count | checker is counting tombstones | must filter `is_delete = false` |
| Window stuck at ~0.0 | you pushed `now - commit` as a gauge | use PromQL against the commit timestamp |
| Window at thousands of seconds, job "up" | no Kafka records; empty batches skip `push_metrics` | run the generator for 10s; window should collapse |
| Spark UI "could not bind 4040" | job already owns 4040 | second process (parity) takes 4041 — ignore |
| `ProcessingTimeExecutor falling behind` | `local[2]` `MERGE` slower than the 10s trigger | expected under catch-up; not an LSN failure |
| Replay harness yells `REPLAY CORRUPTED THE TABLE` after a count drop | it cannot distinguish heal from corrupt | trust `scripts/parity_checker.py` |
| `ClassNotFoundException` on a package you did request | stale `PYSPARK_SUBMIT_ARGS` | `unset PYSPARK_SUBMIT_ARGS` and re-run |
| `pip install -r requirements.txt` file-not-found | this repo uses `req.txt` | `pip install -r req.txt` |

---

## Runbooks

The `runbooks/` directory exists in this tree. The four operational notes written during Day 6 are:

- Lag climbing on all partitions → `runbooks/lag_rising.md`
- Lag spread huge (one hot partition) → `runbooks/hot_partition.md`
- DLQ / input ≥ 0.1% → `runbooks/dlq_spike.md`
- Job will not start, checkpoint looks torn → `runbooks/checkpoint_corruption.md`

If those files are empty in a particular clone, copy them from the Day-6 packet before a demo. `--starting-offsets latest` after deleting a checkpoint drops the backlog. That is data loss. Only use it when the runbook says so.

Measured SLA write-up: [`documents/inconsistency_window_sla.md`](documents/inconsistency_window_sla.md).

---

## Teardown

Stop compute, keep the lake:

```bash
pkill -f spark.jobs.payments_cdc_job || true
pkill -f scripts/lag_sidecar.py || true
cd infra && docker compose stop && cd ..
```

Destroy the lake (you will rebuild parity from Postgres snapshot + CDC):

```bash
cd infra && docker compose down -v && cd ..
```

---

## Honest Limitations

1. **Empty Kafka micro-batches do not emit heartbeat or commit-timestamp gauges.** A quiet pipeline looks stale in Grafana until the next event.
2. The **CI schema gate** is a no-op on a branch that does not introduce a proposed schema file. The red proof is PR #5, not `main`.
3. On `local[2]` the **inconsistency window while catching up is the `MERGE` duration (observed 16–42s)**, not the 10s trigger.
4. **`scripts/chaos/replay_offsets.py`** still treats a count heal as an assertion failure. It cannot distinguish reconciliation from corruption. Parity is the gate.
5. **The DLQ is at-least-once while the ledger is exactly-once.** A crash between the DLQ write and the ledger commit replays the batch and re-writes the same rejections. Deliberate — losing a rejection is worse than recording it twice.
6. **GE (`great_expectations/payments_trend_monitor.py`) is a post-commit monitor**, not a gate in `process_batch`. Quarantine happens in the validator.

---

## Future Improvements

- [ ] Emit a heartbeat from the empty-Kafka wrapper path so a quiet job is distinguishable from a dead one.
- [ ] Add a CDF-based parity check that verifies every Postgres deletion has a matching Delta tombstone, closing the live-rows blind spot.
- [ ] Capture `screenshots/09_dlq_message.png` and `screenshots/10_grafana_live.png` if they are going into the walkthrough.
- [ ] Check the four Day-6 runbook files into `runbooks/` if this clone only has the empty directory.
- [ ] Replace host-network Pushgateway with a remote-write path and proper service discovery.
- [ ] Derive the CI proposed schema from the code rather than a checked-in file, so the gate fires on every PR without leaving a breaking file in the default branch.

---

## License

MIT. Synthetic payments data only — no real cardholder information is generated or stored.
