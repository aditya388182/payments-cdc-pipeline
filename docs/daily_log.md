# Memory Carry Forward (MCF) Doc – Day 1

**Project:** Project 1: Real-Time Payments Ledger Ingestion with Exactly-Once CDC  
**Date:** July 18, 2026  
**Session Type:** Day 1 Infrastructure Setup  
**Prepared for:** Next AI session / new chat context transfer

---

## 1. Executive Summary

**Day Number:** Day 1 (Infrastructure, Database, Kafka, Debezium setup)

**Primary Objectives for the Day:**
- Establish the complete Docker-based infrastructure (Postgres 15 + Debezium + Kafka KRaft + Schema Registry + Connect + MinIO).
- Create all foundational configuration files so that a row inserted/updated/deleted in Postgres appears as an Avro CDC event in Kafka.
- Prepare the environment for Spark Structured Streaming work on subsequent days.

**Objectives Completed:**
- Repository structure created.
- All 5 core Day 1 configuration files delivered in complete, production-ready form:
  - `.gitignore`
  - `infra/docker-compose.yml` (full 6-service stack)
  - `infra/postgres/init.sql` (tables + heartbeat + seed data)
  - `infra/kafka/topics.sh`
  - `connectors/debezium-postgres-connector.json`
- Initial `docs/daily_log.md` started.
- Scaffolding bash commands provided.

**Outstanding Tasks (as of end of Day 1 session):**
- Actual execution of `docker compose up -d` and health verification.
- Verification of `wal_level = logical`.
- Registration and startup of Debezium connector.
- Confirmation of initial snapshot (`op='r'`) events in Kafka.
- Manual INSERT/UPDATE/DELETE testing to observe `op='c'`, `op='u'`, `op='d'` + tombstone.
- Heartbeat table timestamp advancement check.
- Final commit and `daily_log.md` update with verification results.

**Current Blockers:** None. All configuration files are ready. The only remaining work is running and validating the stack.

**Overall Project Status:** 
Infrastructure configuration phase complete. Ready to move into runtime verification and then Day 2 (Spark job core). The critical correctness safeguards (ledger commit flag, delete flattening, validator exemption, tombstone-aware parity) are noted for implementation in Day 2+ Spark code and do not apply to Day 1 infra files.

---

## 2. System Architecture & Infrastructure Snapshot

**Current Planned Topology (as designed for Day 1):**

- **Host Machine (Ubuntu EC2 t3.xlarge):** Runs VS Code Remote-SSH, Python virtual environment, and will later run the PySpark Structured Streaming job (`local[2]` or spark-submit).
- **Docker Network (default bridge or custom):** All services communicate internally via service names.
  - `postgres:5432` – Primary database with `wal_level=logical`.
  - `kafka:9092` (internal) / `localhost:29092` (host) – Kafka in KRaft mode (no ZooKeeper).
  - `schema-registry:8081` – Confluent Schema Registry for Avro.
  - `connect:8083` – Kafka Connect with Debezium PostgreSQL connector.
  - `minio:9000/9001` – MinIO object storage (later used for Delta Lake `s3a://payments-lake`).

**Port Mappings (Host → Container):**
- Postgres: 5432 → 5432
- Kafka (PLAINTEXT_HOST): 29092 → 29092
- Schema Registry: 8081 → 8081
- Kafka Connect: 8083 → 8083
- MinIO API: 9000 → 9000
- MinIO Console: 9001 → 9001

**Key Design Decisions Visible in Day 1 Files:**
- Kafka runs in **KRaft mode** (single node, `KAFKA_PROCESS_ROLES: broker,controller`).
- Debezium uses **pgoutput** plugin + logical replication slot.
- Heartbeat table + `heartbeat.action.query` is configured to prevent WAL bloat.
- Two DLQ topics are created: `payments.transactions.dlq` (business rules) and `connect-dlq` (serialization/infra errors).
- MinIO bucket `payments-lake` is auto-created by the `minio-init` sidecar.

**Current State (End of Day 1 Configuration Phase):**
All configuration files exist in the working directory structure. Docker containers have **not yet been started** in this session. The next session should begin by running the Docker stack and performing verification.

---

## 3. Data Flow Documentation (Planned)

**End-to-End Flow (as designed):**

1. **Source:** Application or manual SQL issues `INSERT`, `UPDATE`, or `DELETE` against the `transactions` table in Postgres.
2. **WAL Capture:** Postgres writes the change to the Write-Ahead Log (WAL) because `wal_level=logical`.
3. **Debezium Capture:** The Debezium PostgreSQL connector (running in Kafka Connect) reads the logical replication slot (`debezium_payments_slot`), converts the change into an Avro message using the Confluent AvroConverter, and publishes it to the topic `payments.public.transactions`.
4. **Schema Validation:** Schema Registry validates the Avro schema (subject `payments.public.transactions-value`).
5. **Kafka Transport:** The event (with `op`, `before`, `after`, `source` structs) is stored in Kafka.
6. **Future Spark Consumption (Day 2+):** PySpark `readStream` will consume from Kafka, strip the Confluent wire-format header, deserialize Avro, flatten the CDC envelope (with special `coalesce` handling for deletes), apply validation, deduplicate by LSN, and perform a guarded MERGE into Delta Lake on MinIO.
7. **Sink:** Delta Lake table `s3a://payments-lake/transactions` (with Change Data Feed enabled) stores the current state. Deleted rows are retained as `is_delete=true` tombstones to protect LSN ordering.

**Important Note for Future Sessions:** The critical transformation logic (especially delete flattening with `coalesce` and the batch ledger with `commit=True/False` flag) will be implemented in the Spark layer on Day 2 and Day 3. Day 1 only establishes the reliable capture path from Postgres → Kafka.

---

## 4. Granular Change Ledger

| File Path | Type of Change | Previous Behavior | New Behavior | Exact Logic Introduced | Reason for Change |
|-----------|----------------|-------------------|--------------|------------------------|-------------------|
| `.gitignore` | Created | N/A | Standard Python + Spark + Docker ignores | Ignores `__pycache__`, `.venv`, `checkpoints/`, `spark-warehouse/`, `*.jar`, `.env` | Prevent accidental commit of generated/temporary files |
| `infra/docker-compose.yml` | Created (assembled) | Scattered incomplete YAML snippets in plan | Single complete, runnable compose file with 6 services | Full KRaft Kafka config, advertised listeners for host access (`localhost:29092`), Confluent Hub install command for Debezium, MinIO + init sidecar | The original plan had fragmented YAML; this is the single source of truth for the entire stack |
| `infra/postgres/init.sql` | Created | N/A | Full DDL + seed data | `transactions`, `merchants`, `debezium_heartbeat` tables + 20 merchants + 1000 transactions | Required for Debezium to have a publication and for initial snapshot testing |
| `infra/kafka/topics.sh` | Created | N/A | Idempotent topic creation script | Creates 3 topics with zstd compression, 7-day retention, and correct partition counts | Ensures consistent topic configuration every time the stack is rebuilt |
| `connectors/debezium-postgres-connector.json` | Created | N/A | Complete Debezium config | `pgoutput` plugin, logical slot, heartbeat query, precise decimals, tombstone on delete, DLQ routing to `connect-dlq` | Core configuration that tells Debezium what to capture and how to handle errors |
| `docs/daily_log.md` | Created (initial) | N/A | Structured daily log template | Sections for objectives, issues, decisions, next steps | Enforces the "commit daily + rich war stories" discipline from the original plan |

---

## 5. Architectural Decisions & Rationale

**Decision 1: Use Confluent Platform images instead of plain Debezium image**
- **Why chosen:** The plain `debezium/connect` image does not include the Confluent Avro converters. Using `cp-kafka-connect` + `confluent-hub install` guarantees Avro support out of the box.
- **Trade-off accepted:** Slightly larger image and first-start delay (plugin download), but dramatically simpler Avro serialization.

**Decision 2: KRaft mode (no ZooKeeper)**
- **Why chosen:** Modern Kafka (7.6.1) supports KRaft. Removes one moving part and reduces memory footprint on the t3.xlarge instance.
- **Rejected alternative:** ZooKeeper mode – more components, more ports, more failure modes.

**Decision 3: Heartbeat table + action query**
- **Why chosen:** Debezium documentation and the original plan strongly recommend this pattern to prevent WAL bloat when there are long periods of no real transactions. The heartbeat table is updated every 10 seconds.
- **Trade-off:** One extra table and a small amount of noise in the CDC stream, but prevents replication slot lag and WAL growth to terabytes.

**Decision 4: Dual DLQ topics (`payments.transactions.dlq` vs `connect-dlq`)**
- **Why chosen:** Clear separation of concerns. `connect-dlq` captures serialization/infrastructure errors from Kafka Connect. `payments.transactions.dlq` will later capture business-rule rejections from the Spark validator (Day 5).
- **Benefit for future interviews:** Demonstrates understanding of infrastructure vs application DLQ patterns.

---

## 6. Safeguards, Bugs, and Mitigations

**Day 1 Specific Notes:**
- No Spark code was written on Day 1, therefore the four **Critical Correctness Safeguards** (Ledger Poisoning prevention via `commit=True/False` flag, Delete Payload Flattening with `coalesce`, Validator Exemption for Deletes, Tombstone-Aware Parity) are **not yet applicable**.
- These safeguards are explicitly documented here so that Day 2+ Spark development strictly implements them.
- No bugs were encountered during file creation because Day 1 work was purely configuration assembly based on the Gap Audit and original plan.

**Known Future Risk (Documented for Day 2+):**
- If the `process_batch` function is implemented without the `commit=True` parameter, the replay test harness on Day 3 will poison the ledger. This MCF document explicitly calls out that requirement.

---

## 7. Environment & Git State

**Expected Environment (as per project spec):**
- **OS:** Ubuntu (EC2 t3.xlarge)
- **Python:** 3.10 or 3.11 (pyspark 3.5.5 wheels available; 3.12 possible with pyspark==3.5.5)
- **Docker:** 24.x+ with Compose v2
- **Java:** JDK 11 or 17 (required by PySpark)
- **Virtual Environment:** `.venv` in project root
- **Git Branch:** `main` (or feature branch created on Day 1)
- **Uncommitted Changes (at time of MCF creation):** All Day 1 files are new and should be committed together after verification.

**Recommended Git Commit Message (for end of Day 1):**
day1: infrastructure foundation - docker-compose, postgres init, kafka topics, debezium connector, scaffolding scripts
text---

## 8. Verification Checklist (Commands for Next Session)

The next AI session **must** run these commands in order before declaring Day 1 complete:

```bash
# 1. Start the stack
cd infra
docker compose up -d

# 2. Verify all containers healthy
docker compose ps
docker exec postgres psql -U payments -c "SELECT 1;"

# 3. Verify WAL level
docker exec postgres psql -U payments -c "SHOW wal_level;"

# 4. Run topic creation script
cd ..
chmod +x infra/kafka/topics.sh
./infra/kafka/topics.sh

# 5. Register Debezium connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/debezium-postgres-connector.json

# 6. Check connector status (must be RUNNING)
curl -s http://localhost:8083/connectors/payments-postgres-cdc/status | python3 -m json.tool

# 7. Check Schema Registry subjects (should see payments.public.transactions-value)
curl -s http://localhost:8081/subjects

# 8. Verify initial snapshot in Kafka (sample 3 messages)
docker exec -it schema-registry kafka-avro-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic payments.public.transactions \
  --from-beginning --max-messages 3 \
  --property schema.registry.url=http://schema-registry:8081

# 9. Test manual DML and observe op codes
docker exec postgres psql -U payments -c "
INSERT INTO transactions (transaction_id, merchant_id, amount_minor, currency, status, event_type)
VALUES (gen_random_uuid(), 'MERCH_001', 12345, 'USD', 'PENDING', 'AUTHORIZATION');"

# Then check Kafka for op='c'
# Repeat for UPDATE and DELETE to see op='u' and op='d' + tombstone

9. Technical Debt
Current (Day 1):

Docker Compose file is monolithic. On Day 6 we may want to split monitoring services into a separate compose profile.
No healthcheck or restart policy on the connect service yet (first startup can be slow due to plugin download).
MinIO console is exposed on 9001 with default credentials (minioadmin/minioadmin). This should be changed or firewalled before any real data.

Future Improvements (noted for later days):

Add Prometheus + Grafana + Pushgateway on Day 6 as planned.
Implement proper secrets management (.env file + Docker secrets) instead of plaintext passwords in compose.
Add automated schema evolution testing (already planned for Day 4).


10. Next-Chat Onboarding Instructions
For the next AI session / new chat:

Read this MCF document first (Memory_Carry_Forward_Doc_Day_1.md). It is the single source of truth for context.
Read the actual Day 1 files in this order:
infra/docker-compose.yml
infra/postgres/init.sql
infra/kafka/topics.sh
connectors/debezium-postgres-connector.json

Run the Verification Checklist (Section 8 above) in exact order.
Update docs/daily_log.md with the results of verification (what worked, what failed, how it was fixed).
Only after verification passes, begin Day 2 work (Spark session setup, Delta table initialization, Avro deserializer with delete flattening, and the core payments_cdc_job.py skeleton with the commit=True ledger flag).

Critical Warning for Next Session:
Do not begin writing Spark code (especially process_batch or the ledger logic) until the infrastructure verification in Section 8 has succeeded. Starting Spark work on a broken capture path will waste significant time.
Files that must exist before Day 2 starts:

All Day 1 files listed above
Updated docs/daily_log.md with verification results
Git commit for Day 1 work

Day 1 Runtime Bug — Encountered & Resolved
Status: RESOLVED during Day 1 verification, before Day 2 began.
The Problem
During Step 5 of the Verification Checklist (registering the Debezium connector), the connector failed to register — Kafka Connect's /connectors REST API behaved as if the Debezium PostgreSQL connector class did not exist, even though the plugin install step had run without an explicit error.
Root Cause
infra/docker-compose.yml, in the connect service's command block, had:
yamlconfluent-hub install --no-prompt debezium/debezium-connector-postgresql:latest
:latest resolved to Debezium 3.x, which requires a Java 17 runtime (confirmed via Debezium's own 3.0 release notes: "Debezium connectors now require Java 17 for runtime and Java 21 for building"). The confluentinc/cp-kafka-connect:7.6.1 image is from the Confluent Platform 7.x generation, which runs on Java 11 — it predates Confluent's later Temurin 21/25 upgrade path (that only started at CP 8.0+).
The failure mode was silent — no bytecode error, no obvious log line. This is a documented, known behavior: Red Hat's own Debezium 3.0 release notes state "If you use Java 11 with new connectors, Kafka Connect silently fails to find the connector. The connector does not report any bytecode errors." Kafka Connect's plugin scanner simply discarded the incompatible jar during JVM startup and finished booting without it, which is why the REST API behaved as though the class was missing rather than throwing a clear version-mismatch error.
Diagnosis Process

Verified the actual current content of docker-compose.yml via grep rather than assuming — confirmed the file said :latest.
Cross-checked the failure signature against Debezium/Red Hat's official documentation to confirm this was a known Java-version incompatibility, not a different underlying issue (network failure, plugin path misconfiguration, etc.).
Ruled out alternative causes before applying a fix, to avoid a second cycle of blind trial-and-error.

The Fix
Pinned the connector install to a Java-11-compatible release instead of :latest:
yamlconfluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.5.4
Applied and verified with:
bashdocker compose up -d --force-recreate connect
until curl -sf http://localhost:8083/connectors >/dev/null 2>&1; do sleep 5; done
curl -s http://localhost:8083/connector-plugins | python3 -m json.tool | grep -A2 "Postgres"
Confirmed the correct version loaded:
json"class": "io.debezium.connector.postgresql.PostgresConnector",
"type": "source",
"version": "2.5.4.Final"
Then registered and confirmed RUNNING:
bashcurl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/debezium-postgres-connector.json
Result: both connector.state and tasks[0].state returned "RUNNING".
Fix Committed
Landed on branch feature/day1-infra-capture (1-line change to infra/docker-compose.yml; infra/kafka/topics.sh also picked up an executable-permission fix in the same pull).
Recommended commit message (if not already used):
fix: pin debezium connector to 2.5.4 — :latest resolved to debezium 3.x,
which requires java 17 and silently fails to load on the java-11-based
cp-kafka-connect:7.6.1 image
Lesson for Future Days
Never leave a :latest tag in a provisioning script that gets re-executed on every container restart (the command: block in Compose runs fresh each time) — a floating tag means "works today" can silently become "broken tomorrow" with zero diff in your own repo. Every image/plugin version in this project should be pinned to an explicit, tested version going forward.

End of Memory Carry Forward (MCF) Doc – Day 1
This document was generated at the conclusion of Day 1 configuration work. It is designed to allow a completely new AI session to continue seamlessly with minimal context loss.
