# Payments CDC Pipeline + On-Call Console: A-to-Z Build Guide

**Project 4 of 5 to build** (P2 in the master plan): a data engineering project with a full software stack.

**Where it lives:** the existing `payments-cdc-pipeline` repo, on the branch `feat/oncall-console`. This file is `docs/oncall/SPEC.md`; the rules are `docs/oncall/RULES.md`. The repo already has the streaming pipeline (Postgres → Debezium → Kafka → Spark Structured Streaming → Delta Lake), an idempotent Delta MERGE with an LSN watermark and a batch ledger, a validator with a dead-letter queue (DLQ), failure tests, a skew fix, Prometheus/Grafana, CI and runbooks. This project adds the on-call product on top: a Go service, a Python gRPC replay service, a React Native (Expo) mobile app, alerts, an audit log, a schema compatibility gate, and load tests.

**Situation:** the repo was built on an earlier EC2 host that no longer exists. It is cloned onto a different box (Ubuntu 24.04, Docker). Bring the stack up with the repo's own commands. If a pinned container image can no longer be pulled (for example MinIO stopped publishing images in 2025, and Debezium moved its images to quay.io), first look for the **same version** on the project's official alternate registry; if none exists, switch to a maintained equivalent (for MinIO, an S3-compatible server such as SeaweedFS) by changing only configuration (endpoint, path-style access, credentials), never pipeline logic. Record every such change in DECISIONS.md.

---

## 1. What you are building

**One-line pitch:** an on-call engineer opens a phone app and sees the payments pipeline live: throughput, consumer lag, freshness and source-versus-lake parity, updated every 2 seconds. When something breaks, they get a push alert, inspect the bad records in the dead-letter queue, and replay them safely. Every action is audited, and replaying a record twice still writes one row.

**The distinct engineering problem:** safe operations on a live streaming pipeline. Real-time fan-out to many clients without letting a slow client hurt the others; admin actions that are authenticated, idempotent and audited; a replay that goes through the normal validated path, never straight into the lake; and a parity signal that doesn't cry wolf during normal streaming lag.

### What it proves
| Track | Proof |
|---|---|
| DE | CDC with exactly-once behavior already measured; DLQ replay through the validated path; lag-aware parity; Avro schema compatibility gate; observability |
| SWE | Go service (chi, pgx) with a WebSocket hub built on goroutines and channels; gRPC with Protocol Buffers between Go and Python services; OAuth 2.0 (GitHub) with PKCE and rotating refresh tokens; a React Native (Expo) app with push alerts; race-detector tests; k6 load test with 1,000 WebSocket clients |

**The interview story:** "The Go hub gives every client its own bounded buffer, so a slow phone gets dropped and reconnects instead of slowing everyone else. Replays go back through the same validator over gRPC, and the LSN guard makes the second replay a no-op, which a test proves. Parity only says 'mismatch' after a drained check; during streaming it says 'catching up'."

### Resume lines this build must make true
One-page (SWE specializing in DE and DS):
- "Built a Go service that streams live metrics over WebSockets with goroutines and channels and replays failed records over gRPC (k6-tested with 1,000 clients), plus a React Native on-call app with push alerts and OAuth 2.0 login."
- "*Data:* streaming CDC (Debezium, Kafka, Spark, Delta Lake) matched 100,812 of 100,812 rows after a driver kill; idempotent upserts keep a record replayed twice to one row; data skew cut 82%."

Two-page version also says: Go console service (chi, pgx), a WebSocket hub pushing throughput, consumer lag and parity every 2 seconds, a gRPC (Protocol Buffers) client that replays DLQ records through the Python validation service; push alerts when lag crosses a threshold, a DLQ viewer, admin-only replay behind GitHub OAuth 2.0, an audit log; a CI gate that rejects breaking Avro schema changes; 23 pytest tests; Go race-detector tests; a k6 WebSocket load test; skew cut 82% (16.4x to 3.0x) with salted aggregation.

**Stack line:** Go, chi, pgx, gRPC, Protocol Buffers, WebSockets, React Native (Expo), OAuth 2.0, Debezium, Kafka, Avro, Spark Structured Streaming, Delta Lake, Prometheus, Grafana, k6.

**Numbers:** "2-second push interval" and "1,000 WebSocket clients" are **scope** (test setup). Every measured number (delivery latency, drops, tests, replay results) comes from the results files. The existing numbers (100,812 of 100,812; 23 tests; 16.4x → 3.0x) are re-verified in the audit, never copied from the resume.

---

## 2. Scope

### In scope
- Phase 0 audit of every existing claim.
- Go `oncall-api` service: REST, WebSocket hub, auth, alerts, audit log, generator control, gRPC client, Prometheus metrics.
- Python `replay-svc`: a gRPC server wrapping the **existing** validator and producer, plus lake statistics.
- Observability-only progress events from the Spark job (after the MERGE and ledger commit).
- Expo mobile app: login, live, alerts, DLQ, replay, audit, settings.
- Avro schema compatibility gate in CI; `buf` lint and breaking checks for the protos.
- k6 WebSocket load test with 1,000 clients.
- Mock GitHub OAuth and a fake push sender, so everything runs without your accounts.

### Out of scope
Changing the MERGE, LSN guard or ledger logic; the 2-hour throughput soak (no throughput number is claimed); hosting Kafka or Spark 24/7; app-store publishing; roles beyond viewer and admin; Kubernetes.

---

## 3. Phase 0: the audit (first)

Write `docs/oncall/AUDIT.md`: one row per existing claim, with columns **exists / runs / tested / evidence / resume action**. Evidence is a command and the relevant lines of its real output.

| # | Claim | What proves it |
|---|---|---|
| 1 | The stack runs: Postgres, Debezium, Kafka (and Schema Registry if used), Spark job, Delta on S3-compatible storage | `docker compose ps` healthy, the connector RUNNING, rows arriving in Delta |
| 2 | "100,812 of 100,812 rows matched after a driver kill" | the repo's failure test and parity checker output (rerun it if feasible; otherwise the committed evidence file and how it was produced) |
| 3 | Idempotent Delta MERGE with an LSN watermark and a batch ledger | the code paths and the exactly-once replay check (for example `scripts/chaos/replay_offsets.py --mode both`) |
| 4 | Validator and DLQ | where DLQ records live (Delta table or Kafka topic), their shape, and how bad records are injected |
| 5 | "23 pytest tests" | `pytest` output with the count |
| 6 | CI schema check | what it checks today (shape only, or compatibility) |
| 7 | "data skew cut 82% (16.4x to 3.0x) with salted aggregation" | the skew script output and **the mechanism actually used** (salting or repartitioning) |
| 8 | Avro in the path | the serializer config (Avro with Schema Registry, or JSON) |
| 9 | Prometheus and Grafana | the dashboards and the metrics they read |
| 10 | Runbooks and incident write-up | the files |

**Rules for the audit:** anything this spec builds gets built; a false claim outside this scope is fixed by changing the resume wording (recorded in AUDIT.md's "Resume changes" table), not by building code to match. Then re-scope in DECISIONS.md if needed, and continue. Also in Phase 0: pin new versions (Go, chi, pgx, gorilla/websocket or coder/websocket, grpc-go, buf, Python grpcio, the Expo SDK and its React Native version, Jest, Playwright, k6) and check the current Expo docs for what push notifications need on a real device (Expo Go vs a development build).

---

## 4. Architecture

```
 Postgres ──Debezium──► Kafka ──► Spark Structured Streaming ──► Delta Lake (S3-compatible storage)
   ▲                      │        validator → DLQ / MERGE (LSN guard, batch ledger)
   │ load generator       │        └─(after commit, observability only)─► topic pipeline.progress
   │                      ▼
   │              topic cdc.replay ◄──────────────┐
   │                                              │ re-publish (normal validated path)
   │                                  replay-svc (Python, gRPC server)
   │                                    Replay · LakeStats · ListDlq      reuses the existing validator
   │                                              ▲
   │                                              │ gRPC (Protocol Buffers)
 oncall-api (Go: chi, pgx) ───────────────────────┘
   │ collectors (goroutines): Kafka lag (admin API) · connector status (Kafka Connect REST)
   │   · pipeline.progress consumer · source count (pgx) · LakeStats (gRPC)
   │ → snapshot every 2 s → hub → per-client bounded channels → WebSocket clients
   │ alert engine → push sender (Expo push API | fake) · audit log (Postgres) · /metrics (Prometheus)
   ▼
 Expo app (React Native)  ◄── WebSocket /v1/live, REST /v1/*, GitHub OAuth 2.0 (PKCE) login
```

### Key design decisions (be able to explain each)
1. **Hub with per-client bounded channels.** One producer goroutine builds a snapshot every 2 s and sends it to each client's buffered channel without blocking; a client whose buffer is full is disconnected (counted in metrics) and reconnects. One slow phone can never stall the broadcast.
2. **Sequence numbers and resume.** Every snapshot has a monotonically increasing `seq`; a reconnecting client sends its last `seq` and receives the latest snapshot plus any alerts it missed. Ping/pong keeps idle connections alive and detects dead ones.
3. **Replay through the validated path.** The Go service never writes to Kafka or Delta. It calls `replay-svc` over gRPC; `replay-svc` re-runs the **existing** validator and, if the record passes, re-publishes it to a replay topic the Spark job already treats like normal input. The LSN guard makes a second replay a no-op that reports "already applied".
4. **Lag-aware parity.** "In sync" when |source count − lake count| ≤ rate × p95 lag × 1.5; "catching up" above that; "mismatch" only from a parity-checker run after the stream is drained. Normal lag never shows a false alarm.
5. **Backend-mediated OAuth with PKCE.** The app opens `/v1/auth/github/start` with a PKCE challenge; the API redirects to GitHub, receives the callback, exchanges the code with its client secret, checks an allowlist of GitHub usernames (with an admin subset), and redirects to the app's deep link with a one-time code bound to that challenge. The app exchanges the code plus its verifier for a 15-minute access token and a rotating refresh token (stored hashed; reusing an old refresh token revokes the whole family). The client secret never reaches the phone.
6. **Idempotency keys on admin actions.** Replay and generator requests carry an `Idempotency-Key`; a repeated request returns the first result. Every admin action writes an audit row (who, action, target, request time, result, error).
7. **Fakes for everything external.** A mock GitHub OAuth server and a fake push sender let every test and CI job run without accounts or a phone.

---

## 5. Python replay service (`oncall/replay/`, gRPC)

`oncall/proto/oncall/v1/replay.proto` (checked by `buf lint` and `buf breaking` against `main`):
- `Replay(ReplayRequest{dlq_id, requested_by, idempotency_key}) → ReplayResponse{status: APPLIED | ALREADY_APPLIED | STILL_INVALID | NOT_FOUND, detail}`
- `ListDlq(ListDlqRequest{cursor, limit, reason}) → ListDlqResponse{records, next_cursor}` and `GetDlq(id)`
- `LakeStats(Empty) → {row_count, max_lsn, last_commit_ts}` (read with the `deltalake` package or the repo's existing reader)

Rules: imports the repo's existing validator and producer code (no copy); `STILL_INVALID` returns the validator's error; deadlines on every call; health service; structured logs; pytest with an in-process gRPC server.

**DLQ demo scenario** (so a replay can succeed for a real reason): records referencing an unknown currency (or the repo's equivalent rule) land in the DLQ; adding the currency to the reference table and replaying succeeds. Uses the repo's existing bad-record injection if it fits.

---

## 6. Go service (`oncall/api/`)

**Libraries:** chi (routing, middleware), pgx (Postgres pool), a maintained WebSocket library, grpc-go, franz-go `kadm` (Kafka lag) or the equivalent, `prometheus/client_golang`, `log/slog` (JSON logs), golang-jwt.

### API
| Method and path | Who | Purpose |
|---|---|---|
| `GET /v1/auth/github/start?code_challenge=&redirect_uri=` | anyone | starts OAuth (state stored server-side, CSRF-safe) |
| `GET /v1/auth/github/callback` | GitHub | exchanges the code, checks the allowlist, redirects to the app with a one-time code |
| `POST /v1/auth/token` `{code, code_verifier}` / `{refresh_token}` | anyone | issues or rotates tokens |
| `POST /v1/auth/logout` | user | revokes the refresh family |
| `GET /v1/live` (WebSocket) | viewer | snapshots every 2 s, alerts, `seq` resume |
| `GET /v1/snapshot` | viewer | the latest snapshot (for the first paint) |
| `GET /v1/alerts`, `POST /v1/alerts/{id}/ack` | viewer | alert list, acknowledge (audited) |
| `GET /v1/dlq?cursor=&reason=`, `GET /v1/dlq/{id}` | viewer | via gRPC `ListDlq`/`GetDlq` |
| `POST /v1/dlq/{id}/replay` | admin | via gRPC `Replay`, `Idempotency-Key` required, audited |
| `POST /v1/generator` `{action: start|stop, rate}` | admin | rate ≤ `MAX_RATE` else 422, audited |
| `GET /v1/audit?cursor=` | admin | audit log |
| `POST /v1/devices` `{expo_push_token}` | viewer | register a phone for push |
| `GET /healthz`, `GET /readyz`, `GET /metrics` | ops | `/metrics` not exposed publicly |

Errors are JSON `{"error": {"code", "message"}}`; 401 without a token, 403 for a viewer on admin routes.

### Snapshot (every 2 s)
events per second (in and out), consumer lag per partition and total, end-to-end freshness (now − max committed source time) with p50/p95 over 60 s, connector status, source row count, lake row count, parity badge (decision 4), DLQ count and growth, `seq`, server timestamp.

### Alert engine
Rules from config: total lag above a threshold for 30 s; connector not RUNNING; freshness above a threshold; DLQ growth; a mismatch from the drained parity check. Each alert has a state (firing, acknowledged, resolved), is deduplicated, pushes once per state change to registered devices (Expo push API sender or the fake), appears on the WebSocket, and is stored in Postgres.

### Tests (`go test -race ./...` must pass)
- Hub: fan-out to many clients; a slow client is dropped while others keep receiving on time; `seq` resume; ping/pong timeout; no goroutine leaks (goleak).
- Auth: PKCE mismatch rejected; state mismatch rejected; non-allowlisted user refused; viewer vs admin; refresh rotation; **reuse of an old refresh token revokes the family**; expired access token → 401.
- Admin: replay without `Idempotency-Key` → 400; a repeated key returns the first result; generator above `MAX_RATE` → 422; every admin action writes an audit row.
- gRPC client: deadlines, retries only on UNAVAILABLE, mapping of each status to HTTP (with an in-memory fake server, bufconn).
- Parity badge: in-sync, catching-up and mismatch cases.
- Alert engine: thresholds with durations, dedup, ack, resolve, one push per state change.
- Integration (against the compose Postgres): migrations, audit queries, device registration.

---

## 7. Spark progress events (observability only)
In `foreachBatch`, **after** the MERGE and the ledger commit succeed, publish one JSON message per batch to `pipeline.progress`: batch id, input rows, rows merged, rows to the DLQ, batch duration, max committed source time, commit time. Never read by the pipeline; failures to publish are logged and never fail the batch. Rerun the exactly-once replay check and the existing tests after this change to prove nothing else moved.

---

## 8. Mobile app (`oncall/mobile/`, Expo, React Native, TypeScript strict)

### Screens
1. **Login:** "Sign in with GitHub" (expo-auth-session / expo-web-browser opening the API's start URL; deep link back); tokens in expo-secure-store.
2. **Live:** throughput and lag charts updating every 2 s, freshness, connector status, parity badge; reconnect indicator.
3. **Alerts:** firing, acknowledged and resolved lists; acknowledge.
4. **DLQ:** list with reason filter and paging; record detail with the payload and validation error; **Replay** (admin only) behind a confirmation sheet; shows APPLIED, ALREADY_APPLIED or STILL_INVALID.
5. **Audit** (admin).
6. **Settings:** push on/off (registers the device), signed-in user and role, sign out.

### Quality and tests
- No `any`; ESLint clean; accessible labels on every control; loading, empty and error states.
- Jest + React Native Testing Library: the WebSocket client (reconnect with `seq`), the token refresh flow, the replay confirmation and result states, role-based hiding of admin actions.
- **Playwright on the Expo web build** (react-native-web) against the compose stack with the mock OAuth server: sign in → open a DLQ record → replay → it leaves the queue and an audit row appears; a viewer sees no Replay button and gets 403 from the API directly. Screenshots of every screen into `docs/oncall/screenshots/`. Guard native-only modules (notifications, secure store) so the web build runs.
- Real-device checks (push delivery, the GitHub login on a phone) are yours; Claude writes the exact steps in BLOCKERS.md, including whether Expo Go is enough or a development build is needed.

---

## 9. Schema gate and CI

- **Avro compatibility gate** (`scripts/check_schema_compat.py` or a Make target): compares each proposed Avro schema with the registered baseline under BACKWARD compatibility (the Schema Registry compatibility endpoint in CI, or an equivalent local check). Fixtures: a breaking change (a removed field, or a type change without a default) **fails with a clear message**; a compatible change (a new optional field with a default) passes. If the audit shows the pipeline doesn't use Avro, gate the actual serialization format and fix the resume wording instead.
- **Proto gate:** `buf lint` and `buf breaking --against` the main branch.
- **CI jobs must fit a standard GitHub-hosted runner.** Add: go (vet, staticcheck or golangci-lint, `go test -race`), replay (pytest), proto (buf), schema gate, mobile (tsc, lint, Jest), and the existing pipeline tests. The full-stack end-to-end run (Kafka, Spark, Playwright, k6) stays local (`make oncall-e2e`, `make ws-load`) with results committed; if a light end-to-end job fits a hosted runner, add it, otherwise record why not in DECISIONS.md.

---

## 10. Load test (k6)
`oncall/loadtest/ws.js`: ramp to **1,000 concurrent WebSocket clients**, hold 5 minutes, each validating `seq` order. Measure: connections established and failed, snapshots received per client vs expected, delivery latency (client receive time − server timestamp; client and server on the same box, so clocks match) p50/p95/p99, disconnects, and server CPU and memory (`docker stats` or `/metrics`). Then run one slow-client variant (a share of clients reading slowly) to show others stay on time. Results: `ws_load.json`, labelled local, with the instance type.

---

## 11. Results and evidence
Use the repo's existing results or evidence folder (otherwise `results/oncall/`). Each file has `commit`, `dirty`, `generated_at_utc` and `command`:
- `audit_summary.json`
- `baseline.json` (existing tests, parity checker, exactly-once replay check, before any change)
- `replay_test.json`: the same DLQ record replayed twice → statuses (APPLIED, ALREADY_APPLIED), exactly one lake row for the key, two audit rows
- `ws_load.json` (both variants)
- `schema_gate.json` (breaking fixture failed, compatible fixture passed, with messages)
- `tests.json` (counts per suite: existing pytest, new pytest, Go with `-race`, Jest, Playwright)

`make bullets` prints the resume bullets with numbers from these files (MISSING for absent keys). **README section:** a 2-minute demo GIF or video (live charts → inject a bad record → it lands in the DLQ → alert → fix the reference data → replay → parity back in sync), the architecture diagram, the numbers table generated from results, how to run (`make oncall-up`), testing approach, limits (local load test; push and real GitHub login verified only on a phone).

---

## 12. Phases (commit at the end of each, using the repo's commit style)
- **Phase 0:** audit, image fixes, versions, Expo push requirements. **Check:** AUDIT.md (10 rows with evidence), `baseline.json`.
- **Phase 1:** protos, `replay-svc`, Spark progress events. **Check:** pytest output; the exactly-once replay check still passes; a progress message on the topic.
- **Phase 2:** Go service: collectors, hub, REST, gRPC client, alerts, audit, generator, metrics. **Check:** `go test -race ./...` output; a `websocat` (or similar) capture of snapshots every 2 s; `replay_test.json` from a real replay-twice run through the API.
- **Phase 3:** auth (mock GitHub OAuth, PKCE, rotation). **Check:** the auth tests; a curl walkthrough of the full flow against the mock.
- **Phase 4:** mobile app, Jest, Playwright on the web build. **Check:** screenshots of every screen; Jest and Playwright output.
- **Phase 5:** schema gate, buf, CI jobs, k6. **Check:** the failing and passing schema fixtures; `ws_load.json`; workflows validated locally (actionlint).
- **Phase 6:** real devices and accounts (yours; BLOCKERS.md): a GitHub OAuth app, the app on your phone, a real push.
- **Phase 7:** README section, demo GIF, `make bullets`, the audit's resume changes; a subagent reviews against "Done means" and fixes what it finds; `docs/oncall/DONE.md`.

---

## 13. Done means
- [ ] AUDIT.md: 10 rows with evidence and a resume action for each false claim; `baseline.json`.
- [ ] `replay-svc` over gRPC reusing the existing validator; replay goes through the validated path; replay-twice proven (`replay_test.json`).
- [ ] Spark progress events added after commit only; existing tests and the exactly-once check still pass.
- [ ] Go service: hub with bounded per-client buffers and `seq` resume, collectors, alerts, audit, generator cap, gRPC client, metrics; `go test -race` passes with no goroutine leaks.
- [ ] OAuth 2.0 with PKCE, allowlist and admin roles, rotating refresh tokens with reuse detection; all auth tests pass against the mock.
- [ ] Expo app: six screens, role-based actions, secure token storage; Jest and Playwright (web build) pass; screenshots saved.
- [ ] Avro (or actual format) compatibility gate with failing and passing fixtures; `buf` lint and breaking checks; CI jobs that fit hosted runners.
- [ ] k6: 1,000-client run and slow-client run; `ws_load.json`.
- [ ] Results files present; README section complete; `make bullets` prints with MISSING only for phone-only checks.

---

## 14. Interview prep
1. Walk through the hub: what happens when one client is slow? Why bounded channels and not unbounded queues?
2. How does a reconnecting client catch up, and what does `seq` guarantee?
3. Why does replay go through gRPC to a Python service instead of the Go service writing to Kafka?
4. What makes replaying a record twice safe? Show the test.
5. Why can't the parity badge just compare counts? What does "catching up" mean mathematically?
6. Walk through the OAuth flow. Why PKCE if the server holds the secret? What does refresh-token reuse detection catch?
7. Why idempotency keys on admin actions, and where are they stored?
8. What does the race detector find that normal tests don't? Did it find anything?
9. What did the k6 run measure, and what limits it (same box, local)?
10. What breaks BACKWARD compatibility in Avro, and why does the gate run in CI?
11. What did the audit find about the existing claims, and what did you change?
