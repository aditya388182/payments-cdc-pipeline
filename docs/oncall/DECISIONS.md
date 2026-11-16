# Decisions

Each entry: what was decided, the evidence, and why. Newest at the bottom.

## D-001 MinIO replaced by SeaweedFS (config only)
- **Evidence:** `docker pull minio/minio:latest` and `minio/mc:latest` -> "pull access denied ... repository does not exist". The project's alternate registry `quay.io/minio/minio:latest` and `quay.io/minio/mc:latest` -> `401 UNAUTHORIZED` (not pullable anonymously). No same-version image exists, so the SPEC "Situation" rule applies.
- **Decision:** `chrislusf/seaweedfs:4.53` (pinned tag) runs `weed mini` with an S3 gateway. To change **only configuration**: the compose service is still named `minio`, still listens on port 9000, and still uses the access key / secret `minioadmin` / `minioadmin` (an `s3.json` identity file in `infra/seaweedfs/`). The bucket `payments-lake` is created on startup by `S3_BUCKET`, replacing the `minio-init` job (the `minio/mc` image is also gone). The Spark `s3a://` endpoint, path-style flag and credentials in the pipeline code did not change at all.
- **Verified:** Delta table init, streaming MERGE, ledger writes, checkpoints and the parity checker all run unchanged against it (see AUDIT.md rows 1-3).
- **Port binding:** published as `127.0.0.1:9000` (this is a new service definition; the SPEC safety rule says new ports bind to loopback). The other pre-existing services keep their original `ports:` mappings.
- The obsolete `version:` key was removed from the compose file (Compose v5 warns on it).
- The Debezium plugin is still installed by `confluent-hub` inside the Connect image at startup (2.5.4); that download works from this box, so no change was needed there.

## D-002 `.claude/settings.json`
`.claude/settings.json` was added by this branch's own commit (6087be0) and already contains exactly the deny rules from `docs/oncall/settings.deny.json`, so the requested merge is a no-op.

## D-003 The repo has no Makefile, no CI workflows, no runbooks, no `.env.example`
- **Evidence:** `git ls-tree -r main --name-only` has no `Makefile`, `.github/`, `runbooks/` or `.env*`. The CI file existed only on PR branches (`git log --all -- .github` -> commits `df853f6`..`d536404`, e.g. `git show df853f6:.github/workflows/ci.yml`), and the README itself says `.github/` is not in the tree.
- **Decision:** the SPEC's `make oncall-up`, `make oncall-e2e`, `make ws-load`, `make bullets` targets and the CI jobs are created new (a top-level `Makefile`, `.github/workflows/`). "Create `.env` from the repo's example" becomes: add `.env.example` (placeholders only) plus a generator script that writes a git-ignored `.env` with random local dev values. Nothing existing is overwritten.

## D-004 Tooling on this box
Python 3.10 venv at `venv/` created with `uv` (the README says the repo needs 3.10; the box has 3.12). Go 1.27.4 installed from go.dev with its published sha256 checked, into `~/.local/go-install` (no system-wide install, no `rm`). `buf` v1.75.0 and `k6` v2.4.0 installed to `~/.local/bin` from their GitHub releases with checksums verified. Node 24 and Java 21 were already present.

## D-005 `scripts/chaos/kill_spark.sh` could never find the job
`pgrep -f "payments_cdc_job.py"` does not match the documented launch command `python -m spark.jobs.payments_cdc_job`; the script printed `ERROR: no spark job ... is running` while the job was running (reproduced in Phase 0). Fixed the pattern to `python.*payments_cdc_job`. This is a test harness, not pipeline logic.

## D-006 `schemas/transactions_v2.avsc` is a 0-byte file
The README admits the registry dump was taken on a laptop and was empty; it never got replaced. The CI gate registers this file as the baseline, so as committed it could not work. Phase 5 replaces it with the real subject schema dumped from the live Schema Registry (data file, no logic change).

## D-007 Parity checker oracle
`scripts/parity_checker.py` fails with `ORACLE FAIL` unless the quarantined count is exactly 0 or 53 (the number of Postgres rows the Day-5 injection suite creates). Any other number of DLQ'd rows (for example the demo scenario in this project) makes it exit 1 even when the lake is correct. The existing script is left as is (baseline records its behaviour). The on-call console's drained parity check is a separate implementation that accounts for quarantined keys without the hard-coded 53.

## D-008 Replay publishes to the normal CDC topic, not a `cdc.replay` topic (SPEC fact was wrong)
- **Evidence:** `spark/jobs/payments_cdc_job.py` reads `.option("subscribe", TOPIC)` with `TOPIC = "payments.public.transactions"` only. The SPEC says replay re-publishes to "a replay topic the Spark job already treats like normal input"; no such topic exists and the job does not read one.
- **Decision:** replay-svc re-publishes the record as a Debezium-envelope Avro message on `payments.public.transactions`, built with the repo's own producer helpers (`scripts/inject_bad_records._get_avro_serializer/_envelope/_try_serialize`), carrying the record's ORIGINAL LSN. That is the exact input the job already decodes, validates and MERGEs, so no pipeline logic or subscription changed. A repeat is a no-op by the existing `staged.lsn > target.lsn` guard.

## D-009 replay-svc design choices
- The validator is a Spark DataFrame function (`spark/utils/validator.validate`), so replay-svc evaluates a one-row DataFrame in a small local Spark session instead of copying the rules into Python. The merchant set comes from the pipeline's own `MerchantRegistry`, force-refreshed per call. Cost: a JVM in the replay process (about 8 s to warm up; the gRPC health check reports NOT_SERVING until it is ready) and about a second per replay.
- Proto names follow `buf lint` STANDARD: enum values are `REPLAY_STATUS_*` with `REPLAY_STATUS_UNSPECIFIED = 0`, and `LakeStats` takes a `LakeStatsRequest` (not `google.protobuf.Empty`).
- Added `REPLAY_STATUS_PENDING` (published but the lake does not hold it yet) to the four statuses in the SPEC, because a replay can be accepted by Kafka and not yet visible; the HTTP layer will map it to 202.
- DLQ record id = its offset in the single-partition DLQ topic. "Open" DLQ records are derived, not stored: a record is applied when the lake holds its key at an LSN >= the record's LSN, so a replayed (or later-deleted) record leaves the queue without any extra state to keep consistent. In-page duplicates of the same key+LSN (the DLQ is at-least-once) are collapsed.

## D-010 Bug found by running it: stale merchant cache re-quarantines a replayed record
- **Evidence:** injected a transaction for `MERCH_DEMO_A` (unknown) -> DLQ offset 108. Replay -> `STILL_INVALID UNKNOWN_MERCHANT` (correct). Added the merchant to Postgres and replayed immediately: record published, but the DLQ topic grew from 109 to 110 messages and the lake never got the row (replay-svc waited 75 s, `PENDING`). Cause: the Spark job's `MerchantRegistry` refreshes at most every 120 s (`MERCHANT_REFRESH_SECONDS`), so the streaming validator still did not know the merchant. After the cache aged out, the same replay returned `APPLIED` and a second one `ALREADY_APPLIED`.
- **Not changed:** the validator and its cache are pipeline rules, so they stay. replay-svc instead detects a new DLQ message for the same key+LSN while it waits and answers `PENDING` immediately with the reason ("quarantined it again ... cache refreshes every 120 s; retry"). The demo and the replay test wait for the cache before replaying.
