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
