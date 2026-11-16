# Rules for the On-Call Console work (read with docs/oncall/SPEC.md)

These add to the repo's own rules (README, Makefile gates, CI, any CLAUDE.md or CONTRIBUTING). Where they conflict, the repo's conventions win, except for the honesty and safety rules below, which always apply.

## Honesty of results (most important)
- Every number in the README, the results files, the app, AUDIT.md or DONE.md comes from code or command output. Never type, round up or estimate a result.
- Existing resume numbers (100,812 of 100,812; 23 pytest tests; 16.4x to 3.0x) are verified from the repo's evidence or a rerun in the audit, never copied from the resume.
- A false resume claim outside this spec's scope is fixed by changing the resume wording (AUDIT.md "Resume changes"), not by building code to match it.
- "1,000 WebSocket clients" and "every 2 seconds" are test setup. Report what the load test measured, including drops and failures.
- Push delivery and the real GitHub login are verified only on a phone. Until then, say "tested with a fake push sender and a mock OAuth server".

## Do not change the pipeline's core
- Never change the MERGE, the LSN guard, the batch ledger or the validator's rules. The only pipeline change allowed is the observability-only progress event after the commit (SPEC section 7) and configuration needed to replace an image that can no longer be pulled.
- After any pipeline-side change, rerun the existing tests and the exactly-once replay check and show their output.

## Stop and record instead of guessing
- A spec fact turns out wrong (an existing module, a library behavior, a version incompatibility, an image that's gone): record the evidence in docs/oncall/DECISIONS.md, update the spec, continue.
- A step needs my accounts, money, a phone or secrets (GitHub OAuth app, Expo account, a real device): list it in docs/oncall/BLOCKERS.md with exact clicks and values, skip it, keep going.

## Safety rules
- Work only on the branch `feat/oncall-console`. Never push, never use gh, never create cloud resources.
- Never print or commit `.env`; local dev values in it are generated. Real credentials (a GitHub OAuth client secret, an Expo access token) live outside the repo in `~/.config/oncall/`: never read, print or copy those files; check whether one exists with `test -f` only. All tests use the mock OAuth server and the fake push sender.
- Deleting files: never chain `rm` with other commands. Use `make clean` targets, `git clean` for build output, or one `rm` of explicit paths inside this repo per command.
- Bind every new service port to 127.0.0.1 in compose.

## Code rules
- **Go:** `gofmt`, `go vet` and a linter clean; `go test -race ./...` passes; contexts with timeouts on every outbound call; no goroutine leaks (goleak in tests); structured `slog` JSON logs without tokens or secrets.
- **Python:** the repo's style and test tools; type hints on public functions; reuse the existing validator and producer, never copy them.
- **Protos:** `buf lint` and `buf breaking` clean.
- **Mobile:** TypeScript strict, no `any`, lint clean, accessible labels, tokens only in secure storage, native-only modules guarded for the web build.
- **Security:** every new endpoint has 401 and 403 tests; admin actions need an idempotency key and write an audit row.
- **Dependencies:** pinned exactly with lockfiles (`go.sum`, the Python lockfile the repo uses, `package-lock.json`).

## Definition of done for a phase
1. The repo's existing tests and checks still pass, and the new ones pass. Show the real output.
2. The phase's Check from the spec has been run and its output shown.
3. docs/oncall/PROGRESS.md updated (what was done, the Check output, interview notes, what's next); committed in the repo's commit style naming the phase.

## Files this work keeps current
docs/oncall/AUDIT.md, docs/oncall/PROGRESS.md, docs/oncall/DECISIONS.md, docs/oncall/BLOCKERS.md, and at the end docs/oncall/DONE.md.
