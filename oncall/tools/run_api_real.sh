#!/usr/bin/env bash
# Runs oncall-api against the REAL GitHub OAuth (and optionally the real Expo push sender) using credentials that live
# outside the repo in ~/.config/oncall/github.env (see docs/oncall/BLOCKERS.md). Neither file is ever printed.
# The rest of the stack must already be up: oncall/tools/stack.sh up (then stop its mock-backed API first).
set -euo pipefail
cd "$(dirname "$0")/../.."
export PATH="$HOME/.local/bin:$HOME/go/bin:$PATH"

CFG="$HOME/.config/oncall/github.env"
[ -f .env ] || { echo "no .env: run oncall/tools/gen_env.sh"; exit 1; }
[ -f "$CFG" ] || { echo "missing $CFG: follow docs/oncall/BLOCKERS.md section 1"; exit 1; }

if [ -f .run/api.pid ] && kill -0 "$(cat .run/api.pid)" 2>/dev/null; then
  echo "stopping the mock-backed API (pid $(cat .run/api.pid))"
  kill -TERM "$(cat .run/api.pid)"; sleep 2
fi

set -a
. ./.env
. "$CFG"          # later assignments win: real GitHub base, client id/secret, allowlist, public URL
set +a
export ONCALL_REPO_DIR="$PWD"

case "${ONCALL_GITHUB_BASE:-}" in
  https://github.com) ;;
  *) echo "ONCALL_GITHUB_BASE is not https://github.com: this script is for the real GitHub"; exit 1 ;;
esac
[ -n "${ONCALL_GITHUB_CLIENT_ID:-}" ] && [ -n "${ONCALL_GITHUB_CLIENT_SECRET:-}" ] || { echo "client id/secret missing in $CFG"; exit 1; }

(cd oncall/api && go build -o ../bin/oncall-api ./cmd/oncall-api)
echo "oncall-api on ${ONCALL_ADDR:-127.0.0.1:8088}, public URL ${ONCALL_PUBLIC_URL:-?}, push sender ${ONCALL_PUSH_SENDER:-fake}"
exec oncall/bin/oncall-api
