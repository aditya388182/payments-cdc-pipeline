#!/usr/bin/env bash
# `make ws-load`: k6 WebSocket load test with 1,000 clients, twice:
#   baseline      - 1,000 fast clients
#   slow_clients  - the same 1,000 fast clients plus SLOW slow readers (tiny receive buffer, one frame read per 6 s while the server sends one per 2 s)
# Local, on this box (clients and server share it), results in results/oncall/ws_load.json.
set -uo pipefail
cd "$(dirname "$0")/../.."
ROOT=$PWD
export PATH="$HOME/.local/bin:$HOME/go/bin:$PATH"
TARGET=${TARGET:-1000}
SLOW=${SLOW:-100}
RAMP=${RAMP:-60s}
HOLD=${HOLD:-5m}
SNDBUF=${SNDBUF:-16384}
SLOW_READ_EVERY=${SLOW_READ_EVERY:-6s}
mkdir -p results/oncall/raw logs

# 1,000 sockets need more than the default 1024 descriptors for both the API and k6
ulimit -n 65535 2>/dev/null || ulimit -n "$(ulimit -Hn)" 2>/dev/null || true
echo "file descriptor limit: $(ulimit -n)"

oncall/tools/stack.sh up || exit 1
set -a; . ./.env; set +a
# no traffic from the pipeline during the run: snapshots keep coming every 2 s but nothing else competes for the box
TOK=$(oncall/bin/oncall-api token -sub admin-user -role admin)
curl -s -o /dev/null -X POST http://127.0.0.1:8088/v1/generator -H "Authorization: Bearer $TOK" -H "Idempotency-Key: wsload-stop-$(date +%s)" -H 'Content-Type: application/json' -d '{"action":"stop"}' || true

secs() { case "$1" in *m) echo $(( ${1%m} * 60 ));; *s) echo "${1%s}";; *) echo "$1";; esac; }
DURATION=$(( $(secs "$RAMP") + $(secs "$HOLD") + 45 ))

run_variant() {
  local variant=$1
  echo "== variant: $variant  (clients=$TARGET ramp=$RAMP hold=$HOLD server SO_SNDBUF=$SNDBUF)"
  ONCALL_WS_SNDBUF=$SNDBUF oncall/tools/stack.sh restart-api >/dev/null 2>&1 || { echo "API restart failed"; return 1; }
  sleep 3
  local tok
  tok=$(oncall/bin/oncall-api token -sub viewer-user -role viewer -ttl 60m)
  venv/bin/python oncall/loadtest/sample_metrics.py "results/oncall/raw/metrics_${variant}.csv" "$DURATION" 5 &
  local sampler=$!
  local slowpid=""
  if [ "$variant" = "slow_clients" ]; then
    (cd oncall/api && go build -o ../bin/ws-slowclients ./cmd/ws-slowclients) || return 1
    TOKEN=$tok oncall/bin/ws-slowclients -n "$SLOW" -read-every "$SLOW_READ_EVERY" -duration "$(( $(secs "$RAMP") + $(secs "$HOLD") + 20 ))s" -out "results/oncall/raw/slow_clients.json" >logs/ws_slowclients.log 2>&1 &
    slowpid=$!
  fi
  TOKEN=$tok k6 run -q -e VARIANT="$variant" -e TARGET="$TARGET" -e RAMP="$RAMP" -e HOLD="$HOLD" oncall/loadtest/ws.js 2>&1 | tail -25
  [ -n "$slowpid" ] && wait "$slowpid"
  kill "$sampler" 2>/dev/null; wait "$sampler" 2>/dev/null
  return 0
}

for v in ${VARIANTS:-baseline slow_clients}; do run_variant "$v"; done
venv/bin/python oncall/tools/ws_load_results.py
oncall/tools/stack.sh restart-api >/dev/null 2>&1 || true
