#!/usr/bin/env bash
# Starts / stops / inspects the whole local stack with the repo's own commands plus the on-call console services.
#   oncall/tools/stack.sh up | down | status | restart-api
# Processes run in the background with pid files in .run/ and logs in logs/. Everything binds to 127.0.0.1.
set -uo pipefail
cd "$(dirname "$0")/../.."
ROOT=$PWD
export PATH="$HOME/.local/bin:$HOME/go/bin:$PATH"
mkdir -p .run logs oncall/bin
PY="$ROOT/venv/bin/python"

alive() { [ -f ".run/$1.pid" ] && kill -0 "$(cat ".run/$1.pid")" 2>/dev/null; }
start_bg() { # name logfile cmd...
  local name=$1 log=$2; shift 2
  if alive "$name"; then echo "  $name already running (pid $(cat ".run/$name.pid"))"; return; fi
  nohup "$@" >"$log" 2>&1 &
  echo $! > ".run/$name.pid"
  echo "  started $name (pid $!)"
}
stop_bg() {
  if alive "$1"; then kill -TERM "$(cat ".run/$1.pid")" 2>/dev/null; echo "  stopped $1"; fi
  : > ".run/$1.pid"
}
wait_for() { # description timeout cmd...
  local d=$1 t=$2; shift 2
  local i=0
  until "$@" >/dev/null 2>&1; do i=$((i+2)); [ $i -ge "$t" ] && { echo "  TIMEOUT waiting for $d"; return 1; }; sleep 2; done
  echo "  ready: $d"
}
load_env() { set -a; . ./.env; set +a; export ONCALL_REPO_DIR=$ROOT; }

lake_initialised() {
  curl -s --aws-sigv4 "aws:amz:us-east-1:s3" --user minioadmin:minioadmin \
    "http://127.0.0.1:9000/payments-lake?list-type=2&prefix=transactions/_delta_log/&max-keys=1" | grep -q "<Key>"
}

build() {
  (cd oncall/api && go build -o ../bin/oncall-api ./cmd/oncall-api && go build -o ../bin/mock-github ./cmd/mock-github) || { echo "go build failed"; return 1; }
}

up() {
  [ -f .env ] || oncall/tools/gen_env.sh
  load_env
  echo "== data plane (docker compose)"
  docker compose -f infra/docker-compose.yml up -d --no-recreate 2>&1 | grep -vi obsolete | tail -3
  wait_for "kafka broker" 90 docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list
  ./infra/kafka/topics.sh >/dev/null
  wait_for "kafka connect REST" 240 curl -sf http://127.0.0.1:8083/connectors
  if ! curl -sf http://127.0.0.1:8083/connectors/payments-postgres-cdc/status >/dev/null; then
    curl -s -X POST http://127.0.0.1:8083/connectors -H 'Content-Type: application/json' -d @connectors/debezium-postgres-connector.json >/dev/null
  fi
  wait_for "connector RUNNING" 60 bash -c "curl -s http://127.0.0.1:8083/connectors/payments-postgres-cdc/status | grep -q RUNNING"
  wait_for "object store" 60 curl -s -o /dev/null http://127.0.0.1:9000/
  oncall/tools/create_db.sh
  if ! lake_initialised; then echo "  initialising the Delta table"; (unset PYSPARK_SUBMIT_ARGS; PYTHONPATH=. "$PY" scripts/init_delta_table.py >logs/init_delta.log 2>&1); fi
  echo "== pipeline and console processes"
  build || return 1
  # workers must use the same interpreter as the driver, otherwise Spark aborts with PYTHON_VERSION_MISMATCH (the box has 3.12 on PATH)
  export PYTHONPATH=$ROOT PYSPARK_PYTHON=$PY PYSPARK_DRIVER_PYTHON=$PY; unset PYSPARK_SUBMIT_ARGS
  start_bg spark logs/spark_job.log "$PY" -m spark.jobs.payments_cdc_job --starting-offsets earliest
  start_bg sidecar logs/sidecar.log "$PY" scripts/lag_sidecar.py
  start_bg replay logs/replay_svc.log "$PY" -m oncall.replay.server
  start_bg mockgithub logs/mock_github.log oncall/bin/mock-github
  start_bg api logs/oncall_api.log oncall/bin/oncall-api
  wait_for "replay-svc ready" 120 grep -q '"msg": "ready"' logs/replay_svc.log
  wait_for "oncall-api healthy" 60 curl -sf http://127.0.0.1:8088/healthz
  echo "== up. API http://127.0.0.1:8088  (tunnel this port)  metrics http://127.0.0.1:8089/metrics"
}

down() {
  for n in api mockgithub replay sidecar spark; do stop_bg "$n"; done
  [ "${1:-}" = "--all" ] && docker compose -f infra/docker-compose.yml stop 2>&1 | grep -vi obsolete | tail -2
}

status() {
  for n in spark sidecar replay mockgithub api; do alive "$n" && echo "  $n: running (pid $(cat ".run/$n.pid"))" || echo "  $n: stopped"; done
  docker compose -f infra/docker-compose.yml ps --format '  {{.Name}}: {{.Status}}' 2>/dev/null | grep -vi obsolete
}

case "${1:-}" in
  up) up ;;
  down) shift; down "$@" ;;
  status) status ;;
  restart-api) load_env; stop_bg api; sleep 1; build && start_bg api logs/oncall_api.log oncall/bin/oncall-api && wait_for "oncall-api healthy" 60 curl -sf http://127.0.0.1:8088/healthz ;;
  *) echo "usage: $0 up|down [--all]|status|restart-api"; exit 2 ;;
esac
