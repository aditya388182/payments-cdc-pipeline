#!/usr/bin/env bash
# Simulate hard crash of the Spark driver
set -euo pipefail

LOG_FILE="documents/chaos_log.txt"
mkdir -p docs

PID=$(pgrep -f "payments_cdc_job.py" | head -1 || true)

if [ -z "${PID}" ]; then
  echo "ERROR: no spark job (payments_cdc_job.py) is running" | tee -a "${LOG_FILE}"
  exit 1
fi

TS=$(date -Is)
echo "KILL -9 spark pid=${PID} @ ${TS}" | tee -a "${LOG_FILE}"
kill -9 "${PID}"

echo "Spark driver killed. Restart the job manually and wait for drain + parity."