#!/usr/bin/env bash
echo "KILL connect @ $(date -Is)"
docker compose -f infra/docker-compose.yml kill connect
sleep 15
docker compose -f infra/docker-compose.yml start connect
echo "connect restarted — waiting for RUNNING..."
until curl -s localhost:8083/connectors/payments-postgres-cdc/status | grep -q RUNNING; do sleep 5; done
echo "RUNNING"