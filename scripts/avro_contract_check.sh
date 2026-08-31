#!/usr/bin/env bash
set -euo pipefail

REGISTRY_URL=${REGISTRY_URL:-"http://localhost:8081"}
SUBJECT="payments.public.transactions-value"
BASELINE="schemas/transactions_v2.avsc"
PROPOSED=${PROPOSED_SCHEMA:-"schemas/transactions_proposed.avsc"}

if [ ! -f "$PROPOSED" ]; then
  echo "No proposed schema found at $PROPOSED. Comparing baseline to baseline."
  PROPOSED="$BASELINE"
fi

# Fail closed if registry is unreachable
if ! curl -s -o /dev/null "$REGISTRY_URL"; then
  echo "Registry at $REGISTRY_URL is unreachable. BREAKING SCHEMA CHANGE — contract test failed closed"
  exit 1
fi

PAYLOAD=$(jq -Rs '{schema: .}' "$PROPOSED")
HTTP_STATUS=$(curl -s -o /tmp/response.json -w "%{http_code}" -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data "$PAYLOAD" "$REGISTRY_URL/compatibility/subjects/$SUBJECT/versions/latest")

IS_COMPATIBLE=$(jq -r '.is_compatible' /tmp/response.json)

if [ "$IS_COMPATIBLE" != "true" ]; then
  echo "BREAKING SCHEMA CHANGE — contract test failed closed"
  exit 1
fi
echo "Schema is fully compatible."
exit 0