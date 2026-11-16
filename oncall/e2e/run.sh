#!/usr/bin/env bash
# Full local end-to-end run (`make oncall-e2e`): the live pipeline + the Go API + mock GitHub OAuth + the Expo WEB build
# driven by Playwright. Too heavy for a hosted CI runner (Kafka + Spark + browsers), so it stays local and its results
# are committed (results/oncall/e2e.json, docs/oncall/screenshots/).
set -uo pipefail
cd "$(dirname "$0")/../.."
ROOT=$PWD
export PATH="$HOME/.local/bin:$HOME/go/bin:$PATH"
mkdir -p logs .run docs/oncall/screenshots results/oncall

echo "== 1. stack"
oncall/tools/stack.sh up || exit 1
echo "== 2. API with demo alert thresholds (lag > 100 for 6 s)"
ONCALL_ALERT_LAG=100 ONCALL_ALERT_LAG_FOR=6s oncall/tools/stack.sh restart-api || exit 1

echo "== 3. Expo web build"
(cd oncall/mobile && EXPO_PUBLIC_API_URL=http://127.0.0.1:8088 CI=1 npx expo export --platform web >"$ROOT/logs/expo_export.log" 2>&1) || { tail -20 logs/expo_export.log; exit 1; }
if [ -f .run/web.pid ] && kill -0 "$(cat .run/web.pid)" 2>/dev/null; then kill -TERM "$(cat .run/web.pid)"; sleep 1; fi
(cd oncall/mobile && PORT=19006 nohup node scripts/serve-web.mjs dist >"$ROOT/logs/web.log" 2>&1 & echo $! > "$ROOT/.run/web.pid")
sleep 2; curl -sf http://127.0.0.1:19006/ >/dev/null || { echo "web server did not start"; exit 1; }

echo "== 4. Playwright"
rc=0
(cd oncall/mobile && npx playwright test 2>&1 | tee "$ROOT/logs/e2e_playwright.log") || rc=$?

echo "== 5. cleanup and results"
set -a; . ./.env; set +a
TOK=$(oncall/bin/oncall-api token -sub admin-user -role admin)
curl -s -o /dev/null -X POST http://127.0.0.1:8088/v1/generator -H "Authorization: Bearer $TOK" -H "Idempotency-Key: e2e-cleanup-$(date +%s)" -H 'Content-Type: application/json' -d '{"action":"stop"}' || true
venv/bin/python oncall/tools/e2e_results.py || true
oncall/tools/stack.sh restart-api >/dev/null 2>&1 || true
exit $rc
