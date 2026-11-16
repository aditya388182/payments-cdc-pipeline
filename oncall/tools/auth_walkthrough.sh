#!/usr/bin/env bash
# curl walkthrough of the whole OAuth 2.0 + PKCE flow against the running API and the mock GitHub server.
# Secrets are never printed: tokens are shortened and the client secret is never read by this script.
set -uo pipefail
API=${API:-http://127.0.0.1:8088}
REDIRECT='oncall://auth/callback'
short() { printf '%s…' "${1:0:14}"; }
b64url() { openssl base64 -A | tr '+/' '-_' | tr -d '='; }
urlenc() { python3 -c 'import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1],safe=""))' "$1"; }
jget() { python3 -c 'import json,sys;d=json.load(sys.stdin);print(d.get(sys.argv[1],"") if not isinstance(d.get(sys.argv[1]),dict) else json.dumps(d[sys.argv[1]]))' "$1"; }
post_token() { curl -s -w '\n%{http_code}' -X POST "$API/v1/auth/token" -H 'Content-Type: application/json' -d "$1"; }

# runs the browser part for $1 (GitHub login) and prints the deep-link query; globals: VERIFIER CODE ERROR
signin() {
  VERIFIER=$(head -c 48 /dev/urandom | b64url)
  local challenge; challenge=$(printf '%s' "$VERIFIER" | openssl dgst -sha256 -binary | b64url)
  echo "  app: code_verifier=$(short "$VERIFIER")  code_challenge=$(short "$challenge") (S256)"
  local loc cb app
  loc=$(curl -s -o /dev/null -w '%{redirect_url}' "$API/v1/auth/github/start?code_challenge=$challenge&code_challenge_method=S256&redirect_uri=$(urlenc "$REDIRECT")")
  echo "  GET /v1/auth/github/start -> 302 ${loc%%\?*}?client_id=…&redirect_uri=…/v1/auth/github/callback&state=…&scope=read:user"
  case "$loc" in *client_secret*) echo "  FAIL: client secret in the redirect"; exit 1;; esac
  cb=$(curl -s -o /dev/null -w '%{redirect_url}' "$loc&login=$1")
  echo "  user signs in as '$1' at GitHub -> 302 ${cb%%\?*}?code=…&state=…"
  app=$(curl -s -o /dev/null -w '%{redirect_url}' "$cb")
  CODE=$(sed -n 's/.*[?&]code=\([^&]*\).*/\1/p' <<<"$app"); ERROR=$(sed -n 's/.*[?&]error=\([^&]*\).*/\1/p' <<<"$app")
  echo "  callback exchanges the GitHub code server-side, checks the allowlist -> 302 ${app%%\?*}?${CODE:+code=…}${ERROR:+error=$ERROR}"
}

echo "== A. admin signs in (happy path)"
signin admin-user
out=$(post_token "{\"code\":\"$CODE\",\"code_verifier\":\"$VERIFIER\"}"); body=${out%$'\n'*}; echo "  POST /v1/auth/token {code, code_verifier} -> HTTP ${out##*$'\n'}  user=$(jget user <<<"$body") expires_in=$(jget expires_in <<<"$body")"
ACCESS=$(jget access_token <<<"$body"); REFRESH1=$(jget refresh_token <<<"$body")
echo "  access_token=$(short "$ACCESS") refresh_token=$(short "$REFRESH1")"
echo "  GET /v1/me           -> $(curl -s -w ' HTTP %{http_code}' "$API/v1/me" -H "Authorization: Bearer $ACCESS")"
echo "  GET /v1/audit (admin) -> HTTP $(curl -s -o /dev/null -w '%{http_code}' "$API/v1/audit" -H "Authorization: Bearer $ACCESS")"
echo "  same one-time code again -> HTTP $(post_token "{\"code\":\"$CODE\",\"code_verifier\":\"$VERIFIER\"}" | tail -1) (single use)"

echo "== B. refresh-token rotation and reuse detection"
out=$(post_token "{\"refresh_token\":\"$REFRESH1\"}"); body=${out%$'\n'*}; REFRESH2=$(jget refresh_token <<<"$body")
echo "  POST /v1/auth/token {refresh_token: R1} -> HTTP ${out##*$'\n'}, new refresh token R2=$(short "$REFRESH2") (R1 != R2: $([ "$REFRESH1" != "$REFRESH2" ] && echo yes || echo NO))"
out=$(post_token "{\"refresh_token\":\"$REFRESH1\"}"); echo "  presenting the OLD R1 again -> HTTP ${out##*$'\n'} $(jget error <<<"${out%$'\n'*}")"
out=$(post_token "{\"refresh_token\":\"$REFRESH2\"}"); echo "  then the NEWEST R2 -> HTTP ${out##*$'\n'} (the whole family was revoked by the reuse)"

echo "== C. PKCE mismatch"
signin admin-user
WRONG=$(head -c 48 /dev/urandom | b64url)
out=$(post_token "{\"code\":\"$CODE\",\"code_verifier\":\"$WRONG\"}"); echo "  wrong code_verifier -> HTTP ${out##*$'\n'} $(jget error <<<"${out%$'\n'*}")"
out=$(post_token "{\"code\":\"$CODE\",\"code_verifier\":\"$VERIFIER\"}"); echo "  right verifier afterwards -> HTTP ${out##*$'\n'} (the failed attempt burned the code)"

echo "== D. state mismatch (CSRF): a forged callback"
echo "  GET /v1/auth/github/callback?code=x&state=forged -> HTTP $(curl -s -o /dev/null -w '%{http_code} location=%{redirect_url}' "$API/v1/auth/github/callback?code=x&state=forged")"

echo "== E. viewer vs admin"
signin viewer-user
out=$(post_token "{\"code\":\"$CODE\",\"code_verifier\":\"$VERIFIER\"}"); body=${out%$'\n'*}; VACCESS=$(jget access_token <<<"$body")
echo "  user=$(jget user <<<"$body")"
echo "  viewer GET /v1/snapshot -> HTTP $(curl -s -o /dev/null -w '%{http_code}' "$API/v1/snapshot" -H "Authorization: Bearer $VACCESS")"
echo "  viewer GET /v1/audit    -> $(curl -s -w ' HTTP %{http_code}' "$API/v1/audit" -H "Authorization: Bearer $VACCESS")"
echo "  no token GET /v1/audit  -> $(curl -s -w ' HTTP %{http_code}' "$API/v1/audit")"

echo "== F. a GitHub user that is not on the allowlist"
signin outsider
echo "  (no code was issued)"

echo "== G. logout revokes the refresh family"
signin admin-user
out=$(post_token "{\"code\":\"$CODE\",\"code_verifier\":\"$VERIFIER\"}"); body=${out%$'\n'*}; A=$(jget access_token <<<"$body"); R=$(jget refresh_token <<<"$body")
echo "  POST /v1/auth/logout -> HTTP $(curl -s -o /dev/null -w '%{http_code}' -X POST "$API/v1/auth/logout" -H "Authorization: Bearer $A")"
out=$(post_token "{\"refresh_token\":\"$R\"}"); echo "  refresh after logout -> HTTP ${out##*$'\n'}"
