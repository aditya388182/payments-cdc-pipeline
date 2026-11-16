#!/usr/bin/env bash
# Writes .env from .env.example with random local-dev values. Never prints it; never overwrites an existing .env.
set -euo pipefail
cd "$(dirname "$0")/../.."
if [ -f .env ]; then echo ".env already exists - leaving it alone"; exit 0; fi
rand() { head -c 24 /dev/urandom | base64 | tr -d '=+/\n' | head -c 32; }
PGPASS=$(rand)
umask 077
sed -e "s|oncall:CHANGEME@|oncall:${PGPASS}@|" \
    -e "s|^ONCALL_JWT_SECRET=CHANGEME|ONCALL_JWT_SECRET=$(rand)$(rand)|" \
    -e "s|^ONCALL_GITHUB_CLIENT_SECRET=CHANGEME|ONCALL_GITHUB_CLIENT_SECRET=$(rand)|" \
    .env.example > .env
echo "ONCALL_PG_PASSWORD=${PGPASS}" >> .env
echo "wrote .env (git-ignored, values hidden)"
