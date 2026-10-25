#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
container="lambdadb-cdc-test-${RANDOM}-$$"
cleanup() { docker rm -f "$container" >/dev/null; }
trap cleanup EXIT
docker run --detach --name "$container" \
  --publish 127.0.0.1::5432 \
  --env POSTGRES_PASSWORD=cdc-local-only --env POSTGRES_DB=cdc_spike \
  postgres:17-alpine -c wal_level=logical -c max_replication_slots=8 -c max_wal_senders=8 >/dev/null
ready=false
for ((i=0; i<60; i++)); do
  if docker exec "$container" pg_isready -U postgres -d cdc_spike >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  echo "isolated PostgreSQL did not become ready" >&2
  exit 1
fi
port="$(docker port "$container" 5432/tcp)"
export CDC_TEST_POSTGRES_DSN="postgres://postgres:cdc-local-only@127.0.0.1:${port##*:}/cdc_spike?sslmode=disable"
go test -v -race -count=1 -timeout 5m ./...
