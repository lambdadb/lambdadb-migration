#!/usr/bin/env bash
# Cross-repository local component test. No cloud credentials or deployment.
set -euo pipefail
cd "$(dirname "$0")/.."
: "${CONNECTION_TEST_BACKEND_DIR:?backend worktree required}"
: "${CONNECTION_TEST_ENGINE_DIR:?engine worktree required}"
run_dir="${CONNECTION_TEST_OUTPUT_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/connections-engine.XXXXXX")}"
if [[ -e "$run_dir/target-url" || -e "$run_dir/target-pid" ]]; then
  echo 'Choose a fresh evidence directory; existing target evidence is preserved' >&2
  exit 1
fi
mkdir -p "$run_dir/efs" "$run_dir/writer"
container=""
gradle_pid=""
cleanup() {
  if [[ -f "$run_dir/target-pid" ]]; then kill "$(cat "$run_dir/target-pid")" 2>/dev/null || true; fi
  if [[ -n "$gradle_pid" ]]; then kill "$gradle_pid" 2>/dev/null || true; wait "$gradle_pid" 2>/dev/null || true; fi
  if [[ -n "$container" ]]; then docker rm -f "$container" >/dev/null; fi
}
trap cleanup EXIT
if [[ -z "${CDC_TEST_POSTGRES_DSN:-}" ]]; then
  container="connections-engine-${RANDOM}-$$"
  docker run --detach --name "$container" --publish 127.0.0.1::5432 \
    --env POSTGRES_PASSWORD=connections-local-only --env POSTGRES_DB=connections_test \
    postgres:17-alpine -c wal_level=logical -c max_replication_slots=16 -c max_wal_senders=16 >/dev/null
  ready=false
  for ((i=0;i<60;i++)); do
    if docker exec "$container" pg_isready -U postgres -d connections_test >/dev/null 2>&1; then ready=true; break; fi
    sleep 1
  done
  [[ "$ready" == true ]] || { echo 'Local PostgreSQL did not become ready' >&2; exit 1; }
  port="$(docker port "$container" 5432/tcp)"
  export CDC_TEST_POSTGRES_DSN="postgres://postgres:connections-local-only@127.0.0.1:${port##*:}/connections_test?sslmode=disable"
fi
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$run_dir/server.key" -out "$run_dir/server.crt" \
  -days 1 -subj /CN=localhost -addext 'subjectAltName=IP:127.0.0.1,DNS:localhost' >"$run_dir/tls.log" 2>&1
openssl pkcs12 -export -in "$run_dir/server.crt" -inkey "$run_dir/server.key" \
  -out "$run_dir/target.p12" -passout pass:local-only >>"$run_dir/tls.log" 2>&1
(cd "$CONNECTION_TEST_BACKEND_DIR/internal/stack/lambdadb-api" && go build -o "$run_dir/connection-regional" ./cmd/connection-regional)
go build -o "$run_dir/connection-worker" ./cmd/connection-worker
(
  cd "$CONNECTION_TEST_ENGINE_DIR"
  EFS_MOUNT_PATH="$run_dir/efs" FS_TEMP_PATH="$run_dir/writer" \
    ./gradlew :runtime-aws:managedConnectionTarget --args="\"$run_dir\" \"$run_dir/target.p12\""
) >"$run_dir/engine.log" 2>&1 &
gradle_pid=$!
ready=false
for ((i=0;i<120;i++)); do
  if [[ -s "$run_dir/target-url" ]]; then ready=true; break; fi
  if ! kill -0 "$gradle_pid" 2>/dev/null; then cat "$run_dir/engine.log"; exit 1; fi
  sleep 1
done
[[ "$ready" == true ]] || { echo 'Engine component target did not become ready' >&2; exit 1; }
export CONNECTION_TEST_TARGET_URL="$(cat "$run_dir/target-url")"
export CONNECTION_TEST_CA_FILE="$run_dir/server.crt"
export CONNECTION_TEST_TLS_CERT="$run_dir/server.crt"
export CONNECTION_TEST_TLS_KEY="$run_dir/server.key"
export CONNECTION_TEST_WORKER_BINARY="$run_dir/connection-worker"
export CONNECTION_TEST_REGIONAL_BINARY="$run_dir/connection-regional"
go test -race -v -count=1 -shuffle=on -timeout 4m ./internal/connections -run '^TestEngineProcess' 2>&1 | tee "$run_dir/worker-tests.log"
printf 'Component evidence retained at %s\n' "$run_dir"
