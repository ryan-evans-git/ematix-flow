#!/usr/bin/env bash
# Phase Δ PR 6: register the Debezium Postgres connector against
# the running `connect` service. Idempotent — re-running after
# the connector exists returns 409, which we tolerate so the
# script is safe to invoke after a container restart.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
HERE="$(cd "$(dirname "$0")" && pwd)"

# Wait for connect to come up (image takes ~15-30s after compose up).
echo "Waiting for connect REST API at $CONNECT_URL ..."
until curl -sf "$CONNECT_URL/" >/dev/null 2>&1; do
    sleep 2
done
echo "connect is ready."

http_code=$(curl -s -o /tmp/cdc-register.out -w "%{http_code}" \
    -X POST -H "Content-Type: application/json" \
    --data @"$HERE/register-connector.json" \
    "$CONNECT_URL/connectors")

case "$http_code" in
    201) echo "Connector registered." ;;
    409) echo "Connector already exists — leaving it alone." ;;
    *)
        echo "Unexpected HTTP $http_code from /connectors:" >&2
        cat /tmp/cdc-register.out >&2
        exit 1
        ;;
esac

# Verify it's running rather than failed.
echo
echo "Connector status:"
curl -s "$CONNECT_URL/connectors/pg-source-connector/status" | python3 -m json.tool || true
