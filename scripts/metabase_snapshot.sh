#!/usr/bin/env bash
# Metabase API Key로 Phase 10 Dashboard와 Card 설정을 JSON으로 보존한다.
set -euo pipefail

if [[ -z "${METABASE_API_KEY:-}" && -f .env ]]; then
  METABASE_API_KEY="$(sed -n 's/^METABASE_API_KEY=//p' .env | head -n 1)"
fi
if [[ -z "${METABASE_API_KEY:-}" ]]; then
  echo "METABASE_API_KEY가 필요합니다." >&2
  exit 1
fi

METABASE_URL="${METABASE_URL:-http://localhost:3000}"
OUTPUT_DIR="${METABASE_SNAPSHOT_DIR:-metabase/export}"
mkdir -p "$OUTPUT_DIR"

for dashboard_id in 2 3 4; do
  curl --fail --silent --show-error \
    -H "X-API-Key: ${METABASE_API_KEY}" \
    "${METABASE_URL}/api/dashboard/${dashboard_id}" \
    -o "${OUTPUT_DIR}/dashboard-${dashboard_id}.json"
done

for card_id in $(seq 46 59); do
  curl --fail --silent --show-error \
    -H "X-API-Key: ${METABASE_API_KEY}" \
    "${METABASE_URL}/api/card/${card_id}" \
    -o "${OUTPUT_DIR}/card-${card_id}.json"
done
