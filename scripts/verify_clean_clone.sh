#!/usr/bin/env bash
# 새 Clone 경로에서 Phase 0~7 검증 명령을 처음부터 순서대로 실행하고 로그를 남긴다.
set -euo pipefail

source_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
clone_dir="$(mktemp -d /tmp/cdp-clean-clone-XXXXXX)"
compose_project="cdp-clean-clone"
log_file="${VERIFY_LOG:-/tmp/cdp-clean-clone-$(date -u +%Y%m%dT%H%M%SZ).log}"
tables=(customers customer_subscriptions customer_membership_tiers subscription_payments products sellers orders order_items order_payments)

cleanup() {
  if [[ -f "$clone_dir/compose.yaml" ]]; then
    (cd "$clone_dir" && docker compose -p "$compose_project" down -v >/dev/null 2>&1) || true
  fi
  rm -rf "$clone_dir"
}
trap cleanup EXIT
step() { echo "==> $*"; }
exec > >(tee -a "$log_file") 2>&1
echo "clean clone verification log: $log_file"

if [[ -n "$(docker compose -p commerce-data-platform ps -q 2>/dev/null)" ]]; then
  echo "Main compose stack is running. Stop it first: docker compose down" >&2
  exit 1
fi
step "clone"
git clone --quiet "$source_root" "$clone_dir"
cp "$source_root/.env" "$clone_dir/.env"
mkdir -p "$clone_dir/data"
mkdir -p "$clone_dir/data/raw"
cp -R "$source_root/data/raw/." "$clone_dir/data/raw/"
cd "$clone_dir"
step "dependencies and static checks"
uv sync --frozen
uv run ruff check .
uv run pytest
step "containers"
docker compose -p "$compose_project" up -d --wait postgres seaweedfs
step "seed"
uv run python -m src.seed --seeded-at 2026-09-03T00:00:00Z
step "generator"
uv run python -m src.generator --seed 7 --logical-date 2026-09-04T00:00:00Z --orders 20 --initialize-metadata
step "ingestion"
uv run python -m src.ingestion --dag-id clean_clone_verification --logical-date 2026-09-04T00:00:00Z --tables "${tables[@]}"
step "publish"
uv run python -m src.warehouse.publish --pipeline-name clean_clone_verification
step "integration tests"
RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest -m "integration and not airflow"
echo "clean clone verification passed"
