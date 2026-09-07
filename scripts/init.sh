#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required. Install uv, then run this script again." >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is required. Install Docker Desktop with WSL integration, then run again." >&2
  exit 1
fi

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env from .env.example. Replace example secrets before starting services."
fi

uv sync --frozen
uv run python --version
uv run ruff check .
uv run pytest
docker compose config >/dev/null

echo "Bootstrap validation completed."
