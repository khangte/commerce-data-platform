#!/usr/bin/env bash
set -euo pipefail

create_role() {
  local role_name="$1"
  local role_password="$2"

  psql --username "$POSTGRES_USER" --dbname postgres \
    --set=role_name="$role_name" \
    --set=role_password="$role_password" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'role_name', :'role_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'role_name')
\gexec
SQL
}

create_database() {
  local database_name="$1"
  local owner_name="$2"

  psql --username "$POSTGRES_USER" --dbname postgres \
    --set=database_name="$database_name" \
    --set=owner_name="$owner_name" <<'SQL'
SELECT format('CREATE DATABASE %I OWNER %I', :'database_name', :'owner_name')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'database_name')
\gexec
SQL
}

create_role "$SOURCE_DB_USER" "$SOURCE_DB_PASSWORD"
create_role "$AIRFLOW_DB_USER" "$AIRFLOW_DB_PASSWORD"
create_role "$PIPELINE_DB_USER" "$PIPELINE_DB_PASSWORD"

create_database "$COMMERCE_SOURCE_DB" "$SOURCE_DB_USER"
create_database "$AIRFLOW_METADATA_DB" "$AIRFLOW_DB_USER"
create_database "$PIPELINE_METADATA_DB" "$PIPELINE_DB_USER"
