#!/usr/bin/env bash
# 복구 완료 후 실행한다. 공유 서비스는 켜 두고 Airflow와 다른 테스트는 중지한다.
set -euo pipefail
project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

uv run python - <<'PY'
import os
import re
from dataclasses import replace

import psycopg
from botocore.exceptions import ClientError
from psycopg import sql

from src.common.database import environment_values
from src.ingestion.bronze.storage import SeaweedFSSettings, seaweedfs_s3_client

values = environment_values()
source_database = values["COMMERCE_SOURCE_DB"]
metadata_database = values["PIPELINE_METADATA_DB"]
source_copy = os.environ.get("TEST_SOURCE_DB", "commerce_source_it")
metadata_copy = os.environ.get("TEST_METADATA_DB", "pipeline_metadata_it")
bucket = os.environ.get("TEST_BUCKET", "commerce-lake-it")

# 접속하거나 삭제하기 전에 모든 대상 이름을 검증한다.
for name in (source_copy, metadata_copy):
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,59}_it", name):
        raise SystemExit(f"격리 DB 이름은 소문자 식별자이고 _it로 끝나야 합니다: {name}")
    if name in (source_database, metadata_database):
        raise SystemExit("격리 DB가 원본 DB와 같습니다. 공유 환경 설정으로 실행하세요.")
if source_copy == metadata_copy:
    raise SystemExit("격리 Source와 Metadata DB는 달라야 합니다.")
if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,58}-it", bucket):
    raise SystemExit("격리 Bucket은 소문자 이름이며 -it로 끝나야 합니다.")
if bucket == values["SEAWEEDFS_BUCKET"]:
    raise SystemExit("격리 Bucket이 원본 Bucket과 같습니다. 공유 환경 설정으로 실행하세요.")

admin = dict(
    host=values.get("POSTGRES_HOST", "localhost"),
    port=int(values["POSTGRES_PORT"]),
    user=values["POSTGRES_USER"],
    password=values["POSTGRES_PASSWORD"],
)
with psycopg.connect(**admin, dbname="postgres", autocommit=True) as connection:
    for original, target, role in (
        (source_database, source_copy, values["SOURCE_DB_USER"]),
        (metadata_database, metadata_copy, values["PIPELINE_DB_USER"]),
    ):
        connection.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(target))
        )
        connection.execute(
            # public Schema의 pg_database_owner 권한은 사본 DB의 Owner에게 적용된다.
            # 관리자 기본 소유로 복사하면 실행 역할의 CREATE 권한이 사라진다.
            sql.SQL("CREATE DATABASE {} TEMPLATE {} OWNER {}").format(
                sql.Identifier(target), sql.Identifier(original), sql.Identifier(role)
            )
        )
        connection.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(target), sql.Identifier(role)
            )
        )

# 복사한 Catalog의 Object는 공유 Bucket을 가리키므로 새 Bucket에서 쓰지 않는다.
# Seed·Generator 이력은 Source 사본의 기준선과 일치하도록 보존한다.
catalog_tables = (
    "bronze_objects", "quarantine_batches", "pipeline_runs", "watermarks",
    "mart_publish_runs", "warehouse_run_reports", "source_mutation_leases",
)
with psycopg.connect(**admin, dbname=metadata_copy) as connection:
    existing = [
        name for name in catalog_tables
        if connection.execute("SELECT to_regclass(%s)", (name,)).fetchone()[0] is not None
    ]
    if existing:
        connection.execute(sql.SQL("TRUNCATE {} RESTART IDENTITY").format(
            sql.SQL(", ").join(sql.Identifier(name) for name in existing)
        ))

settings = replace(SeaweedFSSettings.from_environment(), bucket=bucket)
client = seaweedfs_s3_client(settings)
try:
    client.head_bucket(Bucket=bucket)
except ClientError as error:
    if error.response.get("Error", {}).get("Code") not in {"404", "NoSuchBucket", "NotFound"}:
        raise
else:
    # 공유 Bucket은 이름 검증으로 제외했다. 테스트 Bucket만 다시 만든다.
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        for item in page.get("Contents", ()):
            client.delete_object(Bucket=bucket, Key=item["Key"])
    client.delete_bucket(Bucket=bucket)
client.create_bucket(Bucket=bucket)
print("격리 환경 준비 완료. 다음 환경 변수로 상태 변경 테스트를 실행하세요:")
print(f"COMMERCE_SOURCE_DB={source_copy} PIPELINE_METADATA_DB={metadata_copy} "
      f"SEAWEEDFS_BUCKET={bucket} RUN_POSTGRES_INTEGRATION=1 "
      "RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/integration")
PY
