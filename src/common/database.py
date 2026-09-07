"""PostgreSQL connection settings shared by Phase 1 components."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import psycopg

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class PostgresSettings:
    """Connection inputs kept in the local environment file, never in source control."""

    host: str
    port: int
    admin_user: str
    admin_password: str
    source_database: str
    source_user: str
    source_password: str
    pipeline_database: str
    pipeline_user: str
    pipeline_password: str

    @classmethod
    def from_environment(cls) -> PostgresSettings:
        values = environment_values()
        required = (
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "POSTGRES_PORT",
            "COMMERCE_SOURCE_DB",
            "SOURCE_DB_USER",
            "SOURCE_DB_PASSWORD",
            "PIPELINE_METADATA_DB",
            "PIPELINE_DB_USER",
            "PIPELINE_DB_PASSWORD",
        )
        missing = [name for name in required if not values.get(name)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

        return cls(
            host=values.get("POSTGRES_HOST", "localhost"),
            port=int(values["POSTGRES_PORT"]),
            admin_user=values["POSTGRES_USER"],
            admin_password=values["POSTGRES_PASSWORD"],
            source_database=values["COMMERCE_SOURCE_DB"],
            source_user=values["SOURCE_DB_USER"],
            source_password=values["SOURCE_DB_PASSWORD"],
            pipeline_database=values["PIPELINE_METADATA_DB"],
            pipeline_user=values["PIPELINE_DB_USER"],
            pipeline_password=values["PIPELINE_DB_PASSWORD"],
        )

    def source_connection(self) -> psycopg.Connection:
        return psycopg.connect(
            host=self.host,
            port=self.port,
            dbname=self.source_database,
            user=self.source_user,
            password=self.source_password,
        )

    def pipeline_connection(self) -> psycopg.Connection:
        return psycopg.connect(
            host=self.host,
            port=self.port,
            dbname=self.pipeline_database,
            user=self.pipeline_user,
            password=self.pipeline_password,
        )


def apply_sql_file(connection: psycopg.Connection, relative_path: str) -> None:
    """Apply an idempotent SQL file from the repository root."""
    sql_path = PROJECT_ROOT / relative_path
    connection.execute(sql_path.read_text(encoding="utf-8"))
    connection.commit()


def environment_values() -> dict[str, str]:
    """Process Environment에 없는 값만 로컬 `.env`에서 읽어 반환한다."""
    values = dict(os.environ)
    environment_path = PROJECT_ROOT / ".env"
    if not environment_path.is_file():
        return values
    for line in environment_path.read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", maxsplit=1)
            values.setdefault(key, value)
    return values
