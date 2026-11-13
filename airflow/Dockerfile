ARG AIRFLOW_VERSION=3.3.1
FROM ghcr.io/astral-sh/uv:0.12.9 AS uv
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

ARG AIRFLOW_VERSION

COPY --from=uv /uv /usr/local/bin/uv
COPY --chown=airflow:root pyproject.toml uv.lock /tmp/project/

# --locked: pyproject.toml과 uv.lock이 어긋나면 빌드를 실패시킨다(--frozen은 검사 없이 통과).
RUN uv export --project /tmp/project --locked --no-dev --no-emit-project --no-hashes \
        --format requirements-txt -o /tmp/requirements.txt \
    && pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt
