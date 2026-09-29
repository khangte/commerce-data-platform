ARG AIRFLOW_VERSION=3.3.1
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

ARG AIRFLOW_VERSION

COPY --chown=airflow:root pyproject.toml /pyproject.toml

RUN python -c "import tomllib; print('\n'.join(tomllib.load(open('/pyproject.toml','rb'))['project']['dependencies']))" > /requirements.txt \
    && pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
