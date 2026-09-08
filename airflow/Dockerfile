ARG AIRFLOW_VERSION=3.3.1
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

ARG AIRFLOW_VERSION

COPY --chown=airflow:root airflow/requirements.txt /requirements.txt

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
