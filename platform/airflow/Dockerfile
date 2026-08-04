FROM apache/airflow:3.3.0-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*
USER airflow

RUN pip install --no-cache-dir \
    "astronomer-cosmos==1.15.0" \
    "apache-airflow-providers-openlineage==2.19.0" \
    "apache-airflow-providers-docker==4.4.0" \
    "boto3>=1.34,<2"

# Keep dbt's fast-moving dependency graph isolated from Airflow's constraints.
RUN python -m venv /home/airflow/dbt_venv && \
    /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
    "dbt-core==1.11.12" "dbt-postgres==1.11.0" \
    "dbt-metricflow[dbt-postgres]==0.13.0"

COPY --chown=airflow:root platform/dbt/packages.yml platform/dbt/dbt_project.yml /tmp/dbt-deps/
RUN mkdir -p /opt/airflow/dbt_packages && \
    /home/airflow/dbt_venv/bin/dbt deps --project-dir /tmp/dbt-deps
