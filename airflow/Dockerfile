FROM apache/airflow:2.8.1-python3.11

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN mkdir -p /opt/airflow/dags
WORKDIR /opt/airflow

# Install shared package with Airflow constraints and copy DAGs
USER root
COPY backend /tmp/backend
RUN chown -R airflow:0 /tmp/backend
USER airflow
ARG AIRFLOW_VERSION=2.8.1
ARG CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt
RUN pip install --no-compile --prefer-binary -c "${CONSTRAINT_URL}" /tmp/backend[ml]
COPY backend/dags /opt/airflow/dags

ENV AIRFLOW_HOME=/opt/airflow

