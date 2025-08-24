FROM apache/airflow:2.8.1-python3.11

# Install additional python packages needed by the DAGs
COPY airflow/requirements.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt

RUN mkdir -p /opt/airflow/dags
WORKDIR /opt/airflow

# Install shared package and copy DAGs
USER root
COPY backend /tmp/backend
RUN chown -R airflow:0 /tmp/backend
USER airflow
RUN pip install --no-cache-dir /tmp/backend
COPY backend/dags /opt/airflow/dags

ENV AIRFLOW_HOME=/opt/airflow

