FROM apache/airflow:2.10.4
USER airflow


ENV AIRFLOW_HOME=/opt/airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-amazon \
    apache-airflow-providers-snowflake \
    && rm -rf /home/airflow/.cache/pip
