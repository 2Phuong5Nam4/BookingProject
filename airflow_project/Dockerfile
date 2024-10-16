# Use the official Airflow image as the base
FROM apache/airflow:2.10.2

# Set the environment variables for Airflow
ENV AIRFLOW_HOME=/opt/airflow
USER root
# Install additional dependencies (e.g., libraries or system packages)
# You can add Python libraries here or system utilities
RUN apt-get update \
    && apt-get install -y


# Set permissions if needed
USER airflow
RUN airflow db init

RUN pip install --no-cache-dir apache-airflow apache-airflow-providers-docker scrapy loguru parsel pandas numpy
# Optionally, add your own DAGs or plugins
COPY ./dags /opt/airflow/dags
COPY ./booking /opt/airflow/booking
# COPY ./plugins /opt/airflow/plugins

# Copy any additional configuration files if necessary
# COPY ./airflow.cfg /opt/airflow/airflow.cfg