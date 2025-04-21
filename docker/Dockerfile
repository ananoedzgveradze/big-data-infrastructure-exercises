FROM apache/airflow:2.7.3-python3.9

USER root

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        default-libmysqlclient-dev \
        pkg-config \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy files as root
COPY requirements-airflow.txt /requirements-airflow.txt
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Switch back to airflow user
USER airflow

# Install Python packages
RUN pip install --no-cache-dir -r /requirements-airflow.txt

# Create the airflow user
ENV AIRFLOW_HOME=/opt/airflow

ENTRYPOINT ["/entrypoint.sh"]
