# Use base image Airflow with Python 3.11
FROM apache/airflow:2.10.0-python3.11

# Set user root to install system dependencies
USER root

# Install system dependencies (PostgreSQL adapter)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libpq-dev gcc && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Back to user airflow
USER airflow

# Copy requirements.txt & install dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt