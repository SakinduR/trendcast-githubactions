# =============================================================================
# Airflow Dockerfile
# Base: apache/airflow:2.8.1-python3.11
# Adds: PostgreSQL driver, YouTube API client, Kafka producer
# =============================================================================

FROM apache/airflow:2.8.1-python3.11

# Switch to root briefly to install system-level dependencies
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the 'airflow' user for pip installs (security best practice)
USER airflow

# Copy and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt

# Airflow home is /opt/airflow by default
WORKDIR /opt/airflow
