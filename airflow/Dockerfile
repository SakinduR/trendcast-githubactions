# Extends the official Apache Airflow image with pipeline-specific Python deps.
# Build context: ./airflow (referenced from docker-compose.yml)

FROM apache/airflow:2.10.5-python3.11

USER root

# Install any OS-level dependencies required by Python packages (extend as needed)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt
