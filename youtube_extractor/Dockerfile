# =============================================================================
# YouTube Extractor Dockerfile
# =============================================================================

FROM python:3.11-slim

# Metadata labels
LABEL maintainer="data-engineering-team"
LABEL description="YouTube Data API v3 extractor service"
LABEL version="1.0.0"

# Set working directory
WORKDIR /app

# Install system dependencies required by psycopg2 & build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency manifest first to leverage Docker layer caching
COPY requirements.txt .

# Install Python packages
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY extractor.py .

# Non-root user for security hardening
RUN useradd --create-home --shell /bin/bash extractor_user
USER extractor_user

# Default entrypoint — can be overridden by Airflow/docker-compose
CMD ["python", "extractor.py"]
