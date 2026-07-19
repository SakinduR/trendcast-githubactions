# =============================================================================
# Spark Dockerfile
# Base: apache/spark:3.5.0 (official Apache image — always available)
# Adds: Python dependencies for the processing script
# =============================================================================

FROM apache/spark:3.5.0

# Switch to root for dependency installation
USER root

# Install pip and system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages for the processing script
# Note: apache/spark:3.5.0 ships Python 3.8 → pandas max is 2.0.3
RUN pip3 install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    kafka-python==2.0.2 \
    pandas==2.0.3 \
    python-dotenv==1.0.1

# Copy the Spark processing script into the container
COPY scripts/ /opt/spark/scripts/

# Permissions
RUN chmod +x /opt/spark/scripts/*.py 2>/dev/null || true

# The official apache/spark image runs as root by default
# Default workdir
WORKDIR /opt/spark
