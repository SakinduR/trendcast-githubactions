# YouTube Data Extractor container
# Build context: ./youtube_extractor (referenced from docker-compose.yml)

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libpq5 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY extractor.py .

# Default output mode for container runs; override via docker-compose command or env.
ENV EXTRACTOR_OUTPUT=postgres \
    LOG_LEVEL=INFO

ENTRYPOINT ["python", "extractor.py"]
CMD ["--output", "postgres"]
