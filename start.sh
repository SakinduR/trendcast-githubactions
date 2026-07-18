#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

COMPOSE_CMD="docker-compose"
AIRFLOW_SERVICE="airflow-webserver"
POSTGRES_SERVICE="postgres"

required_dirs=(
  "airflow/dags"
  "airflow/logs"
  "airflow/plugins"
  "jupyter/notebooks"
)

for dir in "${required_dirs[@]}"; do
  mkdir -p "$dir"
done

if [[ ! -f ".env" ]]; then
  echo "Error: .env file is missing in the project root." >&2
  exit 1
fi

echo "Starting core services..."
$COMPOSE_CMD up -d

echo "Waiting for PostgreSQL to become healthy..."
postgres_container_id="$($COMPOSE_CMD ps -q "$POSTGRES_SERVICE")"
if [[ -z "$postgres_container_id" ]]; then
  echo "Error: Could not find the PostgreSQL container after startup." >&2
  exit 1
fi

for attempt in {1..30}; do
  health_status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$postgres_container_id" 2>/dev/null || true)"
  if [[ "$health_status" == "healthy" ]]; then
    break
  fi
  sleep 5
done

health_status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$postgres_container_id" 2>/dev/null || true)"
if [[ "$health_status" != "healthy" ]]; then
  echo "Error: PostgreSQL did not report a healthy status in time." >&2
  $COMPOSE_CMD ps
  exit 1
fi

set -a
. ./.env
set +a

if [[ -z "${YOUTUBE_API_KEY:-}" ]]; then
  echo "Error: YOUTUBE_API_KEY is not set in .env." >&2
  exit 1
fi

echo "Setting Airflow runtime variable YOUTUBE_API_KEY..."
$COMPOSE_CMD exec -T "$AIRFLOW_SERVICE" airflow variables set YOUTUBE_API_KEY "$YOUTUBE_API_KEY"

echo "Deployment automation complete."
echo "Optional manual DAG trigger:"
echo "  $COMPOSE_CMD exec -T $AIRFLOW_SERVICE airflow dags trigger youtube_data_pipeline"

# Manual trigger example:
# docker-compose exec -T airflow-webserver airflow dags trigger youtube_data_pipeline