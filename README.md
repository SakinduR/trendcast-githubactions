# YouTube Data Pipeline 🚀

A production-grade, fully containerised ETL pipeline that extracts YouTube channel statistics via the YouTube Data API v3, streams data through Apache Kafka, processes it with Apache Spark, stores it in PostgreSQL, orchestrates workflows via Apache Airflow, and provides interactive analysis via Jupyter Notebooks.

---

## Architecture

```
YouTube Data API v3
        │
        ▼
┌─────────────────┐
│ YouTube          │  Python + google-api-python-client
│ Extractor        │  Publishes JSON to Kafka
└────────┬────────┘
         │  Kafka Topic: youtube_raw_data
         ▼
┌─────────────────┐
│ Apache Kafka     │  Confluent Platform 7.5
│ + Zookeeper     │  Message broker / streaming bus
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Apache Spark     │  PySpark Structured Streaming
│ (Master/Worker) │  Enrichment + Deduplication
└────────┬────────┘
         │
         ▼
┌─────────────────┐       ┌─────────────────┐
│   PostgreSQL 13  │◄──────│ Apache Airflow  │
│  channel_stats   │  DAG  │ (Scheduler +    │
│      table       │       │  Webserver)     │
└────────┬────────┘       └─────────────────┘
         │
         ▼
┌─────────────────┐
│ Jupyter Lab      │  pandas + psycopg2 + seaborn + plotly
│ (Analysis)       │  Interactive KPI dashboards
└─────────────────┘
```

---

## Project Structure

```
youtube-pipeline/
├── docker-compose.yml          ← All services defined here
├── .env                        ← Secrets & config (never commit!)
├── .gitignore
│
├── airflow/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── dags/
│       └── youtube_pipeline.py ← DAG: create_tables >> extract_youtube_data
│
├── spark/
│   ├── Dockerfile
│   └── scripts/
│       └── process_youtube_data.py  ← PySpark Structured Streaming
│
├── jupyter/
│   ├── Dockerfile
│   └── notebooks/
│       └── youtube_analysis.ipynb   ← Interactive analytics
│
├── youtube_extractor/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── extractor.py           ← YouTube API → Kafka / PostgreSQL
│
└── postgres/
    └── init/
        └── 01_schema.sql      ← Auto-runs on first container start
```

---

## Quick Start

### Prerequisites

- [Docker Desktop](https://docs.docker.com/get-docker/) ≥ 24
- [Docker Compose](https://docs.docker.com/compose/) ≥ 2.20
- A [YouTube Data API v3 key](https://console.cloud.google.com/apis/credentials)

### Step 1 — Configure Environment

```bash
# Copy the template (already provided) and fill in your values
cp .env .env.local   # optional, or edit .env directly

# Required values to fill in:
# YOUTUBE_API_KEY=<your_api_key>
# YOUTUBE_CHANNEL_IDS=UCxxxxxx,UCyyyyyy
# AIRFLOW__CORE__FERNET_KEY=<generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())">
```

### Step 2 — Build & Start All Services

```bash
docker compose up --build -d
```

> First build takes ~5–10 minutes as images are pulled and compiled.

### Step 3 — Verify Services Are Running

```bash
docker compose ps
```

| Service | URL | Credentials |
|---|---|---|
| Airflow Webserver | http://localhost:8080 | admin / admin |
| Jupyter Lab | http://localhost:8888 | token from `.env` |
| Spark Master UI | http://localhost:8081 | — |
| PostgreSQL | localhost:5433 | from `.env` |
| Kafka | localhost:29092 | — |

### Step 4 — Configure Airflow PostgreSQL Connection

1. Open Airflow at http://localhost:8080
2. Navigate to **Admin → Connections → Add Connection**
3. Set the following:
   - **Connection ID**: `youtube_postgres`
   - **Connection Type**: `Postgres`
   - **Host**: `postgres`
   - **Database**: `youtube_pipeline`
   - **Login**: `airflow`
   - **Password**: `airflow_secret_password` (or your `.env` value)
   - **Port**: `5432`

### Step 5 — Trigger the DAG

1. Enable the `youtube_data_pipeline` DAG in the Airflow UI
2. Click **Trigger DAG** to run immediately
3. Watch the tasks: `create_tables` → `extract_youtube_data`

### Step 6 — Explore Data in Jupyter

Open http://localhost:8888, navigate to `work/youtube_analysis.ipynb`, and run all cells.

---

## Service Details

### YouTube Extractor (`youtube_extractor/`)

Run in standalone mode:

```bash
# Kafka mode (default)
docker compose run --rm youtube-extractor python extractor.py --mode=kafka

# Direct PostgreSQL mode (bypasses Kafka)
docker compose run --rm youtube-extractor python extractor.py --mode=postgres
```

### Spark Processing (`spark/`)

Submit the PySpark job:

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark/scripts/process_youtube_data.py
```

### PostgreSQL — Direct Query

```bash
docker compose exec postgres psql -U airflow -d youtube_pipeline -c \
  "SELECT channel_title, subscriber_count, engagement_ratio FROM channel_stats_enriched ORDER BY subscriber_count DESC LIMIT 10;"
```

---

## Channel Stats Schema

```sql
CREATE TABLE channel_stats (
    channel_id          VARCHAR(64)   PRIMARY KEY,   -- YouTube channel ID
    channel_title       VARCHAR(255)  NOT NULL,
    channel_description TEXT,
    published_at        TIMESTAMPTZ,                 -- Channel creation date
    country             VARCHAR(10),                 -- ISO 3166-1 alpha-2
    total_views         BIGINT        DEFAULT 0,     -- Lifetime view count
    subscriber_count    BIGINT        DEFAULT 0,     -- Current subscribers
    video_count         INTEGER       DEFAULT 0,
    processed_at        TIMESTAMPTZ   NOT NULL,      -- Last extraction time
    created_at          TIMESTAMPTZ   NOT NULL
);
```

Enriched view with computed KPIs:

```sql
SELECT * FROM channel_stats_enriched;
-- Adds: avg_views_per_video, views_per_subscriber, engagement_ratio, size_tier, channel_age_days
```

---

## Airflow DAG — Task Graph

```
create_tables
     │
     ▼
extract_youtube_data
```

- **Schedule**: `0 */6 * * *` (every 6 hours)
- **Retries**: 3 × with exponential backoff (5m → 10m → 20m)
- **Idempotency**: `INSERT ... ON CONFLICT DO UPDATE` ensures safe reruns

---

## Stopping & Cleanup

```bash
# Stop all services (preserve data)
docker compose down

# Stop and delete all volumes (WARNING: deletes PostgreSQL data)
docker compose down -v

# Remove built images
docker compose down --rmi all
```

---

## Quota Considerations

The YouTube Data API v3 has a **10,000 unit daily quota**. Each `channels.list` request costs **1 unit** and can retrieve up to **50 channels**. With a 6-hour schedule (4 runs/day):

- 4 runs × 1 request per 50 channels = very low quota consumption
- Quota resets at midnight Pacific Time

Monitor usage at [Google Cloud Console → APIs → YouTube Data API v3](https://console.cloud.google.com/apis/api/youtube.googleapis.com/quotas).

---

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Commit with conventional commits: `git commit -m "feat: add subscriber growth trending"`
4. Open a Pull Request

---

## License

MIT — See LICENSE file for details.
