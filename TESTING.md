# Testing and Validation Guide

This manual covers Windows 11 validation for the YouTube pipeline stack running through Docker Desktop.

## Prerequisites

| Item | Expected Value |
|---|---|
| Operating system | Windows 11 |
| Shell | Windows PowerShell |
| Container runtime | Docker Desktop |
| Project root | `D:\DSEP\ETL Pipeline\youtube-pipeline` |
| PostgreSQL container | `youtube-pipeline-postgres` |
| Airflow webserver service | `airflow-webserver` |
| Airflow scheduler service | `airflow-scheduler` |

## 1. Component Testing

### 1.1 Check PostgreSQL container health

Run this from PowerShell in the project root:

```powershell
docker exec youtube-pipeline-postgres pg_isready -U airflow -d youtube_data
```

Expected result:

| Output | Meaning |
|---|---|
| `accepting connections` | PostgreSQL is healthy and ready for queries |

If you want to inspect the Docker health state directly, use this additional command:

```powershell
docker inspect -f "{{.State.Health.Status}}" youtube-pipeline-postgres
```

### 1.2 Verify Airflow DAG syntax errors and list active DAGs

The current compose file exposes Airflow through the `airflow-webserver` service. Use the following command to list DAGs:

```powershell
docker compose exec airflow-webserver airflow dags list
```

To check for import and syntax problems in the DAG files, run:

```powershell
docker compose exec airflow-webserver airflow dags list-import-errors
```

Recommended combined verification sequence:

```powershell
docker compose exec airflow-webserver airflow dags list
docker compose exec airflow-webserver airflow dags list-import-errors
```

### 1.3 Tail live Airflow scheduler logs

To monitor scheduling and task execution in real time:

```powershell
docker compose logs -f airflow-scheduler
```

If you need to inspect scheduler logs from inside the container filesystem, you can also use:

```powershell
docker compose exec airflow-scheduler bash -lc "tail -f /opt/airflow/logs/scheduler/latest/*.log"
```

## 2. Integration and Data Quality Testing

### 2.1 Production-grade load verification query

Run this inside the PostgreSQL container to confirm that channel statistics were loaded successfully and ordered correctly by subscriber count:

```powershell
docker exec -it youtube-pipeline-postgres psql -U airflow -d youtube_data
```

Then execute:

```sql
SELECT
    channel_id,
    channel_title,
    subscriber_count,
    total_views,
    video_count,
    processed_at
FROM channel_stats
ORDER BY subscriber_count DESC NULLS LAST;
```

Optional row-count sanity check:

```sql
SELECT COUNT(*) AS total_records
FROM channel_stats;
```

### 2.2 Data quality checks

#### Check for NULL values in critical columns

```sql
SELECT
    COUNT(*) AS null_critical_field_rows
FROM channel_stats
WHERE channel_id IS NULL
   OR channel_title IS NULL
   OR subscriber_count IS NULL;
```

Detailed NULL breakdown:

```sql
SELECT
    SUM(CASE WHEN channel_id IS NULL THEN 1 ELSE 0 END) AS null_channel_id,
    SUM(CASE WHEN channel_title IS NULL THEN 1 ELSE 0 END) AS null_channel_title,
    SUM(CASE WHEN subscriber_count IS NULL THEN 1 ELSE 0 END) AS null_subscriber_count
FROM channel_stats;
```

#### Check for duplicate records to confirm PRIMARY KEY integrity

Because `channel_id` is defined as the primary key, duplicates should not exist. This query verifies that assumption:

```sql
SELECT
    channel_id,
    COUNT(*) AS duplicate_count
FROM channel_stats
GROUP BY channel_id
HAVING COUNT(*) > 1;
```

If this returns no rows, primary key integrity is intact.

#### Check timestamp accuracy for data freshness

Confirm that `processed_at` values are populated and reasonably current:

```sql
SELECT
    COUNT(*) AS missing_processed_at_rows
FROM channel_stats
WHERE processed_at IS NULL;
```

Check the latest freshness window:

```sql
SELECT
    channel_id,
    channel_title,
    processed_at,
    CURRENT_TIMESTAMP - processed_at AS data_age,
    CASE
        WHEN processed_at >= CURRENT_TIMESTAMP - INTERVAL '6 hours' THEN 'fresh'
        ELSE 'stale'
    END AS freshness_status
FROM channel_stats
ORDER BY processed_at DESC;
```

Validate that no record has a future timestamp:

```sql
SELECT
    COUNT(*) AS future_timestamp_rows
FROM channel_stats
WHERE processed_at > CURRENT_TIMESTAMP;
```

## 3. Recommended Verification Order

| Step | Action | Success Condition |
|---|---|---|
| 1 | Check PostgreSQL readiness | `pg_isready` reports `accepting connections` |
| 2 | List DAGs | `youtube_data_pipeline` appears in the DAG list |
| 3 | Check import errors | No import errors are returned |
| 4 | Tail scheduler logs | Tasks move from queued to running without repeated failures |
| 5 | Run load verification query | Rows appear ordered by `subscriber_count DESC` |
| 6 | Run NULL checks | No critical NULLs are returned |
| 7 | Run duplicate check | No duplicate `channel_id` rows are returned |
| 8 | Run freshness checks | `processed_at` values are present and not in the future |

## 4. Notes for Windows Users

| Topic | Guidance |
|---|---|
| PowerShell vs CMD | Use PowerShell for the commands above to avoid shell quoting issues |
| Multi-line SQL | Enter SQL inside the `psql` prompt after connecting with `docker exec -it ... psql` |
| Service names | Use `airflow-webserver` and `airflow-scheduler` because those are the actual compose service names in this project |
| Host vs container ports | PostgreSQL is reached internally on port `5432`; host mappings are only used when connecting from Windows directly |

## 5. Quick Copy-Paste Checklist

```powershell
docker exec youtube-pipeline-postgres pg_isready -U airflow -d youtube_data
docker compose exec airflow-webserver airflow dags list
docker compose exec airflow-webserver airflow dags list-import-errors
docker compose logs -f airflow-scheduler
docker exec -it youtube-pipeline-postgres psql -U airflow -d youtube_data
```

```sql
SELECT channel_id, channel_title, subscriber_count, total_views, video_count, processed_at
FROM channel_stats
ORDER BY subscriber_count DESC NULLS LAST;

SELECT COUNT(*) AS null_critical_field_rows
FROM channel_stats
WHERE channel_id IS NULL
   OR channel_title IS NULL
   OR subscriber_count IS NULL;

SELECT channel_id, COUNT(*) AS duplicate_count
FROM channel_stats
GROUP BY channel_id
HAVING COUNT(*) > 1;

SELECT COUNT(*) AS missing_processed_at_rows
FROM channel_stats
WHERE processed_at IS NULL;
```