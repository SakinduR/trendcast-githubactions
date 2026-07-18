@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "PROJECT_ROOT=%~dp0"
pushd "%PROJECT_ROOT%"

set "COMPOSE_CMD=docker-compose"
set "AIRFLOW_SERVICE=airflow-webserver"
set "POSTGRES_SERVICE=postgres"

if not exist "airflow\dags" mkdir "airflow\dags"
if not exist "airflow\logs" mkdir "airflow\logs"
if not exist "airflow\plugins" mkdir "airflow\plugins"
if not exist "jupyter\notebooks" mkdir "jupyter\notebooks"

if not exist ".env" (
    echo Error: .env file is missing in the project root.
    popd
    exit /b 1
)

echo Starting core services...
%COMPOSE_CMD% up -d
if errorlevel 1 (
    echo Error: docker-compose up -d failed.
    popd
    exit /b 1
)

echo Waiting for PostgreSQL to become healthy...
set "POSTGRES_CONTAINER_ID="
for /f "delims=" %%I in ('%COMPOSE_CMD% ps -q %POSTGRES_SERVICE%') do set "POSTGRES_CONTAINER_ID=%%I"

if not defined POSTGRES_CONTAINER_ID (
    echo Error: Could not find the PostgreSQL container after startup.
    popd
    exit /b 1
)

set "POSTGRES_HEALTH="
for /l %%N in (1,1,30) do (
    for /f "delims=" %%H in ('docker inspect -f "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" "!POSTGRES_CONTAINER_ID!" 2^>nul') do set "POSTGRES_HEALTH=%%H"
    if /I "!POSTGRES_HEALTH!"=="healthy" goto :postgres_ready
    timeout /t 5 /nobreak >nul
)

echo Error: PostgreSQL did not report a healthy status in time.
%COMPOSE_CMD% ps
popd
exit /b 1

:postgres_ready
set "YOUTUBE_API_KEY="
for /f "usebackq tokens=1* delims==" %%A in (`findstr /b /i "YOUTUBE_API_KEY=" ".env"`) do set "YOUTUBE_API_KEY=%%B"

if not defined YOUTUBE_API_KEY (
    echo Error: YOUTUBE_API_KEY is not set in .env.
    popd
    exit /b 1
)

echo Setting Airflow runtime variable YOUTUBE_API_KEY...
%COMPOSE_CMD% exec -T %AIRFLOW_SERVICE% airflow variables set YOUTUBE_API_KEY "%YOUTUBE_API_KEY%"
if errorlevel 1 (
    echo Error: Failed to set the Airflow variable.
    popd
    exit /b 1
)

echo Deployment automation complete.
echo Manual DAG trigger example:
echo   %COMPOSE_CMD% exec -T %AIRFLOW_SERVICE% airflow dags trigger youtube_data_pipeline

popd
endlocal

rem Manual trigger example:
rem docker-compose exec -T airflow-webserver airflow dags trigger youtube_data_pipeline