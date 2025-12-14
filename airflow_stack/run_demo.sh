#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "==> Hard reset (avoid stale containers + 8501 conflicts)..."
docker compose down --remove-orphans

echo "==> Build + start all services..."
docker compose up -d --build

echo "==> Run dbt pipeline (explicit dbt path)..."
docker compose exec -T airflow-webserver bash -lc '
set -euo pipefail
cd /opt/project/caspian_vault
/home/airflow/.local/bin/dbt deps --profile caspian_vault --target dev
/home/airflow/.local/bin/dbt build --profile caspian_vault --target dev
/home/airflow/.local/bin/dbt snapshot --profile caspian_vault --target dev
'

echo
echo "✅ Demo is up."
echo "Airflow UI:   http://localhost:8080"
echo "Dashboard:    http://localhost:8501"
