from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

DBT_DIR = "/opt/project/caspian_vault"
DBT_BIN = "/home/airflow/.local/bin/dbt"

with DAG(
    dag_id="caspian_full_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["docker", "celery", "dbt"],
) as dag:

    dbt_build_and_snapshot = BashOperator(
        task_id="dbt_build_and_snapshot",
        bash_command=f"""
        set -euo pipefail

        export DBT_PROFILES_DIR=/opt/project/caspian_vault/dbt_profiles
        export DBT_LOG_PATH=/opt/airflow/logs/dbt
        export DBT_TARGET_PATH=/opt/airflow/dbt_target

        mkdir -p "$DBT_LOG_PATH" "$DBT_TARGET_PATH"

        cd {DBT_DIR}

        {DBT_BIN} deps --profile caspian_vault --target dev
        {DBT_BIN} build --profile caspian_vault --target dev
        {DBT_BIN} snapshot --profile caspian_vault --target dev
        """,
    )
