from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

DBT_DIR = "/opt/project/caspian_vault"
DBT_BIN = "/home/airflow/.local/bin/dbt"
PROFILE = "caspian_vault"
TARGET = "dev"

# Shared dbt env (keep logs/artifacts inside airflow volume)
DBT_ENV = """
set -euo pipefail
export DBT_PROFILES_DIR=/opt/project/caspian_vault/dbt_profiles
export DBT_LOG_PATH=/opt/airflow/logs/dbt
export DBT_TARGET_PATH=/opt/airflow/dbt_target
export DBT_STATE_PATH=/opt/airflow/dbt_state
export DBT_PACKAGES_INSTALL_PATH=/opt/airflow/dbt_packages

mkdir -p "$DBT_LOG_PATH" "$DBT_TARGET_PATH" "$DBT_PACKAGES_INSTALL_PATH"
cd {dbt_dir}
"""

with DAG(
    dag_id="caspian_full_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["docker", "celery", "dbt"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            DBT_ENV.format(dbt_dir=DBT_DIR)
            + f"""
{DBT_BIN} deps --profile {PROFILE} --target {TARGET}
"""
        ),
    )

    dbt_build_staging = BashOperator(
        task_id="dbt_build_staging",
        bash_command=(
            DBT_ENV.format(dbt_dir=DBT_DIR)
            + f"""
{DBT_BIN} build --profile {PROFILE} --target {TARGET}  --select "staging"
"""
        ),
    )

    dbt_build_vault = BashOperator(
        task_id="dbt_build_vault",
        bash_command=(
            DBT_ENV.format(dbt_dir=DBT_DIR)
            + f"""
{DBT_BIN} build --profile {PROFILE} --target {TARGET}  --select "vault"
"""
        ),
    )

    dbt_build_marts = BashOperator(
        task_id="dbt_build_marts",
        bash_command=(
            DBT_ENV.format(dbt_dir=DBT_DIR)
            + f"""
{DBT_BIN} build --profile {PROFILE} --target {TARGET}  --select "marts"
"""
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            DBT_ENV.format(dbt_dir=DBT_DIR)
            + f"""
{DBT_BIN} test --profile {PROFILE} --target {TARGET} 
"""
        ),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            DBT_ENV.format(dbt_dir=DBT_DIR)
            + f"""
{DBT_BIN} snapshot --profile {PROFILE} --target {TARGET} 
"""
        ),
    )

    # FAIL-FAST chain
    # dbt_deps >> dbt_build_staging >> dbt_build_vault >> dbt_build_marts >> dbt_test >> dbt_snapshot >> save_dbt_state
    dbt_deps >> dbt_build_staging >> dbt_build_vault >> dbt_build_marts >> dbt_test >> dbt_snapshot
