FROM apache/airflow:2.9.3

USER root
RUN apt-get update && apt-get install -y --no-install-recommends git \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
  "sqlalchemy<2.0" \
  "dbt-core==1.10.15" \
  "dbt-postgres==1.9.0" \
  "dbt-trino==1.9.3"

# --- Dashboard deps (Streamlit + plotting + DuckDB + Postgres client libs) ---
USER root
RUN /home/airflow/.local/bin/python -m pip install --no-cache-dir \
    streamlit==1.52.1 \
    duckdb \
    plotly \
    pandas \
    psycopg2-binary \
    sqlalchemy
USER airflow

# --- Demo dashboard deps (Streamlit) ---
USER root
RUN /home/airflow/.local/bin/python -m pip install --no-cache-dir \
    streamlit duckdb plotly pandas
USER airflow
