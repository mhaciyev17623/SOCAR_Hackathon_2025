
# Socar Hackathon 2025 - Data Engineering track 


## Overview

This project implements an end-to-end data engineering platform for recovering, processing, modeling, and analyzing legacy and corrupted seismic data for Caspian Petrochemical.

The platform restores decades of fragmented seismic history, preserves full data provenance, and delivers analytical insights required for time-critical drilling decisions.

The solution covers:
- Corrupted Parquet recovery and forensic analysis
- Hidden flag extraction from Parquet metadata
- Decoding of legacy `.sgx` seismic binary format
- Raw data ingestion with full lineage
- Data Vault 2.0 modeling
- Dimensional analytics and marts
- Workflow orchestration with Apache Airflow
- Interactive analytics dashboard



## Execution Environment

This project is designed to run on the **SOCAR-provided hackathon virtual machine**, accessed via SSH.

 ⁠bash
ssh -i <private-key-file> hackathon@<vm-ip>
⁠ `

All scripts, pipelines, dbt models, and dashboards were executed and tested in this environment.



## High-Level Architecture

Raw seismic data (corrupted Parquet files and legacy `.sgx` binaries) is recovered and converted into structured Parquet files.
Recovered data is ingested into a Raw Vault and transformed using **Data Vault 2.0** principles.
Dimensional models and marts are built for analytics and visualization, fully orchestrated by Apache Airflow.



## Technology Stack

* Orchestration: Apache Airflow (CeleryExecutor)
* Data Modeling: dbt
* Storage: Postgres / DuckDB
* Processing: Bash, Python
* Containerization: Docker, Docker Compose
* Visualization: Streamlit
* Data Formats: Parquet, custom `.sgx` binary format




## Task 1 — Data Recovery & Forensics

### Find Hidden Flag in Parquet Files

 ⁠bash
bash solutions/flag_parquet.sh --data-dir <data-directory>


⁠ * Inspects Parquet footers and metadata
* Detects abnormal padding
* Extracts embedded hidden data

### Recover Corrupted Parquet Files

 ⁠bash
bash solutions/corrupted_parquet.sh --data-dir <data-directory>


⁠ * Repairs corrupted Parquet metadata
* Rewrites valid Parquet files
* Outputs recovered data to `processed_data/` in Parquet format

### Decode Legacy `.sgx` Files

 ⁠bash
bash solutions/load_sgx.sh --data-dir <data-directory>


⁠ * Parses custom binary `.sgx` format
* Correctly handles little-endian encoding
* Decodes global headers and trace records
* Converts data to Parquet format



## Task 2 — Data Vault 2.0 Modeling

### Vault Design

The Raw Vault is built following **Data Vault 2.0** principles to preserve historical truth and provenance.

**Hubs**

* `dv_hub_well`
* `dv_hub_survey_type`

**Links**

* `dv_link_well_survey`

**Satellites**

* `dv_sat_readings`

Each satellite includes:

* Load timestamp
* Source file reference
* Row checksum
* Hashdiff for change detection

### Data Quality Tests

Implemented using dbt:

* Not-null constraints
* Uniqueness checks
* Referential integrity validation
* Record count consistency tests



## Task 3 — Analytics & Dimensional Models

### Dimensions

* `dim_well` (includes deterministic pseudo geo-coordinates for mapping)
* `dim_survey_type`
* `dim_source_format`
* `dim_time`

### Fact Table

* `fct_readings`

### Data Marts

* **mart_well_performance**
  Summarizes well performance by data source format

* **mart_sensor_analysis**
  Analyzes sensor (survey type) reliability and data quality

* **mart_survey_summary**
  Summarizes survey coverage, readings, and ingestion timelines



## Task 4 — Platform & Orchestration

### Start the Platform

 ⁠bash
docker compose up -d


Services include:

* Airflow Webserver
* Scheduler
* Celery Workers
* Redis
* Postgres
* Streamlit Dashboard

### Airflow DAG

The DAG orchestrates:

1. Raw data ingestion
2. Staging transformations
3. Data Vault builds
4. Dimensional model builds
5. Data quality validation



## Dashboard

The Streamlit dashboard provides:

* Well map visualization
* Well state monitoring
* Amplitude distributions
* Data quality KPIs
* Survey and sensor analytics






## Running dbt Manually

 ⁠bash
dbt deps
dbt build --profile caspian_vault --target dev




## Final Notes

* All required Bash scripts are located in the `solutions/` directory
* Scripts accept `--data-dir` as input and output Parquet data to `processed_data/`





