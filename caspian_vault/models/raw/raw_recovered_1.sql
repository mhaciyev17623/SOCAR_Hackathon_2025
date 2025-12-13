{{ config(materialized='view') }}
select *
from read_parquet('/home/hackathon/processed_data/archive_batch_seismic_readings.parquet')
