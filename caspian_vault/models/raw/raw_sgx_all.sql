{{ config(materialized='view') }}
select *
from read_parquet('/home/hackathon/processed_data/legacy_survey_*.parquet')
