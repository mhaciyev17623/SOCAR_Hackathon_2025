{{ config(materialized='table') }}

-- Time dimension derived from load timestamps (ingest_ts / load_ts)
with t as (
  select distinct cast(load_ts as timestamp) as ts
  from {{ ref('dv_sat_readings') }}
)
select
  ts,
  date(ts) as d,
  year(ts) as year,
  month(ts) as month,
  day(ts) as day,
  strftime(ts, '%Y-%m') as year_month,
  strftime(ts, '%Y-%m-%d') as ymd,
  hour(ts) as hour
from t
