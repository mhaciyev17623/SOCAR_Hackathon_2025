-- test passes if returns 0 rows
with src as (
  select count(*) as n from {{ ref('stg_readings') }}
),
sat as (
  select count(*) as n from {{ ref('dv_sat_readings') }}
)
select
  src.n as src_rows,
  sat.n as sat_rows
from src, sat
where src.n != sat.n
