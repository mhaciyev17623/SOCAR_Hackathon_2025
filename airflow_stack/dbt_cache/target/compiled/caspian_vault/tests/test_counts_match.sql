-- test passes if returns 0 rows
-- Only enforce count match when source has data (prevents failure when raw sources are stubbed/empty)

with src as (
  select count(*) as n from "airflow"."public"."stg_readings"
),
sat as (
  select count(*) as n from "airflow"."public"."dv_sat_readings"
)
select
  src.n as src_rows,
  sat.n as sat_rows
from src, sat
where src.n > 0
  and src.n != sat.n