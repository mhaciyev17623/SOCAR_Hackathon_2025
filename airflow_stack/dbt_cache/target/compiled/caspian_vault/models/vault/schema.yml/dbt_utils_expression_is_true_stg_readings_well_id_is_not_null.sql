



select
    1
from "airflow"."public"."stg_readings"

where not(well_id is not null)

