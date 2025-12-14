{% snapshot snap_mart_well_performance %}
{{
  config(
    target_schema='snapshots',
    unique_key='snap_key',
    strategy='timestamp',
    updated_at='load_ts'
  )
}}

select
  cast(hk_well as varchar) || '|' || cast(source_format as varchar) as snap_key,
  m.*,
  cast(current_timestamp as timestamp) as load_ts
from {{ ref('mart_well_performance') }} m

{% endsnapshot %}
