
      update "airflow"."snapshots"."snap_mart_well_performance"
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "snap_mart_well_performance__dbt_tmp060636913503" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = "airflow"."snapshots"."snap_mart_well_performance".dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      
        and "airflow"."snapshots"."snap_mart_well_performance".dbt_valid_to is null;
      


    insert into "airflow"."snapshots"."snap_mart_well_performance" ("snap_key", "hk_well", "source_format", "total_readings", "avg_amplitude", "data_quality_rate", "load_ts", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."snap_key",DBT_INTERNAL_SOURCE."hk_well",DBT_INTERNAL_SOURCE."source_format",DBT_INTERNAL_SOURCE."total_readings",DBT_INTERNAL_SOURCE."avg_amplitude",DBT_INTERNAL_SOURCE."data_quality_rate",DBT_INTERNAL_SOURCE."load_ts",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "snap_mart_well_performance__dbt_tmp060636913503" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;

  