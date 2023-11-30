
{% snapshot MART_SALES_OM_DEMAND_SHAPING_RULES_HIST %}

{{
    config(
       transient=false
      ,target_schema='MART_SALES'
      ,alias='OM_DEMAND_SHAPING_RULES_HIST'
      ,unique_key = 'DEMAND_SHAPING_RULE_KEY'
      ,tags ='MART_SALES'
      ,strategy='timestamp'
      ,updated_at='BIW_UPD_DTTM::DATE'
      ,invalidate_hard_deletes=True
      ,merge_update_columns =['RULE_NAME', 'LEVEL_NAME', 'LEVEL_VALUE1', 'RULE_VALUE', 'RULE_ACTIVE_START_DATE',
                              'RULE_ACTIVE_END_DATE', 'RULE_CREATION_DATETIME', 'RULE_CREATED_BY', 'RULE_LAST_UPDATED_DATETIME', 'RULE_LAST_UPDATED_BY',
                              'LEVEL_VALUE2', 'LEVEL_VALUE3', 'IS_RULE_ACTIVE', 'BIW_UPD_DTTM', 'BIW_BATCH_ID', 'BIW_MD5_KEY'] 
    )
}}
--Building SCD2 table for Sales Territory
--dbt docs requested to do select *
select * from {{ ref('MART_SALES_OM_DEMAND_SHAPING_RULES') }} 

{% endsnapshot %}