{% snapshot MART_SALES_SALES_TERRITORY_HIST %}
{{
    config(
       transient=false
      ,target_schema='MART_SALES'
      ,alias='SALES_TERRITORY_HIST'
      ,unique_key = 'SALES_TERRITORY_KEY'
      ,tags ='MART_SALES'
      ,strategy='timestamp'
      ,updated_at='BIW_UPD_DTTM::DATE'
      ,invalidate_hard_deletes=True
      ,merge_update_columns = 
                            ['SALES_ORGANIZATION_CODE', 'SALES_ORGANIZATION_DESCRIPTION', 'SALES_REP_FIRM_NAME', 'SALES_OFFICE_CODE', 'SALES_OFFICE_NAME'
                            , 'SALES_AREA_NAME', 'SALES_SUB_AREA_NAME', 'SALES_REGION_CODE', 'SALES_REGION_NAME'
                            , 'BIW_UPD_DTTM', 'BIW_BATCH_ID', 'BIW_MD5_KEY']

    )
}}
--Building SCD2 table for Sales Territory
--dbt docs requested to do select *
select * from {{ ref('MART_SALES_SALES_TERRITORY') }}
{% endsnapshot %}