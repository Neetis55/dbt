{% snapshot MART_SALES_PRODUCT_HIST %}
{{
    config(
       transient=false
      ,target_schema='MART_SALES'
      ,alias='PRODUCT_HIST'
      ,unique_key = 'PRODUCT_KEY'
      ,tags ='MART_SALES'
      ,strategy='timestamp'
      ,updated_at='BIW_UPD_DTTM::DATE'
      ,invalidate_hard_deletes=True
      ,merge_update_columns = ['PRODUCT_ID', 'PRODUCT_SOURCE','IS_WEB_VISIBLE', 'WEB_PART_NAME','WEB_PART_GROUP',
                               'MOQ_QUANTITY','PART_OID_AK_ID','CREATED_DATE','IS_LAUNCH_SALES_TIPS' ,'IS_LAUNCH_PRESENTATION_CUSTOMER'
                                 ,'IS_LAUNCH_PRESENTATION_DISTRIBUTOR','IS_DATA_SHEET_AVAILABLE','BIW_UPD_DTTM', 'BIW_BATCH_ID','BIW_MD5_KEY']
    )
}}
--Building SCD2 table for Sales Territory
--dbt docs requested to do select *
select * from {{ ref('MART_SALES_PRODUCT') }} 

{% endsnapshot %}